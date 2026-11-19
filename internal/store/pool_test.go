package store

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// PoolConfig keeps everything the URL says and adds plan_cache_mode, which
// replaces a value the URL gives (D79).
func TestPoolConfig_KeepsTheURLAndSetsThePlanCacheMode(t *testing.T) {
	cfg, err := PoolConfig("postgres://edns:edns@db.example.com:5433/edns?sslmode=disable" +
		"&pool_max_conns=7&application_name=relay&options=-c%20statement_timeout%3D5s&plan_cache_mode=auto")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConns != 7 {
		t.Errorf("MaxConns = %d, want 7", cfg.MaxConns)
	}
	cc := cfg.ConnConfig
	if cc.Host != "db.example.com" || cc.Port != 5433 || cc.Database != "edns" || cc.User != "edns" {
		t.Errorf("connection target %s:%d/%s as %s", cc.Host, cc.Port, cc.Database, cc.User)
	}
	for k, want := range map[string]string{
		"application_name": "relay",
		"options":          "-c statement_timeout=5s",
		"plan_cache_mode":  PlanCacheMode,
	} {
		if got := cc.RuntimeParams[k]; got != want {
			t.Errorf("RuntimeParams[%q] = %q, want %q", k, got, want)
		}
	}

	if _, err := PoolConfig("postgres://edns@db.example.com:notaport/edns"); err == nil {
		t.Error("an invalid URL was accepted")
	}
}

// connectors are the functions that open a Postgres connection or pool,
// by import path.
var connectors = map[string][]string{
	"github.com/jackc/pgx/v5/pgxpool": {"New", "NewWithConfig"},
	"github.com/jackc/pgx/v5":         {"Connect", "ConnectConfig", "ConnectWithOptions"},
	"github.com/jackc/pgx/v5/pgconn":  {"Connect", "ConnectConfig", "ConnectWithOptions"},
	"github.com/jackc/pgx/v5/stdlib":  {"OpenDB", "OpenDBFromPool"},
	"database/sql":                    {"Open", "OpenDB"},
}

// allowedConnectors are the only places outside test files that may use one
// of the connectors, keyed by the file's path from the module root.
var allowedConnectors = map[string][]string{
	// The constructor of every role's pool (D79).
	"internal/store/pool.go": {"pgxpool.NewWithConfig"},
	// The migrate role (cmd/migrate) applies the schema with golang-migrate
	// through a database/sql handle; it runs no statement of the store.
	"migrations/migrations.go": {"sql.Open"},
	// The tests' admin connection, which creates and drops test databases.
	"internal/testutil/postgres.go": {"pgx.Connect"},
}

// No code outside a test file opens a Postgres connection or pool other than
// through OpenPool, so that every role plans with the current statistics
// (D79). The scan covers every Go file of the module, whatever its build
// tags; the allowed uses must still be found, which shows the scan works.
func TestNoPoolIsOpenedOutsideOpenPool(t *testing.T) {
	root := moduleRoot(t)
	found := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if p != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		uses, err := connectorUses(p)
		if err != nil {
			return err
		}
		if len(uses) > 0 {
			found[filepath.ToSlash(rel)] = uses
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for file, uses := range found {
		for _, use := range uses {
			if !slices.Contains(allowedConnectors[file], use) {
				t.Errorf("%s calls %s: open the pool with store.OpenPool (D79)", file, use)
			}
		}
	}
	for file, uses := range allowedConnectors {
		for _, use := range uses {
			if !slices.Contains(found[file], use) {
				t.Errorf("the allowed use %s in %s was not found: the scan is broken or the list is out of date", use, file)
			}
		}
	}
}

// versionSuffix matches the major-version element of an import path.
var versionSuffix = regexp.MustCompile(`^v[0-9]+$`)

// connectorUses returns the connectors that the file refers to, as
// "name.Func" with the package's default name.
func connectorUses(file string) ([]string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	local := map[string]string{} // local name -> import path
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		if _, ok := connectors[p]; !ok {
			continue
		}
		name := defaultName(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		local[name] = p
	}
	if len(local) == 0 {
		return nil, nil
	}
	var uses []string
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		p, ok := local[id.Name]
		if ok && slices.Contains(connectors[p], sel.Sel.Name) {
			uses = append(uses, defaultName(p)+"."+sel.Sel.Name)
		}
		return true
	})
	return uses, nil
}

// defaultName is the package name an import path is used under without an
// explicit name: its last element, or the one before a major version.
func defaultName(importPath string) string {
	base := path.Base(importPath)
	if versionSuffix.MatchString(base) {
		return path.Base(path.Dir(importPath))
	}
	return base
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
