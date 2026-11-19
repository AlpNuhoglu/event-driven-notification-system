package domain_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The guard of ParseID: the UUID library's parser accepts other spellings of
// an id than the canonical one, so no code but ParseID may reach it. It is
// reached directly (uuid.Parse, ParseBytes, MustParse, Validate, and the
// UnmarshalText, UnmarshalJSON and Scan methods of the library's types) and
// indirectly, without a visible call: encoding/json decoding into a value
// whose type holds a uuid.UUID calls UnmarshalText, and so does the binding
// of the generated API code (github.com/oapi-codegen/runtime) for a
// parameter of type uuid.UUID. The scan type-checks every non-test package
// of the module, generated code included, and finds both kinds.
//
// What it cannot see: a decode into a destination whose static type is an
// interface (any) and whose dynamic type holds a uuid.UUID, a decoder other
// than encoding/json and the oapi-codegen runtime, reflection, and the
// library's methods called through an interface (database/sql and pgx call
// Scan that way, with values from the database).

const uuidPath = "github.com/google/uuid"

// lenientFuncs are the library's functions that parse text leniently.
var lenientFuncs = []string{"Parse", "ParseBytes", "MustParse", "Validate"}

// lenientMethods are the methods of the library's types (UUID, NullUUID)
// that parse text leniently.
var lenientMethods = []string{"UnmarshalText", "UnmarshalJSON", "Scan"}

// decoders are the functions that decode text into a destination argument,
// by package; a name ending in "*" is a prefix.
var decoders = map[string][]string{
	"encoding/json":                   {"Unmarshal", "Decode"},
	"encoding/xml":                    {"Unmarshal", "Decode", "DecodeElement"},
	"github.com/oapi-codegen/runtime": {"Bind*"},
}

// allowedLenientUses is the one use the scan must find, and allow.
var allowedLenientUses = []string{"internal/domain/id.go: uuid.Parse"}

func TestLenientUUIDParserIsCalledOnlyByParseID(t *testing.T) {
	m := loadModule(t)
	// A package that does not type-check is still scanned as far as it
	// does; its errors fail the test after the findings.
	defer func() {
		for _, err := range m.typeErrors {
			t.Errorf("type-check: %v", err)
		}
	}()
	var found []string
	var offending []string
	for _, p := range m.pkgs {
		for _, u := range lenientUses(m.fset, p.files, p.info) {
			rel, err := filepath.Rel(m.root, u.pos.Filename)
			if err != nil {
				t.Fatal(err)
			}
			key := filepath.ToSlash(rel) + ": " + u.what
			found = append(found, key)
			if !slices.Contains(allowedLenientUses, key) {
				offending = append(offending, fmt.Sprintf("%s:%d: %s", filepath.ToSlash(rel), u.pos.Line, u.what))
			}
		}
	}
	for _, o := range offending {
		t.Errorf("%s: parse ids from outside with domain.ParseID (the canonical form only)", o)
	}
	for _, a := range allowedLenientUses {
		if n := countOf(found, a); n != 1 {
			t.Errorf("the allowed use %q was found %d times, want once: the scan is broken or the list is out of date", a, n)
		}
	}
}

// The scan finds every route it claims to find: a fixture that takes each
// of them, and one that takes none, are type-checked with the module's
// dependencies.
func TestLenientUUIDScan_FindsDirectAndIndirectRoutes(t *testing.T) {
	m := loadModule(t)
	const src = `package fixture

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/oapi-codegen/runtime"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

type body struct {
	Items []struct{ ID *uuid.UUID }
}

type byID map[uuid.UUID]int

func routes(b []byte, s string) {
	_, _ = uuid.Parse(s)                                  // want uuid.Parse
	_, _ = uuid.ParseBytes(b)                             // want uuid.ParseBytes
	_ = uuid.MustParse(s)                                 // want uuid.MustParse
	_ = uuid.Validate(s)                                  // want uuid.Validate
	parse := uuid.Parse                                   // want uuid.Parse
	_, _ = parse(s)
	var id uuid.UUID
	_ = id.UnmarshalText(b)                               // want uuid.UUID.UnmarshalText
	_ = id.Scan(s)                                        // want uuid.UUID.Scan
	var n uuid.NullUUID
	_ = n.UnmarshalJSON(b)                                // want uuid.NullUUID.UnmarshalJSON
	_ = json.Unmarshal(b, &id)                            // want json.Unmarshal into *uuid.UUID
	var x body
	_ = json.NewDecoder(strings.NewReader(s)).Decode(&x)  // want json.Decode into *fixture.body
	var k byID
	_ = json.Unmarshal(b, &k)                             // want json.Unmarshal into *fixture.byID
	var p openapi_types.UUID
	_ = runtime.BindStyledParameterWithOptions("simple", "id", s, &p, runtime.BindStyledParameterOptions{}) // want runtime.BindStyledParameterWithOptions into *types.UUID
}

func clean(b []byte, s string) {
	var str string
	_ = json.Unmarshal(b, &str)
	var m map[string]json.RawMessage
	_ = json.Unmarshal(b, &m)
	_ = runtime.BindStyledParameterWithOptions("simple", "id", s, &str, runtime.BindStyledParameterOptions{})
	_, _ = json.Marshal(uuid.New())
	_ = uuid.New().String()
}
`
	f, err := parser.ParseFile(m.fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	info := newInfo()
	conf := types.Config{Importer: m.importer}
	if _, err := conf.Check("fixture", m.fset, []*ast.File{f}, info); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i, line := range strings.Split(src, "\n") {
		if _, w, ok := strings.Cut(line, "// want "); ok {
			want = append(want, fmt.Sprintf("%d: %s", i+1, strings.TrimSpace(w)))
		}
	}
	var got []string
	for _, u := range lenientUses(m.fset, []*ast.File{f}, info) {
		got = append(got, fmt.Sprintf("%d: %s", u.pos.Line, u.what))
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("found\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// lenientUse is one place that reaches the lenient parser.
type lenientUse struct {
	pos  token.Position
	what string
}

// lenientUses returns the uses of the lenient parser in the files.
func lenientUses(fset *token.FileSet, files []*ast.File, info *types.Info) []lenientUse {
	var out []lenientUse
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Ident:
				if what := lenientObject(info.Uses[n]); what != "" {
					out = append(out, lenientUse{fset.Position(n.Pos()), what})
				}
			case *ast.CallExpr:
				fn := callee(info, n)
				if fn == nil || !isDecoder(fn) {
					return true
				}
				for _, arg := range n.Args {
					if tv := info.TypeOf(arg); tv != nil && holdsUUID(tv, map[types.Type]bool{}) {
						what := fmt.Sprintf("%s.%s into %s", fn.Pkg().Name(), fn.Name(),
							types.TypeString(tv, func(p *types.Package) string { return p.Name() }))
						out = append(out, lenientUse{fset.Position(n.Pos()), what})
					}
				}
			}
			return true
		})
	}
	return out
}

// lenientObject names obj when it is one of the library's lenient parsers.
func lenientObject(obj types.Object) string {
	fn, ok := obj.(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != uuidPath {
		return ""
	}
	recv := fn.Signature().Recv()
	if recv == nil {
		if slices.Contains(lenientFuncs, fn.Name()) {
			return "uuid." + fn.Name()
		}
		return ""
	}
	if !slices.Contains(lenientMethods, fn.Name()) {
		return ""
	}
	t := recv.Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if named, ok := types.Unalias(t).(*types.Named); ok {
		return "uuid." + named.Obj().Name() + "." + fn.Name()
	}
	return "uuid." + fn.Name()
}

// callee returns the function or method a call calls, or nil.
func callee(info *types.Info, call *ast.CallExpr) *types.Func {
	var id *ast.Ident
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		id = fun
	case *ast.SelectorExpr:
		id = fun.Sel
	default:
		return nil
	}
	fn, _ := info.Uses[id].(*types.Func)
	return fn
}

func isDecoder(fn *types.Func) bool {
	if fn.Pkg() == nil {
		return false
	}
	for _, name := range decoders[fn.Pkg().Path()] {
		if prefix, ok := strings.CutSuffix(name, "*"); ok && strings.HasPrefix(fn.Name(), prefix) || name == fn.Name() {
			return true
		}
	}
	return false
}

// holdsUUID reports whether a value of type t holds a type of the UUID
// library, through pointers, slices, arrays, maps (keys as well: a key that
// implements encoding.TextUnmarshaler is decoded with it) and struct fields.
func holdsUUID(t types.Type, seen map[types.Type]bool) bool {
	if seen[t] {
		return false
	}
	seen[t] = true
	switch t := types.Unalias(t).(type) {
	case *types.Named:
		if pkg := t.Obj().Pkg(); pkg != nil && pkg.Path() == uuidPath {
			return true
		}
		return holdsUUID(t.Underlying(), seen)
	case *types.Pointer:
		return holdsUUID(t.Elem(), seen)
	case *types.Slice:
		return holdsUUID(t.Elem(), seen)
	case *types.Array:
		return holdsUUID(t.Elem(), seen)
	case *types.Map:
		return holdsUUID(t.Key(), seen) || holdsUUID(t.Elem(), seen)
	case *types.Struct:
		for i := range t.NumFields() {
			if holdsUUID(t.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func countOf(list []string, s string) int {
	n := 0
	for _, v := range list {
		if v == s {
			n++
		}
	}
	return n
}

func newInfo() *types.Info {
	return &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{},
		Uses:  map[*ast.Ident]types.Object{},
		Defs:  map[*ast.Ident]types.Object{},
	}
}

// module is the module's non-test packages, type-checked from source, with
// an importer that reads the export data of every package they depend on.
type module struct {
	root     string
	fset     *token.FileSet
	importer types.Importer
	pkgs     []checkedPackage
	// typeErrors are the errors of the packages that do not type-check.
	typeErrors []error
}

type checkedPackage struct {
	path  string
	files []*ast.File
	info  *types.Info
}

// listedPackage is the part of `go list -json` the scan uses.
type listedPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	Export     string
	DepOnly    bool
	Error      *struct{ Err string }
}

var (
	moduleOnce sync.Once
	moduleVal  *module
	moduleErr  error
)

// loadModule type-checks the module once per test binary.
func loadModule(t *testing.T) *module {
	t.Helper()
	moduleOnce.Do(func() { moduleVal, moduleErr = typeCheckModule(t.Context()) })
	if moduleErr != nil {
		t.Fatal(moduleErr)
	}
	return moduleVal
}

func typeCheckModule(ctx context.Context) (*module, error) {
	root, err := findModuleRoot()
	if err != nil {
		return nil, err
	}
	// -export builds the export data of every package (from the build
	// cache when it is warm); -deps adds the dependencies of the module's
	// packages, marked DepOnly.
	cmd := exec.CommandContext(ctx, "go", "list", "-e", "-export", "-deps",
		"-json=ImportPath,Dir,GoFiles,CgoFiles,Export,DepOnly,Error", "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var typeErrors []error
	if err != nil {
		// A package that does not compile has no export data; go list
		// still lists every package, and the scan goes on without it.
		if len(out) == 0 {
			return nil, fmt.Errorf("go list: %w: %s", err, stderr.String())
		}
		typeErrors = append(typeErrors, fmt.Errorf("go list: %w: %s", err, stderr.String()))
	}
	exports := map[string]string{}
	var own []listedPackage
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var p listedPackage
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("go list output: %w", err)
		}
		if p.Export != "" {
			exports[p.ImportPath] = p.Export
		}
		if !p.DepOnly && len(p.GoFiles)+len(p.CgoFiles) > 0 {
			if p.Error != nil {
				typeErrors = append(typeErrors, fmt.Errorf("package %s: %s", p.ImportPath, p.Error.Err))
			}
			own = append(own, p)
		}
	}
	if len(own) == 0 {
		return nil, errors.New("go list found no package of the module")
	}

	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(file) //nolint:gosec // a file of the build cache named by go list
	})
	m := &module{root: root, fset: fset, importer: imp, typeErrors: typeErrors}
	for _, p := range own {
		if len(p.CgoFiles) > 0 {
			return nil, fmt.Errorf("package %s has cgo files, which the scan does not type-check", p.ImportPath)
		}
		var files []*ast.File
		for _, name := range p.GoFiles {
			f, err := parser.ParseFile(fset, filepath.Join(p.Dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		info := newInfo()
		conf := types.Config{Importer: imp, Error: func(err error) {
			m.typeErrors = append(m.typeErrors, fmt.Errorf("%s: %w", p.ImportPath, err))
		}}
		_, _ = conf.Check(p.ImportPath, fset, files, info) // the errors are in m.typeErrors
		m.pkgs = append(m.pkgs, checkedPackage{path: p.ImportPath, files: files, info: info})
	}
	return m, nil
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found")
		}
		dir = parent
	}
}
