package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/AlpNuhoglu/event-driven-notification-system/migrations"
)

// PostgresImage is the Postgres image of the tests. It must equal the image
// of the postgres service in docker-compose.yml (checked by a unit test).
const PostgresImage = "postgres:18.7"

// templateDB is the migrated database every test database is cloned from.
const templateDB = "edns_template"

const startTimeout = 3 * time.Minute

type server struct {
	container *tcpostgres.PostgresContainer
	adminURL  string // the container's default database
}

var (
	startOnce sync.Once
	shared    *server
	startErr  error
	// createMu serialises CREATE DATABASE ... TEMPLATE: Postgres refuses to
	// copy a template that another session is copying or connected to.
	createMu sync.Mutex
)

// Main runs the tests of a package and afterwards terminates every container
// a helper of this package started. Integration test packages call it from
// TestMain; without it the containers are still removed by the testcontainers
// reaper when the test binary exits.
func Main(m *testing.M) {
	code := m.Run()
	terminateMu.Lock()
	fns := terminators
	terminateMu.Unlock()
	for _, fn := range fns {
		fn()
	}
	os.Exit(code)
}

var (
	terminateMu sync.Mutex
	terminators []func()
)

// onExit registers the termination of a container a helper started; Main
// runs it after the tests. name appears in the message when it fails.
func onExit(name string, c testcontainers.Container) {
	terminateMu.Lock()
	defer terminateMu.Unlock()
	terminators = append(terminators, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := testcontainers.TerminateContainer(c, testcontainers.StopContext(ctx)); err != nil {
			fmt.Fprintf(os.Stderr, "terminate %s container: %v\n", name, err)
		}
	})
}

func start() (*server, error) {
	type started struct {
		container *tcpostgres.PostgresContainer
		adminURL  string
	}
	st, err := startRetrying("postgres", func() (started, error) {
		ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
		defer cancel()
		c, err := tcpostgres.Run(ctx, PostgresImage,
			tcpostgres.WithDatabase("edns"),
			tcpostgres.WithUsername("edns"),
			tcpostgres.WithPassword("edns"),
			tcpostgres.BasicWaitStrategies(),
			testcontainers.WithCmdArgs("-c", "synchronous_commit=off", "-c", "full_page_writes=off",
				"-c", "max_connections=300"),
		)
		if err != nil {
			if c != nil {
				discard("postgres", c)
			}
			return started{}, fmt.Errorf("start postgres: %w", err)
		}
		adminURL, err := c.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			discard("postgres", c)
			return started{}, fmt.Errorf("connection string: %w", err)
		}
		return started{container: c, adminURL: adminURL}, nil
	})
	if err != nil {
		return nil, err
	}
	c, adminURL := st.container, st.adminURL
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	s := &server{container: c, adminURL: adminURL}
	if err := s.exec(ctx, "CREATE DATABASE "+templateDB); err != nil {
		return nil, errors.Join(err, testcontainers.TerminateContainer(c))
	}
	tmplURL, err := withDatabase(adminURL, templateDB)
	if err != nil {
		return nil, errors.Join(err, testcontainers.TerminateContainer(c))
	}
	if err := migrations.Up(tmplURL); err != nil {
		return nil, errors.Join(fmt.Errorf("migrate template: %w", err), testcontainers.TerminateContainer(c))
	}
	onExit("postgres", c)
	return s, nil
}

func (s *server) exec(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, s.adminURL)
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("%s: %w", sql, err)
	}
	return nil
}

func sharedServer(t testing.TB) *server {
	t.Helper()
	startOnce.Do(func() { shared, startErr = start() })
	if startErr != nil {
		t.Fatalf("postgres: %v", startErr)
	}
	return shared
}

// NewDatabase creates an empty database on the shared Postgres container and
// returns its URL. The container is started on first use. The database is
// dropped when the test ends.
func NewDatabase(t testing.TB) string {
	t.Helper()
	return newDatabase(t, "")
}

// NewMigratedDatabase creates a database with every migration applied (a
// copy of a migrated template) and returns its URL. Each test gets its own,
// so tests that scan whole tables do not see each other's rows. The database
// is dropped when the test ends.
func NewMigratedDatabase(t testing.TB) string {
	t.Helper()
	return newDatabase(t, templateDB)
}

func newDatabase(t testing.TB, template string) string {
	t.Helper()
	s := sharedServer(t)
	name := "t_" + randomHex(t)
	sql := "CREATE DATABASE " + name
	if template != "" {
		sql += " TEMPLATE " + template
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	createMu.Lock()
	err := s.exec(ctx, sql)
	createMu.Unlock()
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := s.exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database: %v", err)
		}
	})
	u, err := withDatabase(s.adminURL, name)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// PoolOpener opens a connection pool on a database URL. The tests pass
// store.OpenPool, the constructor of every role's pool (D79), so that they
// run with the plans production runs with. This package cannot call it
// itself: the store's own tests import testutil.
type PoolOpener func(ctx context.Context, databaseURL string) (*pgxpool.Pool, error)

// NewPool returns a connection pool, opened with open, on a new migrated
// database (see NewMigratedDatabase). The pool is closed when the test ends,
// before the database is dropped.
func NewPool(t testing.TB, open PoolOpener) *pgxpool.Pool {
	t.Helper()
	u := NewMigratedDatabase(t)
	pool, err := open(context.Background(), u)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func withDatabase(rawURL, name string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse database url: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}

func randomHex(t testing.TB) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}
