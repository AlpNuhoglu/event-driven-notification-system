// Package migrations embeds the versioned SQL schema files and applies them
// with golang-migrate.
package migrations

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

//go:embed *.sql
var files embed.FS

// Up applies every pending migration.
func Up(databaseURL string) error {
	return run(databaseURL, func(m *migrate.Migrate) error { return m.Up() })
}

// Down reverts every applied migration.
func Down(databaseURL string) error {
	return run(databaseURL, func(m *migrate.Migrate) error { return m.Down() })
}

func run(databaseURL string, apply func(*migrate.Migrate) error) (err error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return errors.Join(fmt.Errorf("migrate driver: %w", err), db.Close())
	}
	src, err := iofs.New(files, ".")
	if err != nil {
		return errors.Join(fmt.Errorf("migration source: %w", err), driver.Close())
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return errors.Join(fmt.Errorf("migrate init: %w", err), src.Close(), driver.Close())
	}
	defer func() {
		srcErr, dbErr := m.Close()
		if err == nil {
			err = errors.Join(srcErr, dbErr)
		}
	}()
	if err := apply(m); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}
