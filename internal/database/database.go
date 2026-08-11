package database

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs" // for iofs source
	_ "github.com/jackc/pgx/v5/stdlib"                 // for pgx driver
)

//go:embed migrations/*.sql
var migrations embed.FS

func Open(uri string, maxConns, maxIdleConns int) (*sql.DB, error) {
	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxIdleConns)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func Migrate(db *sql.DB) error {
	fs, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("could not create iofs source: %w", err)
	}

	// 1. Create the database driver instance
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("could not create postgres driver instance: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", fs, "pgx", driver)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	return nil
}
