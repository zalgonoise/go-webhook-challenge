package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgres(ctx context.Context) (*postgres.PostgresContainer, string, error) {
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:17-alpine"),
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		return nil, "", err
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", err
	}

	return pgContainer, connStr, nil
}

func TestOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pgContainer, connStr, err := setupPostgres(ctx)
	require.NoError(t, err)
	defer pgContainer.Terminate(ctx)

	t.Run("Success", func(t *testing.T) {
		db, err := Open(connStr, 10, 5)

		require.NoError(t, err)
		require.NotNil(t, db)
		defer db.Close()

		stats := db.Stats()
		require.Equal(t, 10, stats.MaxOpenConnections)
	})

	t.Run("FailureInvalidConnection", func(t *testing.T) {
		db, err := Open("postgres://invalid:user@localhost:1234/db", 10, 5)

		require.Error(t, err)
		require.Nil(t, db)
	})
}

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pgContainer, connStr, err := setupPostgres(ctx)
	require.NoError(t, err)
	defer pgContainer.Terminate(ctx)

	db, err := Open(connStr, 10, 5)
	require.NoError(t, err)
	defer db.Close()

	err = Migrate(db)

	require.NoError(t, err)

	var tableName string
	query := `SELECT table_name FROM information_schema.tables WHERE table_name = 'webhook_jobs'`
	err = db.QueryRowContext(ctx, query).Scan(&tableName)

	require.NoError(t, err)
	require.Equal(t, "webhook_jobs", tableName)
}
