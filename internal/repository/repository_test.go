package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zalgonoise/go-webhook-challenge/internal/database"
	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
)

func setupTestDB(ctx context.Context, t *testing.T) (*Repository, func()) {
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
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := database.Open(connStr, 10, 5)
	require.NoError(t, err)

	err = database.Migrate(db)
	require.NoError(t, err)

	repo := NewRepository(db)

	cleanup := func() {
		db.Close()
		pgContainer.Terminate(ctx)
	}

	return repo, cleanup
}

func TestRepository_Insert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	repo, cleanup := setupTestDB(ctx, t)
	defer cleanup()

	t.Run("Success", func(t *testing.T) {
		id, err := repo.Insert(ctx, []byte(`{"data":"test"}`), "test.event", "http://test.com", "sig123")

		require.NoError(t, err)
		assert.NotEmpty(t, id)

		var status string
		err = repo.db.QueryRowContext(ctx, "SELECT status FROM webhook_jobs WHERE id = $1", id).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", status)
	})
}

func TestRepository_FetchNextJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	repo, cleanup := setupTestDB(ctx, t)
	defer cleanup()

	t.Run("Success", func(t *testing.T) {
		id, err := repo.Insert(ctx, []byte(`{"data":"test"}`), "test.event", "http://test.com", "sig123")
		require.NoError(t, err)

		jobTx, err := repo.FetchNextJob(ctx)
		require.NoError(t, err)
		require.NotNil(t, jobTx)
		defer jobTx.Rollback()

		assert.Equal(t, id, jobTx.Job.Id)
		assert.Equal(t, pb.Status_StatusProcessing, jobTx.Job.Status)

		var status string
		err = repo.db.QueryRowContext(ctx, "SELECT status FROM webhook_jobs WHERE id = $1", id).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", status)

		err = jobTx.Commit()
		require.NoError(t, err)
	})

	t.Run("NoJobsScheduled", func(t *testing.T) {
		jobTx, err := repo.FetchNextJob(ctx)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoJobsScheduled)
		assert.Nil(t, jobTx)
	})

	t.Run("SkipsFutureJobs", func(t *testing.T) {
		_, err := repo.db.ExecContext(ctx,
			"INSERT INTO webhook_jobs (event_type, payload, destination_url, signature, next_retry_at) VALUES ($1, $2, $3, $4, $5)",
			"test.event", []byte(`{}`), "http://test.com", "sig123", time.Now().Add(time.Hour),
		)
		require.NoError(t, err)

		jobTx, err := repo.FetchNextJob(ctx)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNoJobsScheduled)
		assert.Nil(t, jobTx)
	})
}

func TestRepository_UpdateJobStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	runTest := func(t *testing.T, statusToSet pb.Status, verificationQuery string, expectedValue any) {
		repo, cleanup := setupTestDB(ctx, t)
		defer cleanup()

		id, err := repo.Insert(ctx, []byte(`{}`), "test.event", "http://test.com", "sig123")
		require.NoError(t, err)

		nextRun := time.Now().Add(time.Minute)
		err = repo.UpdateJobStatus(ctx, id, statusToSet, &nextRun)
		require.NoError(t, err)

		var actualValue any
		switch expectedValue.(type) {
		case time.Time:
			actualValue = new(time.Time)
		default:
			actualValue = new(string)
		}

		err = repo.db.QueryRowContext(ctx, verificationQuery, id).Scan(actualValue)
		require.NoError(t, err)

		switch v := expectedValue.(type) {
		case time.Time:
			assert.WithinDuration(t, v, *actualValue.(*time.Time), time.Second)
		default:
			assert.Equal(t, v, *actualValue.(*string))
		}
	}

	t.Run("SetCompleted", func(t *testing.T) {
		runTest(t, pb.Status_StatusCompleted, "SELECT status FROM webhook_jobs WHERE id = $1", "COMPLETED")
	})

	t.Run("SetDropped", func(t *testing.T) {
		runTest(t, pb.Status_StatusDropped, "SELECT status FROM webhook_jobs WHERE id = $1", "DROPPED")
	})

	t.Run("SetPending", func(t *testing.T) {
		runTest(t, pb.Status_StatusPending, "SELECT status FROM webhook_jobs WHERE id = $1", "PENDING")
	})

	t.Run("SetFailed", func(t *testing.T) {
		repo, cleanup := setupTestDB(ctx, t)
		defer cleanup()

		id, err := repo.Insert(ctx, []byte(`{}`), "test.event", "http://test.com", "sig123")
		require.NoError(t, err)

		nextRun := time.Now().Add(time.Minute)
		err = repo.UpdateJobStatus(ctx, id, pb.Status_StatusFailed, &nextRun)
		require.NoError(t, err)

		var status string
		var attemptCount int
		var nextRetryAt time.Time
		err = repo.db.QueryRowContext(ctx, "SELECT status, attempt_count, next_retry_at FROM webhook_jobs WHERE id = $1", id).Scan(&status, &attemptCount, &nextRetryAt)
		require.NoError(t, err)

		assert.Equal(t, "FAILED", status)
		assert.Equal(t, 1, attemptCount)
		assert.WithinDuration(t, nextRun, nextRetryAt, time.Second)
	})
}
