package poller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/zalgonoise/go-webhook-challenge/internal/config"
	"github.com/zalgonoise/go-webhook-challenge/internal/repository"
	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockTx is a mock for the *sql.Tx.
type mockTx struct {
	mock.Mock
}

func (m *mockTx) Commit() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockTx) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

// mockRepository is a mock for the poller's Repository interface.
type mockRepository struct {
	mock.Mock
}

func (m *mockRepository) FetchNextJob(ctx context.Context) (*repository.JobTx, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*repository.JobTx), args.Error(1)
}

func (m *mockRepository) UpdateJobStatus(ctx context.Context, id string, status pb.Status, nextRun *time.Time) error {
	args := m.Called(ctx, id, status, nextRun)
	return args.Error(0)
}

func TestHandleJob(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	testConfig := config.Poller{
		MaxRetries:    3,
		MinRetryDelay: 100 * time.Millisecond,
		MaxRetryDelay: 500 * time.Millisecond,
	}

	baseJob := &pb.DispatchJob{
		Id:           "job-123",
		EventType:    "test.event",
		Payload:      []byte(`{"data":"test"}`),
		Signature:    "test-signature",
		AttemptCount: 0,
		NextRetryAt:  timestamppb.Now(),
	}

	t.Run("Success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		job := *baseJob
		job.DestinationUrl = server.URL

		repo := new(mockRepository)
		tx := new(mockTx)
		jobTx := &repository.JobTx{Job: &job, Tx: tx}
		poller := NewPoller(testConfig, repo, logger)

		tx.On("Commit").Return(nil).Once()
		repo.On("UpdateJobStatus", mock.Anything, job.Id, pb.Status_StatusCompleted, (*time.Time)(nil)).Return(nil).Once()

		err := poller.HandleJob(ctx, jobTx)

		require.NoError(t, err)
		repo.AssertExpectations(t)
		tx.AssertExpectations(t)
	})

	t.Run("Failure/RetryOnNon2xx", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		job := *baseJob
		job.DestinationUrl = server.URL

		repo := new(mockRepository)
		tx := new(mockTx)
		jobTx := &repository.JobTx{Job: &job, Tx: tx}
		poller := NewPoller(testConfig, repo, logger)

		tx.On("Commit").Return(nil).Once()
		repo.On("UpdateJobStatus", mock.Anything, job.Id, pb.Status_StatusFailed, mock.AnythingOfType("*time.Time")).
			Return(nil).
			Once()

		err := poller.HandleJob(ctx, jobTx)

		require.NoError(t, err)
		repo.AssertExpectations(t)
		tx.AssertExpectations(t)
	})

	t.Run("Failure/RetryOnNetworkError", func(t *testing.T) {
		job := *baseJob
		job.DestinationUrl = "http://localhost:9999"

		repo := new(mockRepository)
		tx := new(mockTx)
		jobTx := &repository.JobTx{Job: &job, Tx: tx}
		poller := NewPoller(testConfig, repo, logger)

		tx.On("Commit").Return(nil).Once()
		repo.On("UpdateJobStatus", mock.Anything, job.Id, pb.Status_StatusFailed, mock.AnythingOfType("*time.Time")).
			Return(nil).
			Once()

		err := poller.HandleJob(ctx, jobTx)

		require.NoError(t, err)
		repo.AssertExpectations(t)
		tx.AssertExpectations(t)
	})

	t.Run("Failure/MaxRetriesReached", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		job := *baseJob
		job.DestinationUrl = server.URL
		job.AttemptCount = int32(testConfig.MaxRetries) // Set attempts to max

		repo := new(mockRepository)
		tx := new(mockTx)
		jobTx := &repository.JobTx{Job: &job, Tx: tx}
		poller := NewPoller(testConfig, repo, logger)

		tx.On("Commit").Return(nil).Once()
		repo.On("UpdateJobStatus", mock.Anything, job.Id, pb.Status_StatusDropped, (*time.Time)(nil)).
			Return(nil).
			Once()

		err := poller.HandleJob(ctx, jobTx)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrMaxRetriesReached)
		repo.AssertExpectations(t)
		tx.AssertExpectations(t)
	})

	t.Run("Failure/DropOnRequestCreationError", func(t *testing.T) {
		job := *baseJob
		job.DestinationUrl = "::not-a-valid-url" // Invalid URL to cause http.NewRequest to fail

		repo := new(mockRepository)
		tx := new(mockTx)
		jobTx := &repository.JobTx{Job: &job, Tx: tx}
		poller := NewPoller(testConfig, repo, logger)

		tx.On("Rollback").Return(nil).Once()
		repo.On("UpdateJobStatus", mock.Anything, job.Id, pb.Status_StatusDropped, (*time.Time)(nil)).
			Return(nil).
			Once()

		err := poller.HandleJob(ctx, jobTx)

		require.Error(t, err)
		repo.AssertExpectations(t)
		tx.AssertExpectations(t)
	})
}
