package dispatcher

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockRepository struct {
	mock.Mock
}

func (m *mockRepository) Insert(ctx context.Context, payload []byte, eventType, destinationURL, signature string) (string, error) {
	args := m.Called(ctx, payload, eventType, destinationURL, signature)
	return args.String(0), args.Error(1)
}

func TestDispatch(t *testing.T) {
	const (
		testSecret    = "test-secret"
		testEventType = "payment.created"
		testDestURL   = "http://example.com/webhook"
		expectedJobID = "job-123"
	)

	payload, err := structpb.NewStruct(map[string]any{
		"id":     "pay_123",
		"amount": 1000,
	})
	require.NoError(t, err)

	payloadBytes, err := payload.MarshalJSON()
	require.NoError(t, err)

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	t.Run("Success", func(t *testing.T) {
		repo := new(mockRepository)
		dispatcher := NewDispatcher(testSecret, repo, logger)

		req := &pb.DispatchRequest{
			EventType:      testEventType,
			DestinationUrl: testDestURL,
			Data:           payload,
		}

		buf, err := payload.MarshalJSON()
		require.NoError(t, err)

		mac := hmac.New(sha256.New, []byte(testSecret))
		mac.Write(buf)

		signature := hex.EncodeToString(mac.Sum(nil))

		repo.On("Insert", ctx, payloadBytes, testEventType, testDestURL, signature).
			Return(expectedJobID, nil).
			Once()

		res, err := dispatcher.Dispatch(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, expectedJobID, res.JobId)
		assert.Equal(t, pb.Status_StatusPending, res.Status)
		repo.AssertExpectations(t)
	})

	t.Run("Failure/InvalidRequest", func(t *testing.T) {
		repo := new(mockRepository)
		dispatcher := NewDispatcher(testSecret, repo, logger)

		req := &pb.DispatchRequest{
			DestinationUrl: testDestURL,
			Data:           payload,
		}

		res, err := dispatcher.Dispatch(ctx, req)

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, ErrInvalidRequest, err)
		repo.AssertNotCalled(t, "Insert")
	})

	t.Run("Failure/RepositoryError", func(t *testing.T) {
		repo := new(mockRepository)
		dispatcher := NewDispatcher(testSecret, repo, logger)
		repoErr := errors.New("database connection failed")

		req := &pb.DispatchRequest{
			EventType:      testEventType,
			DestinationUrl: testDestURL,
			Data:           payload,
		}

		buf, err := payload.MarshalJSON()
		require.NoError(t, err)

		mac := hmac.New(sha256.New, []byte(testSecret))
		mac.Write(buf)

		signature := hex.EncodeToString(mac.Sum(nil))

		repo.On("Insert", ctx, payloadBytes, testEventType, testDestURL, signature).
			Return("", repoErr).
			Once()

		res, err := dispatcher.Dispatch(ctx, req)

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, ErrFailedToInsert, err)
		repo.AssertExpectations(t)
	})
}
