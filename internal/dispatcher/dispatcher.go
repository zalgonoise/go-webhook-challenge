package dispatcher

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"

	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidRequest = status.Error(codes.InvalidArgument, "invalid request")
	ErrFailedToInsert = status.Error(codes.Internal, "failed to accept payment request")
)

type Repository interface {
	Insert(ctx context.Context, payload []byte, eventType, destinationURL, signature string) (string, error)
}

type Dispatcher struct {
	pb.UnimplementedDispatcherServiceServer

	hmacSecret []byte

	repo   Repository
	logger *slog.Logger
}

func NewDispatcher(hmacSecret string, repo Repository, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		repo:       repo,
		logger:     logger,
		hmacSecret: []byte(hmacSecret),
	}
}

func (d *Dispatcher) generateSignature(payload []byte) string {
	mac := hmac.New(sha256.New, d.hmacSecret)
	mac.Write(payload)

	return hex.EncodeToString(mac.Sum(nil))
}

func (d *Dispatcher) Dispatch(ctx context.Context, req *pb.DispatchRequest) (*pb.DispatchResponse, error) {
	if err := req.ValidateAll(); err != nil {
		d.logger.WarnContext(ctx, "invalid request", slog.String("error", err.Error()), slog.Any("request", req))

		return nil, ErrInvalidRequest
	}

	data, err := req.Data.MarshalJSON()
	if err != nil {
		return nil, ErrInvalidRequest
	}

	signature := d.generateSignature(data)

	id, err := d.repo.Insert(ctx, data, req.EventType, req.DestinationUrl, signature)
	if err != nil {
		d.logger.ErrorContext(ctx, "inserting job", slog.String("error", err.Error()))

		return nil, ErrFailedToInsert
	}

	return &pb.DispatchResponse{
		Status: pb.Status_StatusPending,
		JobId:  id,
	}, nil
}
