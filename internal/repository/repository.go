package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	queryInsert = `
INSERT INTO webhook_jobs (event_type, payload, destination_url, signature)
VALUES ($1, $2, $3, $4)
RETURNING id;
`

	queryFetchNextJob = `
UPDATE webhook_jobs
SET status = 'PROCESSING',
    updated_at = NOW()
WHERE id = (
	SELECT id
	FROM webhook_jobs
	WHERE status IN ('PENDING', 'FAILED')
	  AND next_retry_at <= NOW()
	ORDER BY next_retry_at ASC
	LIMIT 1
	FOR UPDATE SKIP LOCKED
)
RETURNING id, event_type, payload, destination_url, signature,
          status, attempt_count, next_retry_at,
          created_at, updated_at;
`

	queryJobCompleted = `
UPDATE webhook_jobs
SET status = 'COMPLETED',
    updated_at = NOW()
WHERE id = $1;
`

	queryJobFailed = `
UPDATE webhook_jobs
SET status = 'FAILED',
    attempt_count = attempt_count + 1,
    next_retry_at = $2,
    updated_at = NOW()
WHERE id = $1;
`

	queryJobDropped = `
UPDATE webhook_jobs
SET status = 'DROPPED',
    attempt_count = attempt_count + 1,
    updated_at = NOW()
WHERE id = $1;
`

	// used after cancelling existing jobs, as a graceful shutdown is being dispatched
	queryJobPending = `
UPDATE webhook_jobs 
SET status = 'PENDING', 
    updated_at = NOW() 
WHERE id = $1;
`
)

type Transactioner interface {
	Commit() error
	Rollback() error
}

type JobTx struct {
	Job *pb.DispatchJob
	Tx  Transactioner
}

func (tx *JobTx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *JobTx) Rollback() error {
	return tx.Tx.Rollback()
}

var (
	ErrJobCreationFailed     = errors.New("failed to create job")
	ErrNoJobsScheduled       = errors.New("no jobs currently scheduled")
	ErrJobFetchingFailed     = errors.New("failed to fetch the next scheduled job")
	ErrJobStatusUpdateFailed = errors.New("failed to update job status")
	ErrInvalidStatusUpdate   = errors.New("invalid status")
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Insert(ctx context.Context, payload []byte, eventType, destinationURL, signature string) (string, error) {
	var id string

	if err := r.db.QueryRowContext(ctx, queryInsert, eventType, payload, destinationURL, signature).Scan(&id); err != nil {
		return "", fmt.Errorf("%w: %w", ErrJobCreationFailed, err)
	}

	return id, nil
}

func (r *Repository) FetchNextJob(ctx context.Context) (*JobTx, error) {
	var (
		jobID          string
		eventType      string
		payload        []byte
		destinationURL string
		signature      string
		status         string
		attemptCount   int
		nextRetryAt    time.Time
		createdAt      time.Time
		updatedAt      time.Time
	)

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrJobFetchingFailed, err)
	}

	err = tx.QueryRowContext(ctx, queryFetchNextJob).Scan(
		&jobID, &eventType, &payload, &destinationURL, &signature,
		&status, &attemptCount, &nextRetryAt,
		&createdAt, &updatedAt)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, errors.Join(ErrNoJobsScheduled, tx.Rollback())
	case err != nil:
		return nil, errors.Join(fmt.Errorf("%w: %w", ErrJobFetchingFailed, err), tx.Rollback())
	default:
		var pbStatus pb.Status

		switch status {
		case "PENDING":
			pbStatus = pb.Status_StatusPending
		case "PROCESSING":
			pbStatus = pb.Status_StatusProcessing
		case "COMPLETED":
			pbStatus = pb.Status_StatusCompleted
		case "FAILED":
			pbStatus = pb.Status_StatusFailed
		case "DROPPED":
			pbStatus = pb.Status_StatusDropped
		}

		return &JobTx{
			Job: &pb.DispatchJob{
				Id:             jobID,
				EventType:      eventType,
				Payload:        payload,
				DestinationUrl: destinationURL,
				Signature:      signature,
				Status:         pbStatus,
				AttemptCount:   int32(attemptCount),
				NextRetryAt:    timestamppb.New(nextRetryAt),
				CreatedAt:      timestamppb.New(createdAt),
				UpdatedAt:      timestamppb.New(updatedAt),
			},
			Tx: tx,
		}, nil
	}
}

func (r *Repository) UpdateJobStatus(ctx context.Context, id string, status pb.Status, nextRun *time.Time) error {
	var (
		res sql.Result
		err error
	)

	switch status {
	case pb.Status_StatusCompleted:
		res, err = r.db.ExecContext(ctx, queryJobCompleted, id)
	case pb.Status_StatusFailed:
		if nextRun == nil {
			now := time.Now()
			nextRun = &now
		}

		res, err = r.db.ExecContext(ctx, queryJobFailed, id, *nextRun)
	case pb.Status_StatusDropped:
		res, err = r.db.ExecContext(ctx, queryJobDropped, id)
	case pb.Status_StatusPending:
		res, err = r.db.ExecContext(ctx, queryJobPending, id)
	default:
		return fmt.Errorf("%w: %s", ErrInvalidStatusUpdate, status.String())
	}

	if err != nil {
		return fmt.Errorf("%w: %w", ErrJobStatusUpdateFailed, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrJobStatusUpdateFailed, err)
	}

	if n != 1 {
		return ErrJobStatusUpdateFailed
	}

	return nil
}
