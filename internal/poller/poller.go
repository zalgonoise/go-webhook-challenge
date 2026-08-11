package poller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/zalgonoise/go-webhook-challenge/internal/config"
	"github.com/zalgonoise/go-webhook-challenge/internal/repository"
	pb "github.com/zalgonoise/go-webhook-challenge/pb/dispatcher/v1"
	"golang.org/x/sync/errgroup"
)

var ErrMaxRetriesReached = errors.New("max retries reached")

type Repository interface {
	FetchNextJob(ctx context.Context) (*repository.JobTx, error)
	UpdateJobStatus(ctx context.Context, id string, status pb.Status, nextRun *time.Time) error
}

type Poller struct {
	frequency     time.Duration
	maxJobs       int
	maxRetries    int
	minRetryDelay time.Duration
	maxRetryDelay time.Duration

	repo Repository

	logger *slog.Logger
}

func NewPoller(cfg config.Poller, repo Repository, logger *slog.Logger) *Poller {
	return &Poller{
		frequency:     cfg.Frequency,
		maxJobs:       cfg.MaxJobs,
		maxRetries:    cfg.MaxRetries,
		minRetryDelay: cfg.MinRetryDelay,
		maxRetryDelay: cfg.MaxRetryDelay,
		repo:          repo,
		logger:        logger,
	}
}
func (p *Poller) Poll(ctx context.Context) {
	ticker := time.NewTicker(p.frequency)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.HandleBatch(ctx); err != nil {
				p.logger.ErrorContext(ctx, "polling", slog.String("error", err.Error()))

				continue
			}
		}
	}
}

func (p *Poller) HandleBatch(ctx context.Context) error {
	// make sure we're not overlapping poller runs
	ctx, cancel := context.WithTimeout(ctx, p.frequency-time.Second)
	defer cancel()

	group, gCtx := errgroup.WithContext(ctx)
	group.SetLimit(p.maxJobs)

	for {
		select {
		case <-ctx.Done():
			return group.Wait()
		default:
		}

		job, err := p.repo.FetchNextJob(gCtx)
		if err != nil {
			if errors.Is(err, repository.ErrNoJobsScheduled) {
				return nil
			}

			p.logger.ErrorContext(gCtx, "failed to fetch next job", slog.String("error", err.Error()))
			return err
		}

		group.Go(func() error {
			return p.HandleJob(gCtx, job)
		})
	}
}

func (p *Poller) HandleJob(ctx context.Context, job *repository.JobTx) error {
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &http.Client{
		Transport: &http.Transport{},
		Timeout:   20 * time.Second,
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, job.Job.DestinationUrl, bytes.NewReader(job.Job.Payload))
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to create request", slog.String("error", err.Error()), slog.String("job_id", job.Job.Id))

		return p.dropJob(ctx, job.Job.Id, errors.Join(err, job.Rollback()))
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		p.logger.WarnContext(ctx, "failed to dispatch job, scheduling retry",
			slog.String("error", err.Error()), slog.String("job_id", job.Job.Id))

		if err := job.Commit(); err != nil {
			rollbackErr := job.Rollback()
			p.logger.WarnContext(ctx, "failed to commit", slog.String("error", errors.Join(err, rollbackErr).Error()), slog.String("job_id", job.Job.Id))
		}

		return p.rescheduleJob(ctx, job.Job)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		p.logger.WarnContext(ctx, "job dispatch returned non-2xx status", slog.Int("status_code", resp.StatusCode), slog.String("job_id", job.Job.Id))

		if err := job.Commit(); err != nil {
			rollbackErr := job.Rollback()

			p.logger.WarnContext(ctx, "failed to commit", slog.String("error", errors.Join(err, rollbackErr).Error()), slog.String("job_id", job.Job.Id))
		}

		return p.rescheduleJob(ctx, job.Job)
	}

	if err := job.Commit(); err != nil {
		rollbackErr := job.Rollback()

		p.logger.WarnContext(ctx, "failed to commit transaction", slog.String("error", errors.Join(err, rollbackErr).Error()), slog.String("job_id", job.Job.Id))
	}

	p.logger.InfoContext(ctx, "job dispatched successfully", slog.String("job_id", job.Job.Id), slog.Int("status_code", resp.StatusCode))
	return p.repo.UpdateJobStatus(ctx, job.Job.Id, pb.Status_StatusCompleted, nil)
}

func (p *Poller) rescheduleJob(ctx context.Context, job *pb.DispatchJob) error {
	p.logger.DebugContext(ctx, "rescheduling job", slog.String("job_id", job.Id),
		slog.Int("retry_count", int(job.AttemptCount)),
		slog.Time("next_retry_at", job.NextRetryAt.AsTime()))

	backoffDuration, ok := exponentialBackoff(int(job.AttemptCount), p.maxRetries, p.minRetryDelay, p.maxRetryDelay, p.logger)
	if !ok {
		p.logger.ErrorContext(ctx, "job has reached max retries, marking as failed", slog.String("job_id", job.Id))
		return p.dropJob(ctx, job.Id, ErrMaxRetriesReached)
	}

	nextRun := time.Now().Add(backoffDuration)

	p.logger.InfoContext(ctx, "rescheduling job", slog.String("job_id", job.Id), slog.Time("next_run", nextRun), slog.Int("attempt", int(job.AttemptCount)+1))

	return p.repo.UpdateJobStatus(ctx, job.Id, pb.Status_StatusFailed, &nextRun)
}

func (p *Poller) dropJob(ctx context.Context, jobID string, err error) error {
	if updateErr := p.repo.UpdateJobStatus(ctx, jobID, pb.Status_StatusDropped, nil); updateErr != nil {
		p.logger.ErrorContext(ctx, "failed to mark job as failed after another error",
			slog.String("original_error", err.Error()),
			slog.String("update_error", updateErr.Error()))
		return errors.Join(err, updateErr)
	}

	return err
}

func exponentialBackoff(attempt, maxRetries int, minRetryDelay, maxRetryDelay time.Duration, logger *slog.Logger) (time.Duration, bool) {
	if attempt >= maxRetries {
		logger.Debug("max retries reached", slog.Int("max_retries", maxRetries))
		return 0, false
	}

	backoff := minRetryDelay * (1 << attempt)

	jitter := time.Duration(rand.Int63n(int64(backoff / 4)))

	delay := backoff + jitter

	if delay > maxRetryDelay {
		logger.Debug("delay exceeds max retry delay", slog.Duration("delay", delay), slog.Duration("max_retry_delay", maxRetryDelay))
		return maxRetryDelay, true
	}

	return delay, true
}
