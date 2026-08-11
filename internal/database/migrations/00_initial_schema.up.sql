-- state machine for the processed jobs
CREATE TYPE job_status AS ENUM (
    'PENDING',    -- ready to be picked up
    'PROCESSING', -- currently being handled by a worker
    'COMPLETED',  -- successfully delivered (status code 2xx received)
    'FAILED',     -- delivery failed, waiting for next retry
    'DROPPED'     -- max attempts reached, permanent failure
);

CREATE TABLE webhook_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  destination_url TEXT NOT NULL,
  signature TEXT NOT NULL,
  status job_status NOT NULL DEFAULT 'PENDING',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- index status and next_retry_at for the poller
CREATE INDEX idx_webhook_jobs_status_retry ON webhook_jobs (status, next_retry_at);