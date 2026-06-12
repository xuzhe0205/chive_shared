-- v0.5.0_chart_snapshot.sql
-- Adds chart_snapshots table for per-(run, ticker) OHLCV + indicators + annotations.
-- Pre-req: v0.4.0 migration applied (runs table exists).

BEGIN;

CREATE TABLE IF NOT EXISTS chart_snapshots (
    run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    ticker      VARCHAR(16) NOT NULL,
    bars        JSONB NOT NULL,
    indicators  JSONB NOT NULL DEFAULT '{}'::jsonb,
    annotations JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, ticker)
);

CREATE INDEX IF NOT EXISTS ix_chart_snapshots_run_id ON chart_snapshots(run_id);

COMMIT;
