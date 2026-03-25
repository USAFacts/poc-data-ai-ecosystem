-- Pipeline logging table — created on first docker compose up.
-- The SQLAlchemy model (db/models.py PipelineLogModel) is the source of truth;
-- this script bootstraps the table so logs can be written before any Python
-- migration runs.

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id            SERIAL PRIMARY KEY,
    timestamp     TIMESTAMPTZ NOT NULL DEFAULT now(),
    level         VARCHAR(10)  NOT NULL,
    logger_name   VARCHAR(255) NOT NULL,
    message       TEXT         NOT NULL,
    run_id        VARCHAR(255),
    workflow      VARCHAR(255),
    step          VARCHAR(255),
    asset         VARCHAR(255),
    extra         JSONB
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_timestamp ON pipeline_logs (timestamp);
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_level     ON pipeline_logs (level);
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_run_id    ON pipeline_logs (run_id);
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_workflow  ON pipeline_logs (workflow);

-- Add run_id column if upgrading from an older schema
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pipeline_logs' AND column_name = 'run_id'
    ) THEN
        ALTER TABLE pipeline_logs ADD COLUMN run_id VARCHAR(255);
        CREATE INDEX IF NOT EXISTS ix_pipeline_logs_run_id ON pipeline_logs (run_id);
    END IF;
END $$;
