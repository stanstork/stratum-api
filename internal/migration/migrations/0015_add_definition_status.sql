-- +goose Up

ALTER TABLE tenant.job_definitions
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'READY',
    ADD COLUMN IF NOT EXISTS progress_snapshot JSONB,
    ADD CONSTRAINT job_definitions_status_check CHECK (status IN ('DRAFT', 'VALIDATING', 'READY'));

ALTER TABLE tenant.job_definitions
    ALTER COLUMN name DROP NOT NULL,
    ALTER COLUMN ast DROP NOT NULL,
    ALTER COLUMN source_connection_id DROP NOT NULL,
    ALTER COLUMN destination_connection_id DROP NOT NULL;

ALTER TABLE tenant.job_definitions
    ADD CONSTRAINT job_definitions_ready_requirements
    CHECK (
        status <> 'READY'
        OR (
            name IS NOT NULL
            AND ast IS NOT NULL
            AND source_connection_id IS NOT NULL
            AND destination_connection_id IS NOT NULL
        )
    );

CREATE TABLE IF NOT EXISTS tenant.job_definition_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_definition_id UUID NOT NULL REFERENCES tenant.job_definitions(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'VALIDATING', 'READY')),
    snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_definition_snapshots_definition
    ON tenant.job_definition_snapshots (job_definition_id);

-- +goose Down

DROP INDEX IF EXISTS idx_job_definition_snapshots_definition;
DROP TABLE IF EXISTS tenant.job_definition_snapshots;

ALTER TABLE tenant.job_definitions
    DROP CONSTRAINT IF EXISTS job_definitions_ready_requirements;

ALTER TABLE tenant.job_definitions
    ALTER COLUMN destination_connection_id SET NOT NULL,
    ALTER COLUMN source_connection_id SET NOT NULL,
    ALTER COLUMN ast SET NOT NULL,
    ALTER COLUMN name SET NOT NULL;

ALTER TABLE tenant.job_definitions
    DROP CONSTRAINT IF EXISTS job_definitions_status_check,
    DROP COLUMN IF EXISTS progress_snapshot,
    DROP COLUMN IF EXISTS status;
