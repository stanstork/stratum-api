-- +goose Up

ALTER TABLE tenant.connections
    ADD COLUMN tenant_id UUID;

ALTER TABLE tenant.job_executions
    ADD COLUMN tenant_id UUID;

-- Backfill connection tenant assignments based on existing job definitions
UPDATE tenant.connections AS c
SET tenant_id = jd.tenant_id
FROM tenant.job_definitions AS jd
WHERE (jd.source_connection_id = c.id OR jd.destination_connection_id = c.id)
  AND c.tenant_id IS NULL;

-- Fallback: assign remaining connections to the oldest tenant to satisfy NOT NULL constraint
WITH fallback AS (
    SELECT id FROM tenant.tenants ORDER BY created_at LIMIT 1
)
UPDATE tenant.connections AS c
SET tenant_id = fallback.id
FROM fallback
WHERE c.tenant_id IS NULL;

-- Backfill executions from their job definitions
UPDATE tenant.job_executions AS je
SET tenant_id = jd.tenant_id
FROM tenant.job_definitions AS jd
WHERE je.job_definition_id = jd.id
  AND je.tenant_id IS NULL;

-- Fallback for executions
WITH fallback AS (
    SELECT id FROM tenant.tenants ORDER BY created_at LIMIT 1
)
UPDATE tenant.job_executions AS je
SET tenant_id = fallback.id
FROM fallback
WHERE je.tenant_id IS NULL;

ALTER TABLE tenant.connections
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE tenant.job_executions
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE tenant.connections
    ADD CONSTRAINT connections_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenant.tenants(id) ON DELETE CASCADE;

ALTER TABLE tenant.connections
    DROP CONSTRAINT IF EXISTS connections_name_key,
    ADD CONSTRAINT connections_name_per_tenant UNIQUE (tenant_id, name);

ALTER TABLE tenant.job_executions
    ADD CONSTRAINT job_executions_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenant.tenants(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_connections_tenant ON tenant.connections(tenant_id);
CREATE INDEX IF NOT EXISTS idx_job_executions_tenant ON tenant.job_executions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_job_executions_job_tenant ON tenant.job_executions(tenant_id, job_definition_id);

-- +goose Down

DROP INDEX IF EXISTS idx_job_executions_job_tenant;
DROP INDEX IF EXISTS idx_job_executions_tenant;
DROP INDEX IF EXISTS idx_connections_tenant;

ALTER TABLE tenant.job_executions
    DROP CONSTRAINT IF EXISTS job_executions_tenant_fk;

ALTER TABLE tenant.connections
    DROP CONSTRAINT IF EXISTS connections_name_per_tenant;

ALTER TABLE tenant.connections
    DROP CONSTRAINT IF EXISTS connections_tenant_fk;

ALTER TABLE tenant.job_executions
    DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE tenant.connections
    DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE tenant.connections
    ADD CONSTRAINT connections_name_key UNIQUE (name);
