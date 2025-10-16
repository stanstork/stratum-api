-- +goose Up

ALTER TABLE tenant.users
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE tenant.connections
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE tenant.job_definitions
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE tenant.invites
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Ensure indexes and uniqueness constraints ignore soft-deleted rows
ALTER TABLE tenant.users
    DROP CONSTRAINT IF EXISTS users_email_key,
    DROP CONSTRAINT IF EXISTS users_tenant_id_email_key;

DROP INDEX IF EXISTS idx_users_tenant_active;
CREATE UNIQUE INDEX IF NOT EXISTS users_email_key_active
    ON tenant.users (email) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS users_tenant_id_email_key_active
    ON tenant.users (tenant_id, email) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_users_tenant_active
    ON tenant.users (tenant_id) WHERE deleted_at IS NULL;

ALTER TABLE tenant.job_definitions
    DROP CONSTRAINT IF EXISTS job_definitions_tenant_id_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS job_definitions_tenant_name_active
    ON tenant.job_definitions (tenant_id, name) WHERE deleted_at IS NULL;

ALTER TABLE tenant.connections
    DROP CONSTRAINT IF EXISTS connections_name_per_tenant,
    DROP CONSTRAINT IF EXISTS connections_name_key;

DROP INDEX IF EXISTS idx_connections_tenant;
CREATE UNIQUE INDEX IF NOT EXISTS connections_tenant_name_active
    ON tenant.connections (tenant_id, name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_connections_tenant
    ON tenant.connections (tenant_id) WHERE deleted_at IS NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_connections_tenant;
DROP INDEX IF EXISTS connections_tenant_name_active;

DROP INDEX IF EXISTS job_definitions_tenant_name_active;

DROP INDEX IF EXISTS idx_users_tenant_active;
DROP INDEX IF EXISTS users_tenant_id_email_key_active;
DROP INDEX IF EXISTS users_email_key_active;

ALTER TABLE tenant.connections
    ADD CONSTRAINT connections_name_per_tenant UNIQUE (tenant_id, name);

ALTER TABLE tenant.job_definitions
    ADD CONSTRAINT job_definitions_tenant_id_name_key UNIQUE (tenant_id, name);

ALTER TABLE tenant.users
    ADD CONSTRAINT users_tenant_id_email_key UNIQUE (tenant_id, email),
    ADD CONSTRAINT users_email_key UNIQUE (email);

ALTER TABLE tenant.invites
    DROP COLUMN IF EXISTS deleted_at;

ALTER TABLE tenant.job_definitions
    DROP COLUMN IF EXISTS deleted_at;

ALTER TABLE tenant.connections
    DROP COLUMN IF EXISTS deleted_at;

ALTER TABLE tenant.users
    DROP COLUMN IF EXISTS deleted_at;

CREATE INDEX IF NOT EXISTS idx_users_tenant_active
    ON tenant.users (tenant_id, is_active);

CREATE INDEX IF NOT EXISTS idx_connections_tenant
    ON tenant.connections (tenant_id);
