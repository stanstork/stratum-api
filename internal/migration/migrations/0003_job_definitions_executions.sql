-- +goose Up

-- +goose StatementBegin
-- Trigger to update updated_at timestamps
CREATE OR REPLACE FUNCTION tenant.set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS tenant.job_definitions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenant.tenants(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  ast JSONB NOT NULL, -- Parsed DSL AST
  source_connection JSONB NULL,
  destination_connection JSONB NULL,
  engine_settings JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(tenant_id, name)
);

CREATE TABLE IF NOT EXISTS tenant.job_executions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_definition_id UUID NOT NULL REFERENCES tenant.job_definitions(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK(status IN('pending','running','succeeded','failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  run_started_at TIMESTAMPTZ,
  run_completed_at TIMESTAMPTZ,
  error_message TEXT,
  logs TEXT
);

-- Triggers to maintain updated_at
CREATE TRIGGER trg_job_definitions_updated_at
  BEFORE UPDATE ON tenant.job_definitions
  FOR EACH ROW EXECUTE FUNCTION tenant.set_updated_at();

CREATE TRIGGER trg_job_executions_updated_at
  BEFORE UPDATE ON tenant.job_executions
  FOR EACH ROW EXECUTE FUNCTION tenant.set_updated_at();

-- +goose Down
DROP TRIGGER IF EXISTS trg_job_executions_updated_at ON tenant.job_executions;
DROP TRIGGER IF EXISTS trg_job_definitions_updated_at ON tenant.job_definitions;
DROP TABLE IF EXISTS tenant.job_executions;
DROP TABLE IF EXISTS tenant.job_definitions;