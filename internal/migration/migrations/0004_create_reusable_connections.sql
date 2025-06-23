-- +goose Up
CREATE TABLE IF NOT EXISTS tenant.connections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    data_format TEXT NOT NULL,
    conn_string TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Triggers to maintain updated_at
CREATE TRIGGER trg_connections_updated_at
  BEFORE UPDATE ON tenant.connections
  FOR EACH ROW EXECUTE FUNCTION tenant.set_updated_at();

-- +goose Down
DROP TABLE IF EXISTS tenant.connections;

DROP TRIGGER IF EXISTS trg_connections_updated_at ON tenant.connections;