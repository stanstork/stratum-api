-- +goose Up

-- +goose StatementBegin
-- Create an ENUM type for data_format
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'connection_format') THEN
        CREATE TYPE tenant.connection_format AS ENUM ('pg','mysql','api','csv');
    END IF;
END$$;
-- +goose StatementEnd

-- Alter data_format column to use the new enum type
ALTER TABLE tenant.connections
  ALTER COLUMN data_format TYPE tenant.connection_format USING data_format::tenant.connection_format;

-- Remove original conn_string column
ALTER TABLE tenant.connections DROP COLUMN conn_string;

-- Add new connection detail columns
ALTER TABLE tenant.connections
  ADD COLUMN host TEXT,
  ADD COLUMN port INT,
  ADD COLUMN username TEXT,
  ADD COLUMN password BYTEA NOT NULL,
  ADD COLUMN db_name TEXT;

-- +goose Down
ALTER TABLE tenant.connections DROP COLUMN db_name;
ALTER TABLE tenant.connections DROP COLUMN password;
ALTER TABLE tenant.connections DROP COLUMN username;
ALTER TABLE tenant.connections DROP COLUMN port;
ALTER TABLE tenant.connections DROP COLUMN host;
ALTER TABLE tenant.connections
  ALTER COLUMN data_format TYPE TEXT USING data_format::text;
DROP TYPE IF EXISTS tenant.connection_format;