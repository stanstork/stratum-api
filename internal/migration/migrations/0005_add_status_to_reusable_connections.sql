-- +goose Up
ALTER TABLE tenant.connections
  ADD COLUMN status TEXT NOT NULL DEFAULT 'untested';

-- Enforce allowed values
ALTER TABLE tenant.connections
  ADD CONSTRAINT reusable_connections_status_check
  CHECK (status IN ('valid', 'invalid', 'untested'));

-- +goose Down
ALTER TABLE tenant.connections DROP CONSTRAINT connections_status_check;
ALTER TABLE tenant.connections DROP COLUMN status;
