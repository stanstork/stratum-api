-- +goose Up
-- Add description and replace JSON connection fields with FK references
ALTER TABLE tenant.job_definitions
  ADD COLUMN description TEXT;

ALTER TABLE tenant.job_definitions
  ADD COLUMN source_connection_id UUID NOT NULL REFERENCES tenant.connections(id),
  ADD COLUMN destination_connection_id UUID NOT NULL REFERENCES tenant.connections(id);

-- Drop old JSON/text connection columns
ALTER TABLE tenant.job_definitions
  DROP COLUMN source_connection,
  DROP COLUMN destination_connection,
  DROP COLUMN engine_settings;

-- +goose Down
ALTER TABLE tenant.job_definitions
  ADD COLUMN source_connection TEXT NOT NULL,
  ADD COLUMN destination_connection TEXT NOT NULL,
  ADD COLUMN engine_settings JSONB NOT NULL;

ALTER TABLE tenant.job_definitions
  DROP COLUMN destination_connection_id,
  DROP COLUMN source_connection_id,
  DROP COLUMN description;