-- +goose Up
ALTER TABLE tenant.job_executions
ADD COLUMN records_processed BIGINT NULL,
ADD COLUMN bytes_transferred BIGINT NULL;

-- +goose Down
ALTER TABLE tenant.job_executions
DROP COLUMN records_processed,
DROP COLUMN bytes_transferred;