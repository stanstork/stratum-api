-- +goose Up

ALTER TABLE tenant.job_executions
    ALTER COLUMN id DROP DEFAULT;

-- +goose Down

ALTER TABLE tenant.job_executions
    ALTER COLUMN id SET DEFAULT gen_random_uuid();
