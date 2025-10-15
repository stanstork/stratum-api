-- +goose Up

ALTER TABLE tenant.users
    ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS last_name TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE tenant.users
    DROP COLUMN IF EXISTS first_name,
    DROP COLUMN IF EXISTS last_name;
