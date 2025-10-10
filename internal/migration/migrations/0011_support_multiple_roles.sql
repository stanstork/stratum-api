-- +goose Up

ALTER TABLE tenant.users
    ADD COLUMN roles TEXT[] NOT NULL DEFAULT ARRAY['viewer'];

UPDATE tenant.users
SET roles = ARRAY[role];

ALTER TABLE tenant.users
    DROP CONSTRAINT IF EXISTS chk_users_role;

ALTER TABLE tenant.users
    ADD CONSTRAINT chk_users_roles
    CHECK (roles IS NOT NULL
        AND array_length(roles, 1) >= 1
        AND roles <@ ARRAY['viewer', 'editor', 'admin', 'super_admin']);

ALTER TABLE tenant.users
    DROP COLUMN role;

-- +goose Down

ALTER TABLE tenant.users
    ADD COLUMN role TEXT NOT NULL DEFAULT 'viewer';

UPDATE tenant.users
SET role = COALESCE(roles[1], 'viewer');

ALTER TABLE tenant.users
    DROP CONSTRAINT IF EXISTS chk_users_roles;

ALTER TABLE tenant.users
    ADD CONSTRAINT chk_users_role
    CHECK (role IN ('viewer', 'editor', 'admin', 'super_admin'));

ALTER TABLE tenant.users
    DROP COLUMN roles;
