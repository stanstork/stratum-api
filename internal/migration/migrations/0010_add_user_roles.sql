-- +goose Up

ALTER TABLE tenant.users
    ADD COLUMN role TEXT NOT NULL DEFAULT 'viewer';

ALTER TABLE tenant.users
    ADD CONSTRAINT chk_users_role
    CHECK (role IN ('viewer', 'editor', 'admin', 'super_admin'));

UPDATE tenant.users
SET role = 'admin'
WHERE email = 'admin@example.com';

WITH default_tenant AS (
    SELECT id FROM tenant.tenants WHERE name = 'default'
)
INSERT INTO tenant.users (tenant_id, email, password_hash, role)
SELECT id, 'superadmin@example.com', crypt('SuperSecret123!', gen_salt('bf')), 'super_admin'
FROM default_tenant
ON CONFLICT (tenant_id, email) DO NOTHING;

-- +goose Down

DELETE FROM tenant.users WHERE email = 'superadmin@example.com';

ALTER TABLE tenant.users
    DROP CONSTRAINT IF EXISTS chk_users_role;

ALTER TABLE tenant.users
    DROP COLUMN IF EXISTS role;
