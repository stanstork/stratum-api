-- +goose Up

-- Load pgcrypto for crypt() and gen_salt()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO tenant.tenants (name)
VALUES ('default')
ON CONFLICT (name) DO NOTHING;

INSERT INTO tenant.users (tenant_id, email, password_hash)
SELECT id, 'admin@example.com', crypt('Qwerty123!', gen_salt('bf'))
FROM tenant.tenants
WHERE name = 'default'
ON CONFLICT (tenant_id, email) DO NOTHING;

-- +goose Down
DELETE FROM tenant.users WHERE email = 'admin@example.com';
DELETE FROM tenant.tenants WHERE name = 'default';c