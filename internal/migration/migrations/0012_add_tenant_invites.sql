-- +goose Up

CREATE TABLE IF NOT EXISTS tenant.invites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenant.tenants(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    roles TEXT[] NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    created_by UUID REFERENCES tenant.users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '7 days'),
    accepted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_invites_tenant ON tenant.invites(tenant_id);
CREATE INDEX IF NOT EXISTS idx_invites_email ON tenant.invites(email);

-- +goose Down
DROP INDEX IF EXISTS idx_invites_email;
DROP INDEX IF EXISTS idx_invites_tenant;
DROP TABLE IF EXISTS tenant.invites;
