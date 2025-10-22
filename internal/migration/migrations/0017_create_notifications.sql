-- +goose Up

CREATE TABLE IF NOT EXISTS tenant.notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID REFERENCES tenant.tenants(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notifications_tenant_created_at
    ON tenant.notifications (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_unread
    ON tenant.notifications (tenant_id, read_at)
    WHERE read_at IS NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_notifications_unread;
DROP INDEX IF EXISTS idx_notifications_tenant_created_at;
DROP TABLE IF EXISTS tenant.notifications;
