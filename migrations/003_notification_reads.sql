-- 003: per-user read state for notifications.
-- pun_is_read was one shared flag, so one approver opening a role notification marked it read for the whole role.
-- Idempotent.
CREATE TABLE IF NOT EXISTS phc_notification_reads_t (
    pnr_user_id BIGINT NOT NULL,
    pnr_notification_id BIGINT NOT NULL REFERENCES phc_user_notifications_t(pun_notification_id) ON DELETE CASCADE,
    pnr_read_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pnr_user_id, pnr_notification_id)
);
