CREATE TABLE IF NOT EXISTS phc_module_t (
    pmd_module_id SERIAL PRIMARY KEY,
    pmd_module_name VARCHAR(100) UNIQUE,
    pmd_module_icon VARCHAR(50),
    pmd_status VARCHAR(10) DEFAULT 'ACT',
    pmd_created_by VARCHAR(50) DEFAULT 'System',
    pmd_modified_by VARCHAR(50) DEFAULT 'System',
    pmd_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pmd_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE phc_module_t ADD COLUMN IF NOT EXISTS pmd_module_icon VARCHAR(50);

CREATE TABLE IF NOT EXISTS phc_audit_log_t (
    pal_audit_id BIGSERIAL PRIMARY KEY,
    pal_table_name VARCHAR(100) NOT NULL,
    pal_record_id VARCHAR(100) NOT NULL,
    pal_action VARCHAR(50) NOT NULL,
    pal_user_id BIGINT,
    pal_username VARCHAR(100) NOT NULL,
    pal_client_ip VARCHAR(50),
    pal_old_values JSONB,
    pal_new_values JSONB,
    pal_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_audit_tbl_rec ON phc_audit_log_t(pal_table_name, pal_record_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON phc_audit_log_t(pal_timestamp DESC);

CREATE TABLE IF NOT EXISTS phc_user_notifications_t (
    pun_notification_id BIGSERIAL PRIMARY KEY,
    pun_recipient_user_id BIGINT,
    pun_recipient_role VARCHAR(50),
    pun_title VARCHAR(150) NOT NULL,
    pun_message TEXT NOT NULL,
    pun_category VARCHAR(50) DEFAULT 'WORKFLOW',
    pun_link_url VARCHAR(255),
    pun_is_read BOOLEAN DEFAULT FALSE,
    pun_created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pun_recipient ON phc_user_notifications_t(pun_recipient_user_id, pun_recipient_role, pun_is_read);

ALTER TABLE phc_users_t ADD COLUMN IF NOT EXISTS pus_failed_attempts INTEGER DEFAULT 0;
ALTER TABLE phc_users_t ADD COLUMN IF NOT EXISTS pus_locked_until TIMESTAMPTZ;
