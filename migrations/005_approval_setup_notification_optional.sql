-- 005: "Pas_NOTIFICATION_ID" was NOT NULL with no default, so no approval setup row could be created.
-- The app sends approval notifications itself (phc_user_notifications_t), so this link is optional. Idempotent.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'phc_approval_setup_t'
               AND column_name = 'Pas_NOTIFICATION_ID' AND is_nullable = 'NO') THEN
        ALTER TABLE phc_approval_setup_t ALTER COLUMN "Pas_NOTIFICATION_ID" DROP NOT NULL;
    END IF;
END $$;
