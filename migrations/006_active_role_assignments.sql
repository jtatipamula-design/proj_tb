-- 006: screen access must come only from active, in-date role assignments.
-- The view ignored status and dates, so a role assignment deleted in the app (a soft delete to 'INA')
-- or past its end date still granted access. Mirrors the fallback query in app/auth.py. Idempotent.
CREATE OR REPLACE VIEW v_user_authorized_screens AS
 SELECT DISTINCT ura.pua_user_id,
    s.psn_screen_code,
    s.psn_screen_name,
    COALESCE(m.pmd_module_name, 'General'::character varying) AS module_name,
    m.pmd_module_id
   FROM phc_screens_t s
     JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
     JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
     JOIN phc_roles_t r ON r.prl_role_id = ura.pua_role_id
     LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
  WHERE (s.psn_status::text = 'ACT'::text OR s.psn_status IS NULL)
    AND ura.pua_status::text = 'ACT'::text
    AND CURRENT_DATE >= ura.pua_start_date AND (ura.pua_end_date IS NULL OR CURRENT_DATE <= ura.pua_end_date)
    AND rsa.prs_status::text = 'ACT'::text
    AND CURRENT_DATE >= rsa.prs_start_date AND (rsa.prs_end_date IS NULL OR CURRENT_DATE <= rsa.prs_end_date)
    AND (r.prl_status IS NULL OR r.prl_status::text = 'ACT'::text)
    AND (r.prl_start_date IS NULL OR CURRENT_DATE >= r.prl_start_date)
    AND (r.prl_end_date IS NULL OR CURRENT_DATE <= r.prl_end_date);
