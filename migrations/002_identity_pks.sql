-- 002: give integer primary keys without a default a sequence, replacing app-side MAX(pk)+1.
-- Idempotent: skips missing tables and columns that already have a default or are identity columns.
-- phc_countries_t.pct_country_isosqno is deliberately excluded (natural ISO key, not a surrogate id).
DO $$
DECLARE
    r   record;
    seq text;
    nxt bigint;
BEGIN
    FOR r IN SELECT * FROM (VALUES
        ('phc_certifications_t',         'pcr_plant_cert_id'),
        ('phc_emp_assignments_t',        'pea_assignment_id'),
        ('phc_equipment_locations_t',    'pel_equ_location_id'),
        ('phc_material_group_master_t',  'pmgm_material_group_id'),
        ('phc_material_master_t',        'pmat_material_id'),
        ('phc_notifications_setup_t',    'pns_notification_id'),
        ('phc_plant_compliance_t',       'ppc_compliance_id'),
        ('phc_plant_equipment_t',        'ppm_equipment_id'),
        ('phc_prod_alt_names_t',         'pan_alt_name_id'),
        ('phc_prod_lifecycle_history_t', 'plh_lifecycle_hist_id'),
        ('phc_products_master_t',        'pma_product_id'),
        ('phc_uom_conversion_t',         'puc_conversion_id'),
        ('phc_uom_master_t',             'pum_uom_id')
    ) AS v(tbl, col)
    LOOP
        IF to_regclass('public.' || quote_ident(r.tbl)) IS NULL THEN
            CONTINUE;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = r.tbl AND column_name = r.col
              AND column_default IS NULL AND is_identity = 'NO'
        ) THEN
            CONTINUE;
        END IF;

        seq := r.tbl || '_' || r.col || '_seq';
        EXECUTE format('CREATE SEQUENCE IF NOT EXISTS public.%I', seq);
        EXECUTE format('SELECT COALESCE(MAX(%I), 0) + 1 FROM public.%I', r.col, r.tbl) INTO nxt;
        PERFORM setval(format('public.%I', seq)::regclass, nxt, false);
        EXECUTE format('ALTER TABLE public.%I ALTER COLUMN %I SET DEFAULT nextval(%L::regclass)',
                       r.tbl, r.col, format('public.%I', seq));
        EXECUTE format('ALTER SEQUENCE public.%I OWNED BY public.%I.%I', seq, r.tbl, r.col);
    END LOOP;
END $$;
