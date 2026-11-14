-- 000_base_schema.sql
-- Base schema for the ERP database (public schema).
-- Generated from the live production schema (pg_catalog introspection) on 2026-11-14.
-- IDEMPOTENT: safe to run against the existing production DB. Every statement is
-- CREATE ... IF NOT EXISTS, CREATE OR REPLACE, or guarded by a pg_constraint / pg_trigger check,
-- so it only creates objects that are missing and never drops or rewrites existing data.
-- Excludes phc_schema_migrations_t (created by app/database.py setup_db) and Neon's demo table playing_with_neon.
-- Seed data lives in scripts/seed_db.py.

-- ===================== SEQUENCES (serial column defaults) =====================
CREATE SEQUENCE IF NOT EXISTS pcv_cleaning_process_records_t_pcv_cpr_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_cpr_execution_steps_t_pcv_step_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_equipment_clearance_checklists_t_pcv_clearance_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_equipment_clearance_items_t_pcv_item_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_equipment_sampling_locations_t_pcv_sampling_loc_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_equipment_surface_areas_t_pcv_surface_area_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_mdd_api_details_t_pcv_mdd_detail_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_mdd_registrations_t_pcv_mdd_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_pde_api_details_t_pcv_pde_detail_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_pde_registrations_t_pcv_pde_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_product_batch_sizes_t_pcv_batch_size_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_product_equipment_mapping_t_pcv_mapping_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_product_pack_styles_t_pcv_pack_style_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_product_stages_t_pcv_stage_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_product_strengths_t_pcv_strength_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_products_t_pcv_product_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_sampling_records_t_pcv_sampling_record_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_solubility_details_t_pcv_solubility_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_test_methods_t_pcv_method_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_test_request_forms_t_pcv_trf_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_test_results_t_pcv_result_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_training_attendees_t_pcv_attendee_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_training_records_t_pcv_training_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_validation_executions_t_pcv_execution_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS pcv_validation_reports_t_pcv_report_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_audit_log_t_pal_audit_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_building_floor_t_pfl_floor_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_building_t_plc_building_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_document_number_range_set_pnrm_document_number_range_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_industries_t_ppi_industry_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_item_category_t_pic_industry_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_item_category_t_pic_item_category_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_item_family_t_pif_item_family_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_item_master_t_pim_item_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_item_sub_category_t_pisc_item_sub_category_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_item_type_t_pit_item_type_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_module_t_pmd_module_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_prod_attributes_template__ppatd_attributes_detail_temp__seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_prod_attributes_template__ppatm_attributes_template_mas_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_attributes_t_ppatr_attributes_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_category_t_ppc_category_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_category_t_ppc_industry_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_group_t_phc_product_group_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_sku_t_ppsku_sku_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_sub_category_t_ppsc_sub_category_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_type_t_ppt_product_type_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_product_variant_t_ppv_variant_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_role_screen_assignment_t_prs_role_screen_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_rooms_t_prm_room_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_screens_t_psn_screen_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_storage_locator_t_psl_locator_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_user_notifications_t_pun_notification_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS phc_user_roles_assignment_t_pua_user_role_assig_id_seq AS integer INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS psn_screen_id_seq AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 191 CACHE 1 NO CYCLE;

-- ===================== TABLES =====================
CREATE TABLE IF NOT EXISTS pcv_cleaning_process_records_t (
    pcv_cpr_id bigint DEFAULT nextval('pcv_cleaning_process_records_t_pcv_cpr_id_seq'::regclass) NOT NULL,
    pcv_cpr_no character varying(50) NOT NULL,
    pcv_execution_id bigint NOT NULL,
    pcv_equipment_id character varying(50) NOT NULL,
    pcv_batch_number character varying(50),
    pcv_sop_no character varying(50) NOT NULL,
    pcv_status character varying(30) DEFAULT 'In-Progress'::character varying,
    pcv_remarks text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_cpr_execution_steps_t (
    pcv_step_id bigint DEFAULT nextval('pcv_cpr_execution_steps_t_pcv_step_id_seq'::regclass) NOT NULL,
    pcv_cpr_id bigint NOT NULL,
    pcv_step_number character varying(10) NOT NULL,
    pcv_instruction text NOT NULL,
    pcv_criteria text NOT NULL,
    pcv_observation_type character varying(30),
    pcv_is_done boolean DEFAULT false,
    pcv_vacuum_cleaner_id character varying(50),
    pcv_performed_by character varying(50),
    pcv_performed_at timestamp with time zone,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_equipment_clearance_checklists_t (
    pcv_clearance_id bigint DEFAULT nextval('pcv_equipment_clearance_checklists_t_pcv_clearance_id_seq'::regclass) NOT NULL,
    pcv_cpr_id bigint NOT NULL,
    pcv_status character varying(30) DEFAULT 'Pending QA Inspection'::character varying,
    pcv_remarks text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_equipment_clearance_items_t (
    pcv_item_id bigint DEFAULT nextval('pcv_equipment_clearance_items_t_pcv_item_id_seq'::regclass) NOT NULL,
    pcv_clearance_id bigint NOT NULL,
    pcv_checkpoint_no integer NOT NULL,
    pcv_checkpoint_description text NOT NULL,
    pcv_inspected_by_pd character varying(30),
    pcv_inspected_by_qa character varying(30),
    pcv_remarks text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_equipment_sampling_locations_t (
    pcv_sampling_loc_id bigint DEFAULT nextval('pcv_equipment_sampling_locations_t_pcv_sampling_loc_id_seq'::regclass) NOT NULL,
    pcv_equipment_id character varying(50) NOT NULL,
    pcv_location_description text NOT NULL,
    pcv_sample_type character varying(30),
    pcv_test_for character varying(30),
    pcv_sample_identifier character varying(30) NOT NULL,
    pcv_image_ref text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_equipment_surface_areas_t (
    pcv_surface_area_id bigint DEFAULT nextval('pcv_equipment_surface_areas_t_pcv_surface_area_id_seq'::regclass) NOT NULL,
    pcv_equipment_id character varying(50) NOT NULL,
    pcv_product_contact_surface_area numeric(12,2) NOT NULL,
    pcv_uom character varying(20) DEFAULT 'Sq.Cm'::character varying NOT NULL,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_equipments_t (
    pcv_equipment_id character varying(50) NOT NULL,
    pcv_equipment_name character varying(150) NOT NULL,
    pcv_make character varying(100),
    pcv_model character varying(50),
    pcv_capacity numeric(10,2),
    pcv_uom character varying(20),
    pcv_location_building character varying(100),
    pcv_room_name character varying(100),
    pcv_room_number character varying(30),
    pcv_is_active boolean DEFAULT true,
    pcv_description text,
    pcv_company_id bigint DEFAULT 1 NOT NULL,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_mdd_api_details_t (
    pcv_mdd_detail_id bigint DEFAULT nextval('pcv_mdd_api_details_t_pcv_mdd_detail_id_seq'::regclass) NOT NULL,
    pcv_mdd_id bigint NOT NULL,
    pcv_api_name character varying(150) NOT NULL,
    pcv_api_mdd numeric(10,2) NOT NULL,
    pcv_uom character varying(20) DEFAULT 'mg'::character varying NOT NULL,
    pcv_description text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_mdd_registrations_t (
    pcv_mdd_id bigint DEFAULT nextval('pcv_mdd_registrations_t_pcv_mdd_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_product_mdd numeric(10,2) NOT NULL,
    pcv_product_mdd_uom character varying(20) NOT NULL,
    pcv_ref_document_no character varying(50) NOT NULL,
    pcv_remarks text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_pde_api_details_t (
    pcv_pde_detail_id bigint DEFAULT nextval('pcv_pde_api_details_t_pcv_pde_detail_id_seq'::regclass) NOT NULL,
    pcv_pde_id bigint NOT NULL,
    pcv_api_name character varying(150) NOT NULL,
    pcv_pde_value numeric(10,4) NOT NULL,
    pcv_uom character varying(20) DEFAULT 'mg/day'::character varying NOT NULL,
    pcv_description text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_pde_registrations_t (
    pcv_pde_id bigint DEFAULT nextval('pcv_pde_registrations_t_pcv_pde_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_ref_document_no character varying(50) NOT NULL,
    pcv_validity_date timestamp with time zone NOT NULL,
    pcv_remarks text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_product_batch_sizes_t (
    pcv_batch_size_id bigint DEFAULT nextval('pcv_product_batch_sizes_t_pcv_batch_size_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_strength_value character varying(100) NOT NULL,
    pcv_stage_code character varying(30) NOT NULL,
    pcv_batch_size numeric(12,2) NOT NULL,
    pcv_uom character varying(20) NOT NULL,
    pcv_ref_bmr_no character varying(50) NOT NULL,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_product_equipment_mapping_t (
    pcv_mapping_id bigint DEFAULT nextval('pcv_product_equipment_mapping_t_pcv_mapping_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_block_no character varying(30) NOT NULL,
    pcv_module_no character varying(30) NOT NULL,
    pcv_equipment_id character varying(50) NOT NULL,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_product_pack_styles_t (
    pcv_pack_style_id bigint DEFAULT nextval('pcv_product_pack_styles_t_pcv_pack_style_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_pack_type character varying(100) NOT NULL,
    pcv_pack_count integer NOT NULL,
    pcv_pack_code character varying(30) NOT NULL,
    pcv_description text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_product_stages_t (
    pcv_stage_id bigint DEFAULT nextval('pcv_product_stages_t_pcv_stage_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_stage_name character varying(100) NOT NULL,
    pcv_stage_code character varying(30) NOT NULL,
    pcv_mfc_mfr_no character varying(50) NOT NULL,
    pcv_description text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_product_strengths_t (
    pcv_strength_id bigint DEFAULT nextval('pcv_product_strengths_t_pcv_strength_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_strength_value character varying(100) NOT NULL,
    pcv_uom character varying(20) NOT NULL,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_production_products_t (
    pcv_product_id bigint DEFAULT nextval('pcv_products_t_pcv_product_id_seq'::regclass) NOT NULL,
    pcv_product_code character varying(50) NOT NULL,
    pcv_product_name character varying(200) NOT NULL,
    pcv_dosage_form character varying(50) NOT NULL,
    pcv_grade character varying(50) DEFAULT 'USP'::character varying,
    pcv_description text,
    pcv_is_active boolean DEFAULT true,
    pcv_company_id bigint DEFAULT 1 NOT NULL,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_sampling_records_t (
    pcv_sampling_record_id bigint DEFAULT nextval('pcv_sampling_records_t_pcv_sampling_record_id_seq'::regclass) NOT NULL,
    pcv_trf_id bigint NOT NULL,
    pcv_is_sampling_done boolean DEFAULT false,
    pcv_sampling_done_at timestamp with time zone,
    pcv_is_acknowledged_by_qc boolean DEFAULT false,
    pcv_qc_acknowledged_at timestamp with time zone,
    pcv_remarks text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_solubility_details_t (
    pcv_solubility_id bigint DEFAULT nextval('pcv_solubility_details_t_pcv_solubility_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_api_name character varying(150) NOT NULL,
    pcv_solubility_in_water character varying(50) NOT NULL,
    pcv_ref_document_no character varying(50) NOT NULL,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_test_methods_t (
    pcv_method_id bigint DEFAULT nextval('pcv_test_methods_t_pcv_method_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_test_method_no character varying(50) NOT NULL,
    pcv_api_name character varying(150) NOT NULL,
    pcv_lod_value numeric(10,4) NOT NULL,
    pcv_loq_value numeric(10,4) NOT NULL,
    pcv_uom character varying(20) DEFAULT 'ppm'::character varying NOT NULL,
    pcv_description text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_test_request_forms_t (
    pcv_trf_id bigint DEFAULT nextval('pcv_test_request_forms_t_pcv_trf_id_seq'::regclass) NOT NULL,
    pcv_cpr_id bigint NOT NULL,
    pcv_equipment_id character varying(50) NOT NULL,
    pcv_submitted_to_qa boolean DEFAULT false,
    pcv_submitted_at timestamp with time zone,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_test_results_t (
    pcv_result_id bigint DEFAULT nextval('pcv_test_results_t_pcv_result_id_seq'::regclass) NOT NULL,
    pcv_sampling_record_id bigint NOT NULL,
    pcv_sampling_loc_id bigint NOT NULL,
    pcv_chemical_result numeric(10,4),
    pcv_microbiological_result numeric(10,4),
    pcv_detergent_result numeric(10,4),
    pcv_result_status character varying(20) DEFAULT 'Pass'::character varying,
    pcv_qa_acknowledged boolean DEFAULT false,
    pcv_qa_acknowledged_at timestamp with time zone,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_training_attendees_t (
    pcv_attendee_id bigint DEFAULT nextval('pcv_training_attendees_t_pcv_attendee_id_seq'::regclass) NOT NULL,
    pcv_training_id bigint NOT NULL,
    pcv_employee_name character varying(100) NOT NULL,
    pcv_employee_id character varying(50) NOT NULL,
    pcv_emp_id bigint,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_training_records_t (
    pcv_training_id bigint DEFAULT nextval('pcv_training_records_t_pcv_training_id_seq'::regclass) NOT NULL,
    pcv_execution_id bigint NOT NULL,
    pcv_trainer_name character varying(100) NOT NULL,
    pcv_trainer_employee_id character varying(50) NOT NULL,
    pcv_emp_id bigint,
    pcv_is_submitted boolean DEFAULT false,
    pcv_submitted_at timestamp with time zone,
    pcv_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_validation_executions_t (
    pcv_execution_id bigint DEFAULT nextval('pcv_validation_executions_t_pcv_execution_id_seq'::regclass) NOT NULL,
    pcv_product_id bigint NOT NULL,
    pcv_block_no character varying(30) NOT NULL,
    pcv_module_no character varying(30) NOT NULL,
    pcv_protocol_no character varying(50) NOT NULL,
    pcv_current_status character varying(50) DEFAULT 'Training'::character varying,
    pcv_description text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS pcv_validation_reports_t (
    pcv_report_id bigint DEFAULT nextval('pcv_validation_reports_t_pcv_report_id_seq'::regclass) NOT NULL,
    pcv_execution_id bigint NOT NULL,
    pcv_report_no character varying(50) NOT NULL,
    pcv_generated_by character varying(50) NOT NULL,
    pcv_generated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    pcv_report_pdf_url text,
    pcv_summary_notes text,
    pcv_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_created_by character varying(50) DEFAULT 'System'::character varying NOT NULL,
    pcv_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pcv_modified_by character varying(50) DEFAULT 'System'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_approval_events_t (
    pae_appr_event_id bigint NOT NULL,
    pae_appr_setup_id bigint NOT NULL,
    pae_approval_type character varying(60) NOT NULL,
    pae_appr_seq integer NOT NULL,
    pae_appr_role_type character varying(60) NOT NULL,
    pae_appr_event_ref_id bigint,
    pae_appr_emp_no character varying(60) NOT NULL,
    pae_perform_role character varying(250),
    pae_event_freq_type character varying(60) NOT NULL,
    pae_event_freq integer NOT NULL,
    pae_event_waiting_min integer NOT NULL,
    pae_event_status character varying(3) NOT NULL,
    pae_decision_notes character varying(2000),
    pae_decision_support bytea,
    pae_event_start date NOT NULL,
    pae_event_end date NOT NULL,
    pae_event_table character varying(120) NOT NULL,
    pae_event_table_pk_id bigint NOT NULL,
    pae_created timestamp without time zone DEFAULT now() NOT NULL,
    pae_modified timestamp without time zone DEFAULT now() NOT NULL,
    pae_created_by character varying(20) NOT NULL,
    pae_modified_by character varying(20) NOT NULL,
    pae_company_id bigint NOT NULL,
    pae_notification_id bigint
);

CREATE TABLE IF NOT EXISTS phc_approval_setup_t (
    pas_appr_setup_id bigint NOT NULL,
    pas_approval_type character varying(60) NOT NULL,
    pas_appr_seq integer NOT NULL,
    pas_appr_role_type character varying(60) NOT NULL,
    pas_appr_emp_no character varying(60),
    pas_event_freq_type character varying(60) NOT NULL,
    pas_event_freq integer NOT NULL,
    pas_event_waiting_min integer NOT NULL,
    pas_start_date date NOT NULL,
    pas_end_date date,
    pas_status character varying(3) DEFAULT 'ACT'::character varying NOT NULL,
    pas_created timestamp without time zone DEFAULT now() NOT NULL,
    pas_modified timestamp without time zone DEFAULT now() NOT NULL,
    pas_created_by character varying(20) NOT NULL,
    pas_modified_by character varying(20) NOT NULL,
    pas_company_id bigint NOT NULL,
    "Pas_NOTIFICATION_ID" bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_approval_types_t (
    pat_approval_type character varying(60) NOT NULL,
    pat_approval_type_desc character varying(2000) NOT NULL,
    pat_approval_selector character varying(240),
    pat_status character varying(3) DEFAULT 'ACT'::character varying,
    pat_start_date date DEFAULT CURRENT_DATE,
    pat_end_date date,
    pat_created timestamp without time zone DEFAULT now() NOT NULL,
    pat_modified timestamp without time zone DEFAULT now() NOT NULL,
    pat_created_by character varying(20) NOT NULL,
    pat_modified_by character varying(20) NOT NULL,
    pat_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_apps_t (
    pap_apps_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pap_company_id bigint NOT NULL,
    pap_apps_name character varying(250) NOT NULL,
    pap_apps_detail character varying(250) NOT NULL,
    pap_start_date date,
    pap_end_date date,
    pap_status character varying(3),
    pap_created date DEFAULT CURRENT_DATE NOT NULL,
    pap_modified date DEFAULT CURRENT_DATE NOT NULL,
    pap_created_by character varying(20) NOT NULL,
    pap_modified_by character varying(20) NOT NULL,
    pap_dept_id bigint NOT NULL,
    pap_org_id bigint,
    pap_services_id bigint,
    pap_cost_center_id bigint,
    pap_apps_code character varying(20) NOT NULL,
    pap_software_version character varying(20),
    pap_software_owner character varying(10),
    pap_no_of_approval_levels integer,
    pap_instrument character varying(60),
    pap_licencecount integer,
    pcp_company_id bigint
);

CREATE TABLE IF NOT EXISTS phc_audit_log_t (
    pal_audit_id bigint DEFAULT nextval('phc_audit_log_t_pal_audit_id_seq'::regclass) NOT NULL,
    pal_table_name character varying(100) NOT NULL,
    pal_record_id character varying(100) NOT NULL,
    pal_action character varying(50) NOT NULL,
    pal_user_id bigint,
    pal_username character varying(100) NOT NULL,
    pal_client_ip character varying(50),
    pal_old_values jsonb,
    pal_new_values jsonb,
    pal_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_building_floor_t (
    pfl_floor_id integer DEFAULT nextval('phc_building_floor_t_pfl_floor_id_seq'::regclass) NOT NULL,
    pfl_floor_number character varying(50) NOT NULL,
    pfl_floor_name character varying(100) NOT NULL,
    pfl_status character varying(3) NOT NULL,
    pfl_remarks text,
    pfl_created timestamp with time zone DEFAULT now() NOT NULL,
    pfl_modified timestamp with time zone DEFAULT now() NOT NULL,
    pfl_created_by character varying(20) NOT NULL,
    pfl_modified_by character varying(20) NOT NULL,
    pfl_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_building_t (
    plc_building_id integer DEFAULT nextval('phc_building_t_plc_building_id_seq'::regclass) NOT NULL,
    plc_building_name character varying(100) NOT NULL,
    plc_building_number character varying(50) NOT NULL,
    plc_status character varying(3) NOT NULL,
    plc_remarks text,
    plc_created timestamp with time zone DEFAULT now() NOT NULL,
    plc_modified timestamp with time zone DEFAULT now() NOT NULL,
    plc_created_by character varying(20) NOT NULL,
    plc_modified_by character varying(20) NOT NULL,
    plc_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_certifications_t (
    pcr_plant_cert_id bigint NOT NULL,
    pct_company_id bigint NOT NULL,
    pcr_plant_id bigint,
    pcr_equipment_id bigint,
    pcr_cert_code character varying(20) NOT NULL,
    pcr_status character varying(3) DEFAULT 'ACT'::character varying NOT NULL,
    pcr_certified_date date,
    pcr_expiry_date date,
    pcr_certifying_body character varying(150),
    pcr_created timestamp without time zone DEFAULT now() NOT NULL,
    pcr_modified timestamp without time zone DEFAULT now() NOT NULL,
    pcr_created_by character varying(20) NOT NULL,
    pcr_modified_by character varying(20) NOT NULL,
    pcr_company_id bigint,
    pcr_org_id bigint
);

CREATE TABLE IF NOT EXISTS phc_cities_t (
    pct_city_code_iso4 character varying(20) NOT NULL,
    pct_city_name character varying(200) NOT NULL,
    pct_city_postal_zip_code character varying(20),
    pct_state_iso3_code character varying(20) NOT NULL,
    pct_country_iso3 character varying(3) NOT NULL,
    pct_latitude numeric(10,6),
    pct_longitude numeric(10,6),
    pct_status character varying(3) NOT NULL,
    pct_remarks text,
    pct_created timestamp with time zone DEFAULT now() NOT NULL,
    pct_modified timestamp with time zone DEFAULT now() NOT NULL,
    pct_created_by character varying(20) NOT NULL,
    pct_modified_by character varying(20) NOT NULL,
    pct_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_companies_t (
    pcp_company_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pcp_company_code character varying(20) NOT NULL,
    pcp_company_name character varying(250) NOT NULL,
    pcp_created date DEFAULT CURRENT_DATE NOT NULL,
    pcp_modified date DEFAULT CURRENT_DATE NOT NULL,
    pcp_created_by character varying(20) NOT NULL,
    pcp_modified_by character varying(20) NOT NULL,
    pcp_start_date date,
    pcp_end_date date,
    pcp_status character varying(3),
    pcp_about_company text,
    pcp_legal_name character varying(300),
    pcp_registration_number character varying(100),
    pcp_tax_number character varying(100),
    pcp_contact_name character varying(300),
    pcp_email character varying(150),
    pcp_phone character varying(50),
    pcp_website character varying(200),
    pcp_currency_code character varying(10),
    pcp_timezone character varying(50),
    pcp_address1 character varying(250),
    pcp_address2 character varying(250),
    pcp_city character varying(100),
    pcp_state character varying(100),
    pcp_postal_code character varying(20),
    pcp_country character varying(100),
    "PCP_PARENT_COMPANY_CODE" character varying(20)
);

CREATE TABLE IF NOT EXISTS phc_cost_center_t (
    pcc_cost_center_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pcc_services_id bigint NOT NULL,
    pcc_company_id bigint NOT NULL,
    pcc_cost_center_code character varying(20) NOT NULL,
    pcc_cost_center_name character varying(250) NOT NULL,
    pcc_created date DEFAULT CURRENT_DATE NOT NULL,
    pcc_modified date DEFAULT CURRENT_DATE NOT NULL,
    pcc_created_by character varying(20) NOT NULL,
    pcc_modified_by character varying(20) NOT NULL,
    pcc_start_date date,
    pcc_end_date date,
    pcc_status character varying(3)
);

CREATE TABLE IF NOT EXISTS phc_countries_t (
    pct_country_isosqno smallint NOT NULL,
    pct_country_iso2 character varying(2) NOT NULL,
    pct_country_iso3 character varying(3) NOT NULL,
    pct_country_name character varying(150) NOT NULL,
    pct_phone_code character varying(10),
    pct_capital character varying(100),
    pct_currency_code character varying(10),
    pct_currency_name character varying(100),
    pct_region character varying(50),
    pct_subregion character varying(100),
    pct_latitude numeric(10,6),
    pct_longitude numeric(10,6),
    pct_status character varying(3) NOT NULL,
    pct_remarks text,
    pct_created timestamp with time zone DEFAULT now() NOT NULL,
    pct_modified timestamp with time zone DEFAULT now() NOT NULL,
    pct_created_by character varying(20) NOT NULL,
    pct_modified_by character varying(20) NOT NULL,
    pct_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_dept_t (
    pdp_dept_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pdp_dept_code character varying(20) NOT NULL,
    pdp_dept_name character varying(250) NOT NULL,
    pdp_created date NOT NULL,
    pdp_modified date NOT NULL,
    pdp_created_by character varying(20) NOT NULL,
    pdp_modified_by character varying(20) NOT NULL,
    pdp_company_id bigint NOT NULL,
    pdp_start_date date,
    pdp_end_date date,
    pdp_status character varying(3)
);

CREATE TABLE IF NOT EXISTS phc_document_number_range_setup_t (
    pnrm_number_range_id bigint DEFAULT nextval('phc_document_number_range_set_pnrm_document_number_range_id_seq'::regclass) NOT NULL,
    pnrm_doc_module_code character varying(60) NOT NULL,
    pnrm_doc_object_code character varying(60) NOT NULL,
    pnrm_doc_number_type character varying(60) NOT NULL,
    pnrm_doc_description text,
    pnrm_prefix character varying(10),
    pnrm_separator1 character varying(6),
    pnrm_year_format character varying(4),
    pnrm_month_format character varying(2),
    pnrm_date_format character varying(2),
    pnrm_separator2 character varying(10),
    pnrm_current_number bigint,
    pnrm_separator3 character varying(6),
    pnrm_suffix character varying(10),
    pnrm_current_doc_number character varying(120),
    pnrm_start_number bigint NOT NULL,
    pnrm_end_number bigint,
    pnrm_reset_frequency character varying(60),
    pnrm_remarks text,
    pnrm_effective_from timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pnrm_effective_to timestamp without time zone,
    pnrm_status character varying(20) NOT NULL,
    pnrm_created_by character varying(60),
    pnrm_modified_by character varying(60),
    pnrm_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pnrm_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pnrm_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_emp_apps_grant_t (
    pea_emp_apps_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pea_company_id bigint NOT NULL,
    pea_apps_id bigint NOT NULL,
    pea_start_date date,
    pea_end_date date,
    pea_status character varying(3),
    pea_created date DEFAULT CURRENT_DATE NOT NULL,
    pea_modified date DEFAULT CURRENT_DATE NOT NULL,
    pea_created_by character varying(20) NOT NULL,
    pea_modified_by character varying(20) NOT NULL,
    pea_org_id bigint
);

CREATE TABLE IF NOT EXISTS phc_emp_assignments_t (
    pea_assignment_id bigint NOT NULL,
    pea_emp_id bigint NOT NULL,
    pea_company_id bigint NOT NULL,
    pea_dept_id bigint,
    pea_designation_code character varying(20),
    pea_local_emp_code character varying(30),
    pea_mgr_emp_id bigint NOT NULL,
    pea_employment_type character varying(20),
    pea_location_type character varying(20),
    pea_primary_assignment_id bigint,
    pea_current_flag boolean DEFAULT true NOT NULL,
    pea_date_of_joining timestamp with time zone NOT NULL,
    pea_date_of_confirmation timestamp with time zone,
    pea_probation_period_months bigint DEFAULT 6,
    pea_change_reason_code character varying(60),
    pea_remarks text,
    pea_status character varying(3) NOT NULL,
    pea_start_date timestamp with time zone DEFAULT now() NOT NULL,
    pea_end_date timestamp with time zone,
    pea_created timestamp with time zone DEFAULT now() NOT NULL,
    pea_modified timestamp with time zone DEFAULT now() NOT NULL,
    pea_created_by character varying(20) NOT NULL,
    pea_modified_by character varying(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_emp_t (
    pem_emp_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pem_company_id bigint NOT NULL,
    pem_cost_center_id bigint NOT NULL,
    pem_services_id bigint NOT NULL,
    pem_dept_id bigint NOT NULL,
    pem_org_id bigint NOT NULL,
    pem_user_name character varying(10) NOT NULL,
    pem_emp_number character varying(20) NOT NULL,
    pem_first_name character varying(250) NOT NULL,
    pem_last_name character varying(250) NOT NULL,
    pem_middle_name character varying(250),
    pem_created date DEFAULT CURRENT_DATE NOT NULL,
    pem_modified date DEFAULT CURRENT_DATE NOT NULL,
    pem_created_by character varying(20) NOT NULL,
    pem_modified_by character varying(20) NOT NULL,
    pem_start_date date,
    pem_end_date date,
    pem_status character varying(3),
    pem_email_id character varying(60),
    pem_mobile_no1 character varying(20),
    pem_mobile_no2 character varying(20),
    pem_designation character varying(20),
    pem_titile character varying(20),
    pem_manager_id bigint,
    "PEM_DISPLAY_NAME" character varying(250),
    pem_designation_code character varying(20),
    pem_gender_type character varying(60),
    pem_date_of_birth date,
    pem_marital_status character varying(60),
    pem_nationality_country_code character varying(20),
    pem_preferred_language_code character varying(20),
    pem_blood_group character varying(5),
    pem_personal_email character varying(150),
    pem_official_email character varying(150),
    pem_phone_primary character varying(30),
    pem_phone_alternate character varying(30),
    pem_address_line1 character varying(200),
    pem_address_line2 character varying(200),
    pem_city character varying(100),
    pem_state_province character varying(100),
    pem_country_code character varying(60),
    pem_postal_code character varying(20),
    pem_date_of_first_joining date,
    pem_profile_photo_url text,
    pem_emergency_contact_name character varying(150),
    pem_emergency_contact_phone character varying(30),
    pem_emergency_contact_relation character varying(60)
);

CREATE TABLE IF NOT EXISTS phc_equipment_locations_t (
    pel_equ_location_id bigint NOT NULL,
    pel_company_id bigint NOT NULL,
    pel_equipment_id bigint NOT NULL,
    pel_plant_id bigint NOT NULL,
    pel_status character varying(3) DEFAULT 'ACT'::character varying NOT NULL,
    pel_start_date date,
    pel_end_date date,
    pel_location_code character varying(20) NOT NULL,
    pel_location_name character varying(250) NOT NULL,
    pel_address_details jsonb DEFAULT '{}'::jsonb,
    pel_zone_classification character varying(100),
    pel_climate_zone character varying(50),
    pel_time_zone character varying(50),
    pel_seismic_risk_code character varying(10),
    pel_flood_risk_code character varying(10),
    pel_created timestamp without time zone DEFAULT now() NOT NULL,
    pel_modified timestamp without time zone DEFAULT now() NOT NULL,
    pel_created_by character varying(20) NOT NULL,
    pel_modified_by character varying(20) NOT NULL,
    pel_org_id bigint
);

CREATE TABLE IF NOT EXISTS phc_error_log_t (
    pel_error_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pel_error_category character varying(90),
    pel_error_description character varying(250),
    pel_created date NOT NULL,
    pel_modified date NOT NULL,
    pel_created_by character varying(20) NOT NULL,
    pel_modified_by character varying(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_industries_t (
    ppi_industry_id bigint DEFAULT nextval('phc_industries_t_ppi_industry_id_seq'::regclass) NOT NULL,
    ppi_industry_code character varying(60) NOT NULL,
    ppi_industry_name character varying(150) NOT NULL,
    ppi_description text,
    ppi_parent_industry_id bigint,
    ppt_start_date date DEFAULT CURRENT_TIMESTAMP,
    ppt_end_date date,
    ppi_status character varying(20) NOT NULL,
    ppi_created_by character varying(100),
    ppi_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppi_modified_by character varying(100),
    ppi_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS phc_item_category_t (
    pic_item_category_id bigint DEFAULT nextval('phc_item_category_t_pic_item_category_id_seq'::regclass) NOT NULL,
    pic_industry_id bigint DEFAULT nextval('phc_item_category_t_pic_industry_id_seq'::regclass) NOT NULL,
    pic_item_category_code character varying(60) NOT NULL,
    pic_item_category_name character varying(150) NOT NULL,
    pic_item_category_desc text,
    pic_parent_item_category_id bigint,
    pic_start_date date DEFAULT CURRENT_DATE NOT NULL,
    pic_end_date date,
    pic_status character varying(20) NOT NULL,
    pic_org_id bigint NOT NULL,
    pic_company_id bigint NOT NULL,
    pic_created_by character varying(100),
    pic_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pic_modified_by character varying(100),
    pic_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_item_family_t (
    pif_item_family_id bigint DEFAULT nextval('phc_item_family_t_pif_item_family_id_seq'::regclass) NOT NULL,
    pif_item_family_code character varying(30) NOT NULL,
    pif_item_family_name character varying(150) NOT NULL,
    pif_item_type_id bigint NOT NULL,
    pif_item_sub_category_id bigint NOT NULL,
    pif_item_category_id bigint NOT NULL,
    pif_description text,
    pif_start_date date DEFAULT CURRENT_DATE NOT NULL,
    pif_end_date date,
    pif_status character varying(20) NOT NULL,
    pif_created_by character varying(100),
    pif_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pif_modified_by character varying(100),
    pif_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pif_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_item_master_t (
    pim_item_id bigint DEFAULT nextval('phc_item_master_t_pim_item_id_seq'::regclass) NOT NULL,
    pim_item_number character varying(60) NOT NULL,
    pim_legacy_item_number character varying(60),
    pim_item_description character varying(255) NOT NULL,
    pim_long_description text,
    pim_item_role_code character varying(60) NOT NULL,
    pim_item_classification_code character varying(60) NOT NULL,
    pim_item_taxonomy_code character varying(60) NOT NULL,
    pim_item_category_id bigint NOT NULL,
    pisc_item_sub_category_id bigint NOT NULL,
    pim_hs_code character varying(60),
    pim_primary_uom_code character varying(60) NOT NULL,
    pim_purchasing_uom_code character varying(60),
    pim_sales_uom_code character varying(60),
    pim_lot_control_code character varying(60) NOT NULL,
    pim_serial_control_flag boolean DEFAULT false,
    pim_shelf_life_days integer DEFAULT 0,
    pim_dual_uom_flag character varying(60) NOT NULL,
    pim_valuation_method_code character varying(60),
    pim_standard_cost numeric(15,4),
    pim_list_price numeric(15,4),
    pim_currency_code character varying(3),
    pim_gl_account_class character varying(60),
    pim_make_buy_flag_code character varying(60),
    pim_safety_stock_qty numeric(12,3),
    pim_reorder_point_qty numeric(12,3),
    pim_min_order_qty numeric(12,3),
    pim_max_order_qty numeric(12,3),
    pim_fixed_lot_multiple numeric(12,3),
    pim_lead_time_days integer DEFAULT 0,
    pim_gross_weight numeric(10,3),
    pim_net_weight numeric(10,3),
    pim_weight_uom_code character varying(60),
    pim_volume numeric(10,3),
    pim_volume_uom_code character varying(60),
    pim_legal_status_code character varying(20),
    pim_industry_id bigint NOT NULL,
    pim_description text,
    pim_start_date date DEFAULT CURRENT_DATE NOT NULL,
    pim_end_date date,
    pim_status character varying(20) NOT NULL,
    pim_created_by character varying(100),
    pim_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pim_modified_by character varying(100),
    pim_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pim_company_id bigint NOT NULL,
    pim_org_id bigint NOT NULL,
    pim_item_family_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_item_sub_category_t (
    pisc_item_sub_category_id bigint DEFAULT nextval('phc_item_sub_category_t_pisc_item_sub_category_id_seq'::regclass) NOT NULL,
    pisc_item_sub_category_code character varying(30) NOT NULL,
    pisc_item_sub_category_name character varying(150) NOT NULL,
    pisc_item_category_id bigint NOT NULL,
    pisc_item_sub_cat_description text,
    pisc_start_date date DEFAULT CURRENT_DATE NOT NULL,
    pisc_end_date date,
    pisc_status character varying(20) NOT NULL,
    pisc_org_id bigint NOT NULL,
    pisc_company_id bigint NOT NULL,
    pisc_created_by character varying(100),
    pisc_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pisc_modified_by character varying(100),
    pisc_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_item_type_t (
    pit_item_type_id bigint DEFAULT nextval('phc_item_type_t_pit_item_type_id_seq'::regclass) NOT NULL,
    pit_item_type_code character varying(30) NOT NULL,
    pit_item_type_name character varying(150) NOT NULL,
    pit_item_sub_category_id bigint NOT NULL,
    pit_description text,
    pit_start_date date DEFAULT CURRENT_DATE NOT NULL,
    pit_end_date date,
    pit_status character varying(20) NOT NULL,
    pit_created_by character varying(100),
    pit_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pit_modified_by character varying(100),
    pit_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pit_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_locations_t (
    plc_location_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    plc_company_id bigint NOT NULL,
    plc_location_code character varying(20) NOT NULL,
    plc_location_name character varying(250) NOT NULL,
    plc_created date NOT NULL,
    plc_modified date NOT NULL,
    plc_created_by character varying(20) NOT NULL,
    plc_modified_by character varying(20),
    plc_start_date date,
    plc_end_date date,
    plc_status character varying(3),
    plc_address_details jsonb DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS phc_lookup_types_t (
    plt_lookup_type_code character varying(60) NOT NULL,
    plt_lookup_type character varying(120) NOT NULL,
    plt_lookup_type_desc character varying(5000),
    plt_start_date date NOT NULL,
    plt_end_date date,
    plt_status character varying(3) DEFAULT 'ACT'::character varying,
    plt_created timestamp without time zone DEFAULT now() NOT NULL,
    plt_modified timestamp without time zone DEFAULT now() NOT NULL,
    plt_created_by character varying(20) DEFAULT '1'::character varying NOT NULL,
    plt_modified_by character varying(20) DEFAULT '1'::character varying NOT NULL,
    plt_company_id bigint NOT NULL,
    plt_lookup_type_code_primary character varying(60)
);

CREATE TABLE IF NOT EXISTS phc_lookup_values_t (
    plv_lookup_value_code character varying(60) NOT NULL,
    plv_lookup_type_code character varying(60) NOT NULL,
    plv_lookup_value_name character varying(60),
    plv_lookup_value_desc character varying(500),
    plv_status character varying(3) DEFAULT 'ACT'::character varying NOT NULL,
    plv_start_date date DEFAULT CURRENT_DATE NOT NULL,
    plv_end_date date,
    plv_created timestamp without time zone DEFAULT now() NOT NULL,
    plv_modified timestamp without time zone DEFAULT now() NOT NULL,
    plv_created_by character varying(20) DEFAULT '1'::character varying NOT NULL,
    plv_modified_by character varying(20) DEFAULT '1'::character varying NOT NULL,
    "PLV_COMPANY_ID" bigint
);

CREATE TABLE IF NOT EXISTS phc_material_group_master_t (
    pmgm_material_group_id bigint NOT NULL,
    pmgm_company_id bigint NOT NULL,
    pmgm_group_code character varying(20) NOT NULL,
    pmgm_group_name character varying(150) NOT NULL,
    pmgm_description character varying(500),
    pmgm_parent_group_id bigint,
    pmgm_status character varying(20) DEFAULT 'ACTIVE'::character varying,
    pmgm_created_by bigint,
    pmgm_created_date timestamp without time zone DEFAULT now(),
    pmgm_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_material_master_t (
    pmat_material_id bigint NOT NULL,
    pmat_company_id bigint NOT NULL,
    pmat_material_code character varying(60) NOT NULL,
    pmat_material_name character varying(250) NOT NULL,
    pmat_material_group_id bigint NOT NULL,
    pmat_material_type character varying(60) NOT NULL,
    pmat_material_category character varying(60),
    pmat_material_description character varying(500),
    pmat_base_uom_id bigint NOT NULL,
    pmat_alt_uom_id bigint,
    pmat_hsn_id bigint,
    pmat_vendor_id bigint,
    pmat_status character varying(60) DEFAULT 'ACTIVE'::character varying,
    pmat_storage_condition character varying(100),
    pmat_shelf_life_days integer,
    pmat_reorder_level numeric(18,4),
    pmat_min_stock numeric(18,4),
    pmat_max_stock numeric(18,4),
    pmat_standard_cost numeric(18,2),
    pmat_qc_required character varying(1) DEFAULT 'Y'::character varying,
    pmat_gmp_material character varying(1) DEFAULT 'Y'::character varying,
    pmat_active_flag character varying(1) DEFAULT 'Y'::character varying,
    pmat_created_by bigint,
    pmat_created_date timestamp without time zone DEFAULT now(),
    pmat_modified_by bigint,
    pmat_modified_date timestamp without time zone,
    pmat_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_module_t (
    pmd_module_id integer DEFAULT nextval('phc_module_t_pmd_module_id_seq'::regclass) NOT NULL,
    pmd_module_name character varying(100) NOT NULL,
    pmd_status character varying(3) DEFAULT 'ACT'::character varying,
    pmd_start_date date DEFAULT CURRENT_DATE,
    pmd_end_date date,
    pmd_created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    pmd_edited_on timestamp without time zone,
    pmd_created_by character varying(50),
    pmd_edited_by character varying(50),
    pmd_module_icon character varying(50)
);

CREATE TABLE IF NOT EXISTS phc_notifications_setup_t (
    pns_notification_id bigint NOT NULL,
    pns_message_type character varying(60) NOT NULL,
    pns_message_name character varying(2000) NOT NULL,
    pns_status character varying(8) DEFAULT 'OPEN'::character varying NOT NULL,
    pns_start_date date NOT NULL,
    pns_end_date date,
    pns_created timestamp without time zone DEFAULT now() NOT NULL,
    pns_modified timestamp without time zone DEFAULT now() NOT NULL,
    pns_created_by character varying(20) NOT NULL,
    pns_modified_by character varying(20) NOT NULL,
    pns_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_orgs_t (
    pos_org_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pos_company_id bigint NOT NULL,
    pos_org_code character varying(20) NOT NULL,
    pos_org_name character varying(250) NOT NULL,
    pos_created date DEFAULT CURRENT_DATE NOT NULL,
    pos_modified date DEFAULT CURRENT_DATE NOT NULL,
    pos_created_by character varying(20) NOT NULL,
    pos_modified_by character varying(20) NOT NULL,
    pos_start_date date,
    pos_end_date date,
    pos_status character varying(3)
);

CREATE TABLE IF NOT EXISTS phc_partners_t (
    part_partner_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    part_partner_number character varying(30) NOT NULL,
    part_partner_name character varying(240) NOT NULL,
    part_partner_type character varying(30) NOT NULL,
    part_partner_category character varying(150),
    part_duns_number character varying(60),
    part_status character varying(3),
    part_start_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    part_end_date timestamp without time zone,
    part_url character varying(240),
    part_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    part_modified timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    part_created_by character varying(20) NOT NULL,
    part_modified_by character varying(20) NOT NULL,
    part_company_id bigint,
    part_org_id bigint,
    "PART_about" text
);

CREATE TABLE IF NOT EXISTS phc_plant_compliance_t (
    ppc_compliance_id bigint NOT NULL,
    ppc_company_id bigint NOT NULL,
    ppc_plant_id bigint NOT NULL,
    ppc_permit_type character varying(100) NOT NULL,
    ppc_permit_number character varying(100),
    ppc_issuing_authority character varying(150),
    ppc_issue_date date,
    ppc_expiry_date date,
    ppc_status character varying(3) DEFAULT 'ACT'::character varying NOT NULL,
    ppc_last_audit_date date,
    ppc_audit_score numeric(5,2),
    ppc_audit_findings character varying(4000),
    ppc_created timestamp without time zone DEFAULT now() NOT NULL,
    ppc_modified timestamp without time zone DEFAULT now() NOT NULL,
    ppc_created_by character varying(20) NOT NULL,
    ppc_modified_by character varying(20) NOT NULL,
    ppc_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_plant_equipment_t (
    ppm_equipment_id bigint NOT NULL,
    ppm_company_id bigint NOT NULL,
    ppm_plant_id bigint NOT NULL,
    ppm_equipment_name character varying(250) NOT NULL,
    ppm_equipment_type character varying(60),
    ppm_manufacturer character varying(250),
    ppm_model_no character varying(250),
    ppm_capacity_value numeric(14,2),
    ppm_capacity_uom character varying(60),
    ppm_serial_no character varying(50),
    ppm_cleaning_type character varying(60),
    ppm_cleaning_sop_id bigint,
    ppm_install_date date,
    ppm_age_years integer,
    ppm_condition character varying(30),
    ppm_status character varying(3) DEFAULT 'ACT'::character varying NOT NULL,
    ppm_created timestamp without time zone DEFAULT now() NOT NULL,
    ppm_modified timestamp without time zone DEFAULT now() NOT NULL,
    ppm_created_by character varying(20) NOT NULL,
    ppm_modified_by character varying(20) NOT NULL,
    ppm_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_plant_master_t (
    ppm_plant_id bigint GENERATED ALWAYS AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    ppm_plant_code character varying(60) NOT NULL,
    ppm_plant_name character varying(250) NOT NULL,
    ppm_parent_company character varying(250),
    ppm_sector_code character varying(60),
    ppm_ownership_code character varying(60),
    ppm_status character varying(3) NOT NULL,
    ppm_location_id bigint NOT NULL,
    ppm_year_established integer,
    ppm_commissioned_date date NOT NULL,
    ppm_decommissioned_date date,
    ppm_created date DEFAULT CURRENT_DATE NOT NULL,
    ppm_modified date DEFAULT CURRENT_DATE NOT NULL,
    ppm_created_by character varying(20) NOT NULL,
    ppm_modified_by character varying(20) NOT NULL,
    ppm_land_area_sqm numeric(14,2),
    ppm_built_up_area_sqm numeric(14,2),
    ppm_warehouse_capacity numeric(14,2),
    ppm_warehouse_uom character varying(20),
    ppm_power_capacity_mw numeric(10,2),
    ppm_backup_power_flag character varying(1),
    ppm_water_supply_kl_day numeric(12,2),
    ppm_gas_supply_flag character varying(3),
    ppm_steam_supply_flag character varying(3),
    ppm_scada_iot_flag character varying(3),
    ppm_org_id bigint NOT NULL,
    "PPM_COMPANY_ID" bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_prod_alt_names_t (
    pan_alt_name_id bigint NOT NULL,
    pan_company_id bigint NOT NULL,
    pan_product_id bigint NOT NULL,
    pan_name_type character varying(30) NOT NULL,
    pan_name_value character varying(200) NOT NULL,
    pan_country character varying(100),
    pan_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_prod_attributes_template_details_t (
    ppatd_attributes_detail_temp_id bigint DEFAULT nextval('phc_prod_attributes_template__ppatd_attributes_detail_temp__seq'::regclass) NOT NULL,
    ppatd_attributes_template_master_id bigint NOT NULL,
    ppatd_attributes_name character varying(250) NOT NULL,
    ppatd_description text,
    ppatd_start_date date,
    ppatd_end_date date,
    ppatd_status character varying(20) NOT NULL,
    ppatd_created_by character varying(100),
    ppatd_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppatd_modified_by character varying(100),
    ppatd_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppatd_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_prod_attributes_template_master_t (
    ppatm_attributes_template_master_id bigint DEFAULT nextval('phc_prod_attributes_template__ppatm_attributes_template_mas_seq'::regclass) NOT NULL,
    ppatm_industry_id bigint NOT NULL,
    ppatm_attributes_master_name character varying(250) NOT NULL,
    ppatm_description text,
    ppatm_start_date date,
    ppatm_end_date date,
    ppatm_status character varying(20) NOT NULL,
    ppatm_created_by character varying(100),
    ppatm_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppatm_modified_by character varying(100),
    ppatm_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppatm_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_prod_lifecycle_history_t (
    plh_lifecycle_hist_id bigint NOT NULL,
    plh_company_id bigint NOT NULL,
    plh_product_id bigint NOT NULL,
    plh_stage_code character varying(20) NOT NULL,
    plh_stage_start_date date NOT NULL,
    plh_stage_end_date date,
    plh_notes character varying(1000),
    plh_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_attributes_t (
    ppatr_attributes_id bigint DEFAULT nextval('phc_product_attributes_t_ppatr_attributes_id_seq'::regclass) NOT NULL,
    ppatr_product_id bigint NOT NULL,
    ppatr_attributes_detail_temp_id bigint NOT NULL,
    ppatr_attributes_name character varying(250) NOT NULL,
    ppatr_description text,
    ppatr_start_date date,
    ppatr_end_date date,
    ppatr_status character varying(20) NOT NULL,
    ppatr_created_by character varying(100),
    ppatr_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppatr_modified_by character varying(100),
    ppatr_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppatr_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_category_t (
    ppc_category_id bigint DEFAULT nextval('phc_product_category_t_ppc_category_id_seq'::regclass) NOT NULL,
    ppc_industry_id bigint DEFAULT nextval('phc_product_category_t_ppc_industry_id_seq'::regclass) NOT NULL,
    ppc_category_code character varying(60) NOT NULL,
    ppc_category_name character varying(150) NOT NULL,
    ppc_description text,
    ppc_parent_category_id bigint,
    ppc_start_date date,
    ppc_end_date date,
    ppc_status character varying(20) NOT NULL,
    ppc_created_by character varying(100),
    ppc_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppc_modified_by character varying(100),
    ppc_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_group_t (
    phc_product_group_id bigint DEFAULT nextval('phc_product_group_t_phc_product_group_id_seq'::regclass) NOT NULL,
    phc_product_group_code character varying(30) NOT NULL,
    phc_product_group_name character varying(150) NOT NULL,
    phc_product_type_id bigint NOT NULL,
    phc_description text,
    phc_start_date date,
    phc_end_date date,
    phc_status character varying(20) NOT NULL,
    phc_created_by character varying(100),
    phc_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    phc_modified_by character varying(100),
    phc_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_sku_t (
    ppsku_sku_id bigint DEFAULT nextval('phc_product_sku_t_ppsku_sku_id_seq'::regclass) NOT NULL,
    ppsku_sku_code character varying(80) NOT NULL,
    ppsku_product_id bigint NOT NULL,
    ppsku_variant_id bigint,
    ppsku_sku_name character varying(250) NOT NULL,
    ppsku_barcode character varying(100),
    ppsku_purchase_uom_id bigint,
    ppsku_sales_uom_id bigint,
    ppsku_inventory_uom_id bigint,
    ppsku_start_date date,
    ppsku_end_date date,
    ppsku_status character varying(20) NOT NULL,
    ppsku_created_by character varying(100),
    ppsku_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppsku_modified_by character varying(100),
    ppsku_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppsku_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_sub_category_t (
    ppsc_sub_category_id bigint DEFAULT nextval('phc_product_sub_category_t_ppsc_sub_category_id_seq'::regclass) NOT NULL,
    ppsc_sub_category_code character varying(30) NOT NULL,
    ppsc_sub_category_name character varying(150) NOT NULL,
    ppsc_category_id bigint NOT NULL,
    ppsc_description text,
    ppsc_start_date date,
    ppsc_end_date date,
    ppsc_status character varying(20) NOT NULL,
    ppsc_created_by character varying(100),
    ppsc_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppsc_modified_by character varying(100),
    ppsc_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_type_t (
    ppt_product_type_id bigint DEFAULT nextval('phc_product_type_t_ppt_product_type_id_seq'::regclass) NOT NULL,
    ppt_product_type_code character varying(30) NOT NULL,
    ppt_product_type_name character varying(150) NOT NULL,
    ppt_sub_category_id bigint NOT NULL,
    ppt_description text,
    ppt_start_date date,
    ppt_end_date date,
    ppt_status character varying(20) NOT NULL,
    ppt_created_by character varying(100),
    ppt_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppt_modified_by character varying(100),
    ppt_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_product_variant_t (
    ppv_variant_id bigint DEFAULT nextval('phc_product_variant_t_ppv_variant_id_seq'::regclass) NOT NULL,
    ppv_product_id bigint NOT NULL,
    ppv_variant_code character varying(50) NOT NULL,
    ppv_variant_name character varying(250) NOT NULL,
    ppv_description text,
    ppv_start_date date,
    ppv_end_date date,
    ppv_status character varying(20) NOT NULL,
    ppv_created_by character varying(100),
    ppv_created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppv_modified_by character varying(100),
    ppv_modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ppv_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_products_master_t (
    pma_product_id bigint NOT NULL,
    pma_company_id bigint NOT NULL,
    pma_product_name character varying(200) NOT NULL,
    pma_uom character varying(20),
    pma_active_flag character varying(3),
    pma_global_product_code character varying(40) NOT NULL,
    pma_brand_name character varying(200) NOT NULL,
    pma_generic_name_inn character varying(200),
    pma_chemical_name character varying(500),
    pma_molecular_formula character varying(200),
    pma_molecular_weight numeric(12,4),
    pma_smiles_string character varying(2000),
    pma_inchi_string character varying(2000),
    pma_cas_number character varying(30),
    pma_atc_code character varying(20),
    pma_unii_code character varying(20),
    pma_dosage_form character varying(250),
    pma_strength character varying(250),
    pma_tdd numeric(18,6),
    pma_safety_factor numeric(8,2),
    pma_cleaning_agent_id bigint,
    pma_idmp_substance_id character varying(50),
    pma_legal_status_code character varying(20),
    pma_controlled_substance_schedule character varying(20),
    pma_product_family character varying(200),
    pma_parent_product_id bigint,
    pma_current_lifecycle_stage_code character varying(20),
    pma_created_by character varying(50),
    pma_created_date timestamp without time zone DEFAULT now(),
    pma_last_updated_by character varying(50),
    pma_last_updated_date timestamp without time zone,
    pma_org_id bigint NOT NULL,
    pma_product_group_id bigint NOT NULL,
    pma_attributes_template_master_id bigint
);

CREATE TABLE IF NOT EXISTS phc_role_screen_assignment_t (
    prs_role_screen_id integer DEFAULT nextval('phc_role_screen_assignment_t_prs_role_screen_id_seq'::regclass) NOT NULL,
    prs_company_id bigint NOT NULL,
    prs_role_id bigint NOT NULL,
    prs_screen_id bigint NOT NULL,
    prs_start_date date NOT NULL,
    prs_end_date date,
    prs_status character varying(3) NOT NULL,
    prs_notes character varying(60),
    prs_created timestamp without time zone NOT NULL,
    prs_modified timestamp without time zone NOT NULL,
    prs_created_by character varying(20) NOT NULL,
    prs_modified_by character varying(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_roles_t (
    prl_role_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    prl_company_id bigint NOT NULL,
    prl_role_code character varying(20) NOT NULL,
    prl_role_name character varying(250) NOT NULL,
    prl_created date DEFAULT CURRENT_DATE NOT NULL,
    prl_modified date DEFAULT CURRENT_DATE NOT NULL,
    prl_created_by character varying(20) NOT NULL,
    prl_modified_by character varying(20) NOT NULL,
    prl_start_date date,
    prl_end_date date,
    prl_status character varying(3)
);

CREATE TABLE IF NOT EXISTS phc_rooms_t (
    prm_room_id integer DEFAULT nextval('phc_rooms_t_prm_room_id_seq'::regclass) NOT NULL,
    prm_building_id integer,
    prm_room_name character varying(100) NOT NULL,
    prm_room_number character varying(50) NOT NULL,
    prm_clean_class character varying(150),
    prm_target_temp_c numeric(4,2) DEFAULT 22.0,
    prm_target_humidity_pct numeric(4,2) DEFAULT 45.0,
    prm_target_dp_pa numeric(5,2) DEFAULT 15.0,
    prm_status character varying(3) NOT NULL,
    prm_remarks text,
    prm_created timestamp with time zone DEFAULT now() NOT NULL,
    prm_modified timestamp with time zone DEFAULT now() NOT NULL,
    prm_created_by character varying(20) NOT NULL,
    prm_modified_by character varying(20) NOT NULL,
    prm_company_id bigint NOT NULL,
    prm_floor_id integer DEFAULT 1 NOT NULL,
    prm_cleanroom_grade character varying(100),
    prm_usable_area_sqm numeric(8,2),
    prm_clear_height_m numeric(4,2),
    prm_max_wip_pallets integer,
    prm_max_batch_vol_limit integer,
    prm_max_equipment_vol_limit integer,
    prm_max_operator_limit integer,
    prm_ehs_containment_rating character varying(100),
    prm_temp_range_c character varying(150)
);

CREATE TABLE IF NOT EXISTS phc_screens_t (
    psn_screen_id integer DEFAULT nextval('phc_screens_t_psn_screen_id_seq'::regclass) NOT NULL,
    psn_company_id bigint NOT NULL,
    psn_screen_code character varying(150) NOT NULL,
    psn_screen_name character varying(250) NOT NULL,
    psn_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    psn_modified timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    psn_created_by character varying(20) NOT NULL,
    psn_modified_by character varying(20) NOT NULL,
    psn_start_date date,
    psn_end_date date,
    psn_status character varying(3) DEFAULT 'ACT'::character varying,
    psn_module_id integer
);

CREATE TABLE IF NOT EXISTS phc_services_t (
    pss_services_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pss_dept_id bigint NOT NULL,
    pss_services_code character varying(20) NOT NULL,
    pss_services_name character varying(250) NOT NULL,
    pss_created date DEFAULT CURRENT_DATE NOT NULL,
    pss_modified date DEFAULT CURRENT_DATE NOT NULL,
    pss_created_by character varying(20) NOT NULL,
    pss_modified_by character varying(20) NOT NULL,
    pss_company_id bigint,
    pss_start_date date,
    pss_end_date date,
    pss_status character varying(3)
);

CREATE TABLE IF NOT EXISTS phc_states_t (
    pst_state_iso3_code character varying(20) NOT NULL,
    pst_state_name character varying(150) NOT NULL,
    pst_state_type character varying(50),
    pst_state_capital_city_code character varying(60),
    pst_latitude numeric(10,6),
    pst_longitude numeric(10,6),
    pst_country_iso3 character varying(3) NOT NULL,
    pst_status character varying(3) NOT NULL,
    pst_remarks text,
    pst_created timestamp with time zone DEFAULT now() NOT NULL,
    pst_modified timestamp with time zone DEFAULT now() NOT NULL,
    pst_created_by character varying(20) NOT NULL,
    pst_modified_by character varying(20) NOT NULL,
    pst_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_storage_location_master_t (
    pslm_storage_location_id bigint GENERATED ALWAYS AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pslm_storage_location_code character varying(30) NOT NULL,
    pslm_storage_location_name character varying(200) NOT NULL,
    pslm_location_description character varying(500),
    pslm_plant_id bigint,
    pslm_location_id bigint,
    pslm_start_date timestamp without time zone,
    pslm_end_date timestamp without time zone,
    pslm_building_id bigint,
    pslm_warehouse_id bigint NOT NULL,
    pslm_area_id bigint,
    pslm_zone_id bigint,
    pslm_aisle character varying(20),
    pslm_rack character varying(20),
    pslm_shelf character varying(20),
    pslm_bin character varying(20),
    pslm_location_type character varying(30),
    pslm_storage_category character varying(30),
    pslm_storage_class character varying(30),
    pslm_temperature_min numeric(5,2),
    pslm_temperature_max numeric(5,2),
    pslm_humidity_min numeric(5,2),
    pslm_humidity_max numeric(5,2),
    pslm_uom_id bigint,
    pslm_max_capacity numeric(18,4),
    pslm_current_capacity numeric(18,4),
    pslm_available_capacity numeric(18,4),
    pslm_fifo_flag character varying(3) DEFAULT 'Y'::character varying,
    pslm_fefo_flag character varying(3) DEFAULT 'Y'::character varying,
    pslm_gmp_area_flag character varying(3) DEFAULT 'Y'::character varying,
    pslm_clean_room_grade character varying(20),
    pslm_quarantine_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_reject_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_return_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_hazardous_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_flammable_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_narcotic_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_cleaning_agent_flag character varying(3) DEFAULT 'N'::character varying,
    pslm_status character varying(3),
    pslm_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pslm_modified timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    pslm_created_by character varying(20) NOT NULL,
    pslm_modified_by character varying(20) NOT NULL,
    pslm_company_id bigint NOT NULL,
    pslm_org_id bigint
);

CREATE TABLE IF NOT EXISTS phc_storage_locator_t (
    psl_locator_id integer DEFAULT nextval('phc_storage_locator_t_psl_locator_id_seq'::regclass) NOT NULL,
    psl_room_id integer,
    psl_aisle_code character varying(10) NOT NULL,
    psl_rack_code character varying(50) NOT NULL,
    psl_shelf_level character varying(10) NOT NULL,
    psl_shelve_number character varying(50) NOT NULL,
    psl_bin_position character varying(10) NOT NULL,
    psl_bin_barcode character varying(100) NOT NULL,
    psl_locator_code character varying(100) NOT NULL,
    psl_is_pickable boolean DEFAULT true,
    psl_is_receiving_staging boolean DEFAULT false,
    psl_is_quarantine_zone boolean DEFAULT false,
    psl_max_weight_kg numeric(10,2),
    psl_status character varying(3) NOT NULL,
    psl_remarks text,
    psl_created timestamp with time zone DEFAULT now() NOT NULL,
    psl_modified timestamp with time zone DEFAULT now() NOT NULL,
    psl_created_by character varying(20) NOT NULL,
    psl_modified_by character varying(20) NOT NULL,
    psl_company_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_uom_conversion_t (
    puc_conversion_id bigint NOT NULL,
    puc_company_id bigint NOT NULL,
    puc_from_uom_id bigint NOT NULL,
    puc_to_uom_id bigint NOT NULL,
    puc_conversion_factor numeric(30,12),
    puc_formula character varying(500),
    puc_rounding_precision integer,
    puc_rounding_method character varying(20),
    puc_effective_from date,
    puc_effective_to date,
    puc_active_flag character varying(1) DEFAULT 'Y'::character varying,
    puc_created_date timestamp without time zone DEFAULT now(),
    puc_created_by bigint,
    puc_last_updated_by bigint,
    puc_last_updated_date timestamp without time zone,
    puc_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_uom_master_t (
    pum_uom_id bigint NOT NULL,
    pum_company_id bigint NOT NULL,
    pum_uom_code character varying(60) NOT NULL,
    pum_uom_name character varying(100) NOT NULL,
    pum_uom_short_name character varying(20),
    pum_uom_symbol character varying(20),
    pum_uom_category character varying(60) NOT NULL,
    pum_si_unit_flag character varying(1) DEFAULT 'Y'::character varying,
    pum_base_uom_flag character varying(1) DEFAULT 'N'::character varying,
    pum_base_uom_id bigint,
    pum_conversion_factor numeric(30,12),
    pum_conversion_formula character varying(500),
    pum_decimal_precision integer DEFAULT 3,
    pum_rounding_method character varying(20),
    pum_min_decimal integer,
    pum_max_decimal integer,
    pum_allow_negative character varying(1) DEFAULT 'N'::character varying,
    pum_fraction_allowed character varying(1) DEFAULT 'Y'::character varying,
    pum_scientific_notation_flag character varying(1) DEFAULT 'N'::character varying,
    pum_active_flag character varying(1) DEFAULT 'Y'::character varying,
    pum_effective_from date,
    pum_effective_to date,
    pum_remarks character varying(1000),
    pum_attribute1 character varying(200),
    pum_attribute2 character varying(200),
    pum_attribute3 character varying(200),
    pum_attribute4 character varying(200),
    pum_attribute5 character varying(200),
    pum_created_by bigint,
    pum_created_date timestamp without time zone DEFAULT now(),
    pum_last_updated_by bigint,
    pum_last_updated_date timestamp without time zone,
    pum_org_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_user_app_roles_assignment_t (
    pua_user_app_role_assig_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pua_company_id bigint NOT NULL,
    pua_user_id bigint NOT NULL,
    pua_role_id bigint NOT NULL,
    pua_apps_id bigint NOT NULL,
    pua_start_date date NOT NULL,
    pua_end_date date,
    pua_status character varying(3) NOT NULL,
    pua_notes character varying(60),
    pua_created date NOT NULL,
    pua_modified date NOT NULL,
    pua_created_by character varying(20) NOT NULL,
    pua_modified_by character varying(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_user_group_t (
    pug_ug_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pug_parent bigint,
    pug_group_title character varying(90),
    pug_group_description character varying(250),
    pug_start_date date DEFAULT CURRENT_DATE NOT NULL,
    pug_end_date date,
    pug_status character varying(3) DEFAULT 'AIU'::character varying NOT NULL,
    pug_created date NOT NULL,
    pug_modified date NOT NULL,
    pug_created_by character varying(20) DEFAULT CURRENT_DATE NOT NULL,
    pug_modified_by character varying(20) DEFAULT CURRENT_DATE NOT NULL,
    pug_company_id bigint
);

CREATE TABLE IF NOT EXISTS phc_user_log_t (
    pul_log_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pul_parent bigint,
    pul_description character varying(250),
    pul_created date NOT NULL,
    pul_modified date NOT NULL,
    pul_created_by character varying(20) NOT NULL,
    pul_modified_by character varying(20) NOT NULL,
    pul_company_id bigint
);

CREATE TABLE IF NOT EXISTS phc_user_notifications_t (
    pun_notification_id bigint DEFAULT nextval('phc_user_notifications_t_pun_notification_id_seq'::regclass) NOT NULL,
    pun_recipient_user_id bigint,
    pun_recipient_role character varying(50),
    pun_title character varying(150) NOT NULL,
    pun_message text NOT NULL,
    pun_category character varying(50) DEFAULT 'WORKFLOW'::character varying,
    pun_link_url character varying(255),
    pun_is_read boolean DEFAULT false,
    pun_created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS phc_user_roles_assignment_t (
    pua_user_role_assig_id integer DEFAULT nextval('phc_user_roles_assignment_t_pua_user_role_assig_id_seq'::regclass) NOT NULL,
    pua_company_id bigint NOT NULL,
    pua_user_id bigint NOT NULL,
    pua_role_id bigint NOT NULL,
    pua_apps_id bigint,
    pua_start_date date NOT NULL,
    pua_end_date date,
    pua_status character varying(3) NOT NULL,
    pua_notes character varying(60),
    pua_created timestamp without time zone NOT NULL,
    pua_modified timestamp without time zone NOT NULL,
    pua_created_by character varying(20) NOT NULL,
    pua_modified_by character varying(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS phc_users_t (
    pus_user_id bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1) NOT NULL,
    pus_company_id bigint NOT NULL,
    pus_emp_id bigint,
    pus_user_name character varying(10) NOT NULL,
    pus_full_name character varying(250) NOT NULL,
    pus_created date DEFAULT CURRENT_DATE NOT NULL,
    pus_modified date DEFAULT CURRENT_DATE NOT NULL,
    pus_created_by character varying(20) NOT NULL,
    pus_modified_by character varying(20) NOT NULL,
    pus_image bytea,
    pus_user_type character varying(10) DEFAULT 'STD'::character varying NOT NULL,
    pus_start_date date NOT NULL,
    pus_end_date date,
    pus_status character varying(3) DEFAULT 'AIU'::character varying NOT NULL,
    pus_email_id character varying(120),
    pus_pwd character varying(90),
    pus_session_id character varying(255),
    pus_failed_attempts integer DEFAULT 0,
    pus_locked_until timestamp with time zone
);

-- ===================== SEQUENCE OWNERSHIP =====================
ALTER SEQUENCE pcv_cleaning_process_records_t_pcv_cpr_id_seq OWNED BY pcv_cleaning_process_records_t.pcv_cpr_id;
ALTER SEQUENCE pcv_cpr_execution_steps_t_pcv_step_id_seq OWNED BY pcv_cpr_execution_steps_t.pcv_step_id;
ALTER SEQUENCE pcv_equipment_clearance_checklists_t_pcv_clearance_id_seq OWNED BY pcv_equipment_clearance_checklists_t.pcv_clearance_id;
ALTER SEQUENCE pcv_equipment_clearance_items_t_pcv_item_id_seq OWNED BY pcv_equipment_clearance_items_t.pcv_item_id;
ALTER SEQUENCE pcv_equipment_sampling_locations_t_pcv_sampling_loc_id_seq OWNED BY pcv_equipment_sampling_locations_t.pcv_sampling_loc_id;
ALTER SEQUENCE pcv_equipment_surface_areas_t_pcv_surface_area_id_seq OWNED BY pcv_equipment_surface_areas_t.pcv_surface_area_id;
ALTER SEQUENCE pcv_mdd_api_details_t_pcv_mdd_detail_id_seq OWNED BY pcv_mdd_api_details_t.pcv_mdd_detail_id;
ALTER SEQUENCE pcv_mdd_registrations_t_pcv_mdd_id_seq OWNED BY pcv_mdd_registrations_t.pcv_mdd_id;
ALTER SEQUENCE pcv_pde_api_details_t_pcv_pde_detail_id_seq OWNED BY pcv_pde_api_details_t.pcv_pde_detail_id;
ALTER SEQUENCE pcv_pde_registrations_t_pcv_pde_id_seq OWNED BY pcv_pde_registrations_t.pcv_pde_id;
ALTER SEQUENCE pcv_product_batch_sizes_t_pcv_batch_size_id_seq OWNED BY pcv_product_batch_sizes_t.pcv_batch_size_id;
ALTER SEQUENCE pcv_product_equipment_mapping_t_pcv_mapping_id_seq OWNED BY pcv_product_equipment_mapping_t.pcv_mapping_id;
ALTER SEQUENCE pcv_product_pack_styles_t_pcv_pack_style_id_seq OWNED BY pcv_product_pack_styles_t.pcv_pack_style_id;
ALTER SEQUENCE pcv_product_stages_t_pcv_stage_id_seq OWNED BY pcv_product_stages_t.pcv_stage_id;
ALTER SEQUENCE pcv_product_strengths_t_pcv_strength_id_seq OWNED BY pcv_product_strengths_t.pcv_strength_id;
ALTER SEQUENCE pcv_products_t_pcv_product_id_seq OWNED BY pcv_production_products_t.pcv_product_id;
ALTER SEQUENCE pcv_sampling_records_t_pcv_sampling_record_id_seq OWNED BY pcv_sampling_records_t.pcv_sampling_record_id;
ALTER SEQUENCE pcv_solubility_details_t_pcv_solubility_id_seq OWNED BY pcv_solubility_details_t.pcv_solubility_id;
ALTER SEQUENCE pcv_test_methods_t_pcv_method_id_seq OWNED BY pcv_test_methods_t.pcv_method_id;
ALTER SEQUENCE pcv_test_request_forms_t_pcv_trf_id_seq OWNED BY pcv_test_request_forms_t.pcv_trf_id;
ALTER SEQUENCE pcv_test_results_t_pcv_result_id_seq OWNED BY pcv_test_results_t.pcv_result_id;
ALTER SEQUENCE pcv_training_attendees_t_pcv_attendee_id_seq OWNED BY pcv_training_attendees_t.pcv_attendee_id;
ALTER SEQUENCE pcv_training_records_t_pcv_training_id_seq OWNED BY pcv_training_records_t.pcv_training_id;
ALTER SEQUENCE pcv_validation_executions_t_pcv_execution_id_seq OWNED BY pcv_validation_executions_t.pcv_execution_id;
ALTER SEQUENCE pcv_validation_reports_t_pcv_report_id_seq OWNED BY pcv_validation_reports_t.pcv_report_id;
ALTER SEQUENCE phc_audit_log_t_pal_audit_id_seq OWNED BY phc_audit_log_t.pal_audit_id;
ALTER SEQUENCE phc_building_floor_t_pfl_floor_id_seq OWNED BY phc_building_floor_t.pfl_floor_id;
ALTER SEQUENCE phc_building_t_plc_building_id_seq OWNED BY phc_building_t.plc_building_id;
ALTER SEQUENCE phc_document_number_range_set_pnrm_document_number_range_id_seq OWNED BY phc_document_number_range_setup_t.pnrm_number_range_id;
ALTER SEQUENCE phc_industries_t_ppi_industry_id_seq OWNED BY phc_industries_t.ppi_industry_id;
ALTER SEQUENCE phc_item_category_t_pic_industry_id_seq OWNED BY phc_item_category_t.pic_industry_id;
ALTER SEQUENCE phc_item_category_t_pic_item_category_id_seq OWNED BY phc_item_category_t.pic_item_category_id;
ALTER SEQUENCE phc_item_family_t_pif_item_family_id_seq OWNED BY phc_item_family_t.pif_item_family_id;
ALTER SEQUENCE phc_item_master_t_pim_item_id_seq OWNED BY phc_item_master_t.pim_item_id;
ALTER SEQUENCE phc_item_sub_category_t_pisc_item_sub_category_id_seq OWNED BY phc_item_sub_category_t.pisc_item_sub_category_id;
ALTER SEQUENCE phc_item_type_t_pit_item_type_id_seq OWNED BY phc_item_type_t.pit_item_type_id;
ALTER SEQUENCE phc_module_t_pmd_module_id_seq OWNED BY phc_module_t.pmd_module_id;
ALTER SEQUENCE phc_prod_attributes_template__ppatd_attributes_detail_temp__seq OWNED BY phc_prod_attributes_template_details_t.ppatd_attributes_detail_temp_id;
ALTER SEQUENCE phc_prod_attributes_template__ppatm_attributes_template_mas_seq OWNED BY phc_prod_attributes_template_master_t.ppatm_attributes_template_master_id;
ALTER SEQUENCE phc_product_attributes_t_ppatr_attributes_id_seq OWNED BY phc_product_attributes_t.ppatr_attributes_id;
ALTER SEQUENCE phc_product_category_t_ppc_category_id_seq OWNED BY phc_product_category_t.ppc_category_id;
ALTER SEQUENCE phc_product_category_t_ppc_industry_id_seq OWNED BY phc_product_category_t.ppc_industry_id;
ALTER SEQUENCE phc_product_group_t_phc_product_group_id_seq OWNED BY phc_product_group_t.phc_product_group_id;
ALTER SEQUENCE phc_product_sku_t_ppsku_sku_id_seq OWNED BY phc_product_sku_t.ppsku_sku_id;
ALTER SEQUENCE phc_product_sub_category_t_ppsc_sub_category_id_seq OWNED BY phc_product_sub_category_t.ppsc_sub_category_id;
ALTER SEQUENCE phc_product_type_t_ppt_product_type_id_seq OWNED BY phc_product_type_t.ppt_product_type_id;
ALTER SEQUENCE phc_product_variant_t_ppv_variant_id_seq OWNED BY phc_product_variant_t.ppv_variant_id;
ALTER SEQUENCE phc_role_screen_assignment_t_prs_role_screen_id_seq OWNED BY phc_role_screen_assignment_t.prs_role_screen_id;
ALTER SEQUENCE phc_rooms_t_prm_room_id_seq OWNED BY phc_rooms_t.prm_room_id;
ALTER SEQUENCE phc_screens_t_psn_screen_id_seq OWNED BY phc_screens_t.psn_screen_id;
ALTER SEQUENCE phc_storage_locator_t_psl_locator_id_seq OWNED BY phc_storage_locator_t.psl_locator_id;
ALTER SEQUENCE phc_user_notifications_t_pun_notification_id_seq OWNED BY phc_user_notifications_t.pun_notification_id;
ALTER SEQUENCE phc_user_roles_assignment_t_pua_user_role_assig_id_seq OWNED BY phc_user_roles_assignment_t.pua_user_role_assig_id;

-- ===================== PRIMARY KEYS =====================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cleaning_process_records_t_pkey' AND conrelid = 'public.pcv_cleaning_process_records_t'::regclass) THEN
        ALTER TABLE pcv_cleaning_process_records_t ADD CONSTRAINT pcv_cleaning_process_records_t_pkey PRIMARY KEY (pcv_cpr_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cpr_execution_steps_t_pkey' AND conrelid = 'public.pcv_cpr_execution_steps_t'::regclass) THEN
        ALTER TABLE pcv_cpr_execution_steps_t ADD CONSTRAINT pcv_cpr_execution_steps_t_pkey PRIMARY KEY (pcv_step_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_clearance_checklists_t_pkey' AND conrelid = 'public.pcv_equipment_clearance_checklists_t'::regclass) THEN
        ALTER TABLE pcv_equipment_clearance_checklists_t ADD CONSTRAINT pcv_equipment_clearance_checklists_t_pkey PRIMARY KEY (pcv_clearance_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_clearance_items_t_pkey' AND conrelid = 'public.pcv_equipment_clearance_items_t'::regclass) THEN
        ALTER TABLE pcv_equipment_clearance_items_t ADD CONSTRAINT pcv_equipment_clearance_items_t_pkey PRIMARY KEY (pcv_item_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_sampling_locations_t_pkey' AND conrelid = 'public.pcv_equipment_sampling_locations_t'::regclass) THEN
        ALTER TABLE pcv_equipment_sampling_locations_t ADD CONSTRAINT pcv_equipment_sampling_locations_t_pkey PRIMARY KEY (pcv_sampling_loc_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_surface_areas_t_pkey' AND conrelid = 'public.pcv_equipment_surface_areas_t'::regclass) THEN
        ALTER TABLE pcv_equipment_surface_areas_t ADD CONSTRAINT pcv_equipment_surface_areas_t_pkey PRIMARY KEY (pcv_surface_area_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipments_t_pkey' AND conrelid = 'public.pcv_equipments_t'::regclass) THEN
        ALTER TABLE pcv_equipments_t ADD CONSTRAINT pcv_equipments_t_pkey PRIMARY KEY (pcv_equipment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_mdd_api_details_t_pkey' AND conrelid = 'public.pcv_mdd_api_details_t'::regclass) THEN
        ALTER TABLE pcv_mdd_api_details_t ADD CONSTRAINT pcv_mdd_api_details_t_pkey PRIMARY KEY (pcv_mdd_detail_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_mdd_registrations_t_pkey' AND conrelid = 'public.pcv_mdd_registrations_t'::regclass) THEN
        ALTER TABLE pcv_mdd_registrations_t ADD CONSTRAINT pcv_mdd_registrations_t_pkey PRIMARY KEY (pcv_mdd_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_pde_api_details_t_pkey' AND conrelid = 'public.pcv_pde_api_details_t'::regclass) THEN
        ALTER TABLE pcv_pde_api_details_t ADD CONSTRAINT pcv_pde_api_details_t_pkey PRIMARY KEY (pcv_pde_detail_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_pde_registrations_t_pkey' AND conrelid = 'public.pcv_pde_registrations_t'::regclass) THEN
        ALTER TABLE pcv_pde_registrations_t ADD CONSTRAINT pcv_pde_registrations_t_pkey PRIMARY KEY (pcv_pde_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_batch_sizes_t_pkey' AND conrelid = 'public.pcv_product_batch_sizes_t'::regclass) THEN
        ALTER TABLE pcv_product_batch_sizes_t ADD CONSTRAINT pcv_product_batch_sizes_t_pkey PRIMARY KEY (pcv_batch_size_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_equipment_mapping_t_pkey' AND conrelid = 'public.pcv_product_equipment_mapping_t'::regclass) THEN
        ALTER TABLE pcv_product_equipment_mapping_t ADD CONSTRAINT pcv_product_equipment_mapping_t_pkey PRIMARY KEY (pcv_mapping_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_pack_styles_t_pkey' AND conrelid = 'public.pcv_product_pack_styles_t'::regclass) THEN
        ALTER TABLE pcv_product_pack_styles_t ADD CONSTRAINT pcv_product_pack_styles_t_pkey PRIMARY KEY (pcv_pack_style_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_stages_t_pkey' AND conrelid = 'public.pcv_product_stages_t'::regclass) THEN
        ALTER TABLE pcv_product_stages_t ADD CONSTRAINT pcv_product_stages_t_pkey PRIMARY KEY (pcv_stage_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_strengths_t_pkey' AND conrelid = 'public.pcv_product_strengths_t'::regclass) THEN
        ALTER TABLE pcv_product_strengths_t ADD CONSTRAINT pcv_product_strengths_t_pkey PRIMARY KEY (pcv_strength_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_products_t_pkey' AND conrelid = 'public.pcv_production_products_t'::regclass) THEN
        ALTER TABLE pcv_production_products_t ADD CONSTRAINT pcv_products_t_pkey PRIMARY KEY (pcv_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_sampling_records_t_pkey' AND conrelid = 'public.pcv_sampling_records_t'::regclass) THEN
        ALTER TABLE pcv_sampling_records_t ADD CONSTRAINT pcv_sampling_records_t_pkey PRIMARY KEY (pcv_sampling_record_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_solubility_details_t_pkey' AND conrelid = 'public.pcv_solubility_details_t'::regclass) THEN
        ALTER TABLE pcv_solubility_details_t ADD CONSTRAINT pcv_solubility_details_t_pkey PRIMARY KEY (pcv_solubility_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_methods_t_pkey' AND conrelid = 'public.pcv_test_methods_t'::regclass) THEN
        ALTER TABLE pcv_test_methods_t ADD CONSTRAINT pcv_test_methods_t_pkey PRIMARY KEY (pcv_method_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_request_forms_t_pkey' AND conrelid = 'public.pcv_test_request_forms_t'::regclass) THEN
        ALTER TABLE pcv_test_request_forms_t ADD CONSTRAINT pcv_test_request_forms_t_pkey PRIMARY KEY (pcv_trf_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_results_t_pkey' AND conrelid = 'public.pcv_test_results_t'::regclass) THEN
        ALTER TABLE pcv_test_results_t ADD CONSTRAINT pcv_test_results_t_pkey PRIMARY KEY (pcv_result_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_training_attendees_t_pkey' AND conrelid = 'public.pcv_training_attendees_t'::regclass) THEN
        ALTER TABLE pcv_training_attendees_t ADD CONSTRAINT pcv_training_attendees_t_pkey PRIMARY KEY (pcv_attendee_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_training_records_t_pkey' AND conrelid = 'public.pcv_training_records_t'::regclass) THEN
        ALTER TABLE pcv_training_records_t ADD CONSTRAINT pcv_training_records_t_pkey PRIMARY KEY (pcv_training_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_executions_t_pkey' AND conrelid = 'public.pcv_validation_executions_t'::regclass) THEN
        ALTER TABLE pcv_validation_executions_t ADD CONSTRAINT pcv_validation_executions_t_pkey PRIMARY KEY (pcv_execution_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_reports_t_pkey' AND conrelid = 'public.pcv_validation_reports_t'::regclass) THEN
        ALTER TABLE pcv_validation_reports_t ADD CONSTRAINT pcv_validation_reports_t_pkey PRIMARY KEY (pcv_report_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_events_t_pkey' AND conrelid = 'public.phc_approval_events_t'::regclass) THEN
        ALTER TABLE phc_approval_events_t ADD CONSTRAINT phc_approval_events_t_pkey PRIMARY KEY (pae_appr_event_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_setup_t_pkey' AND conrelid = 'public.phc_approval_setup_t'::regclass) THEN
        ALTER TABLE phc_approval_setup_t ADD CONSTRAINT phc_approval_setup_t_pkey PRIMARY KEY (pas_appr_setup_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_types_t_pkey' AND conrelid = 'public.phc_approval_types_t'::regclass) THEN
        ALTER TABLE phc_approval_types_t ADD CONSTRAINT phc_approval_types_t_pkey PRIMARY KEY (pat_approval_type);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_apps_t_pkey' AND conrelid = 'public.phc_apps_t'::regclass) THEN
        ALTER TABLE phc_apps_t ADD CONSTRAINT phc_apps_t_pkey PRIMARY KEY (pap_apps_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_audit_log_t_pkey' AND conrelid = 'public.phc_audit_log_t'::regclass) THEN
        ALTER TABLE phc_audit_log_t ADD CONSTRAINT phc_audit_log_t_pkey PRIMARY KEY (pal_audit_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_building_floor_t_pkey' AND conrelid = 'public.phc_building_floor_t'::regclass) THEN
        ALTER TABLE phc_building_floor_t ADD CONSTRAINT phc_building_floor_t_pkey PRIMARY KEY (pfl_floor_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_building_t_pkey' AND conrelid = 'public.phc_building_t'::regclass) THEN
        ALTER TABLE phc_building_t ADD CONSTRAINT phc_building_t_pkey PRIMARY KEY (plc_building_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_certifications_pkey' AND conrelid = 'public.phc_certifications_t'::regclass) THEN
        ALTER TABLE phc_certifications_t ADD CONSTRAINT phc_certifications_pkey PRIMARY KEY (pcr_plant_cert_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_cities_t_pkey' AND conrelid = 'public.phc_cities_t'::regclass) THEN
        ALTER TABLE phc_cities_t ADD CONSTRAINT phc_cities_t_pkey PRIMARY KEY (pct_city_code_iso4);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_companies_t_pkey' AND conrelid = 'public.phc_companies_t'::regclass) THEN
        ALTER TABLE phc_companies_t ADD CONSTRAINT phc_companies_t_pkey PRIMARY KEY (pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_cost_center_t_pkey' AND conrelid = 'public.phc_cost_center_t'::regclass) THEN
        ALTER TABLE phc_cost_center_t ADD CONSTRAINT phc_cost_center_t_pkey PRIMARY KEY (pcc_cost_center_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_countries_t_pkey' AND conrelid = 'public.phc_countries_t'::regclass) THEN
        ALTER TABLE phc_countries_t ADD CONSTRAINT phc_countries_t_pkey PRIMARY KEY (pct_country_isosqno);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_dept_t_pkey' AND conrelid = 'public.phc_dept_t'::regclass) THEN
        ALTER TABLE phc_dept_t ADD CONSTRAINT phc_dept_t_pkey PRIMARY KEY (pdp_dept_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_document_number_range_setup_t_pkey' AND conrelid = 'public.phc_document_number_range_setup_t'::regclass) THEN
        ALTER TABLE phc_document_number_range_setup_t ADD CONSTRAINT phc_document_number_range_setup_t_pkey PRIMARY KEY (pnrm_number_range_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_emp_apps_grant_t_pkey' AND conrelid = 'public.phc_emp_apps_grant_t'::regclass) THEN
        ALTER TABLE phc_emp_apps_grant_t ADD CONSTRAINT phc_emp_apps_grant_t_pkey PRIMARY KEY (pea_emp_apps_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phr_emp_assignments_t_pkey' AND conrelid = 'public.phc_emp_assignments_t'::regclass) THEN
        ALTER TABLE phc_emp_assignments_t ADD CONSTRAINT phr_emp_assignments_t_pkey PRIMARY KEY (pea_assignment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_emp_t_pkey' AND conrelid = 'public.phc_emp_t'::regclass) THEN
        ALTER TABLE phc_emp_t ADD CONSTRAINT phc_emp_t_pkey PRIMARY KEY (pem_emp_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_equipment_locations_pkey' AND conrelid = 'public.phc_equipment_locations_t'::regclass) THEN
        ALTER TABLE phc_equipment_locations_t ADD CONSTRAINT phc_equipment_locations_pkey PRIMARY KEY (pel_equ_location_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_error_log_t_pkey' AND conrelid = 'public.phc_error_log_t'::regclass) THEN
        ALTER TABLE phc_error_log_t ADD CONSTRAINT phc_error_log_t_pkey PRIMARY KEY (pel_error_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_industries_t_pkey' AND conrelid = 'public.phc_industries_t'::regclass) THEN
        ALTER TABLE phc_industries_t ADD CONSTRAINT phc_industries_t_pkey PRIMARY KEY (ppi_industry_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_category_t_pkey' AND conrelid = 'public.phc_item_category_t'::regclass) THEN
        ALTER TABLE phc_item_category_t ADD CONSTRAINT phc_item_category_t_pkey PRIMARY KEY (pic_item_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_t_pkey' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_t_pkey PRIMARY KEY (pif_item_family_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_master_t_pkey' AND conrelid = 'public.phc_item_master_t'::regclass) THEN
        ALTER TABLE phc_item_master_t ADD CONSTRAINT phc_item_master_t_pkey PRIMARY KEY (pim_item_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_sub_category_t_pkey' AND conrelid = 'public.phc_item_sub_category_t'::regclass) THEN
        ALTER TABLE phc_item_sub_category_t ADD CONSTRAINT phc_item_sub_category_t_pkey PRIMARY KEY (pisc_item_sub_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_type_t_pkey' AND conrelid = 'public.phc_item_type_t'::regclass) THEN
        ALTER TABLE phc_item_type_t ADD CONSTRAINT phc_item_type_t_pkey PRIMARY KEY (pit_item_type_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_locations_t_pkey' AND conrelid = 'public.phc_locations_t'::regclass) THEN
        ALTER TABLE phc_locations_t ADD CONSTRAINT phc_locations_t_pkey PRIMARY KEY (plc_location_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_lookup_types_pkey' AND conrelid = 'public.phc_lookup_types_t'::regclass) THEN
        ALTER TABLE phc_lookup_types_t ADD CONSTRAINT phc_lookup_types_pkey PRIMARY KEY (plt_lookup_type_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_lookup_values_t_pkey' AND conrelid = 'public.phc_lookup_values_t'::regclass) THEN
        ALTER TABLE phc_lookup_values_t ADD CONSTRAINT phc_lookup_values_t_pkey PRIMARY KEY (plv_lookup_value_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_material_group_master_pkey' AND conrelid = 'public.phc_material_group_master_t'::regclass) THEN
        ALTER TABLE phc_material_group_master_t ADD CONSTRAINT phc_material_group_master_pkey PRIMARY KEY (pmgm_material_group_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_material_master_pkey' AND conrelid = 'public.phc_material_master_t'::regclass) THEN
        ALTER TABLE phc_material_master_t ADD CONSTRAINT phc_material_master_pkey PRIMARY KEY (pmat_material_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_module_t_pkey' AND conrelid = 'public.phc_module_t'::regclass) THEN
        ALTER TABLE phc_module_t ADD CONSTRAINT phc_module_t_pkey PRIMARY KEY (pmd_module_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_notifications_setup_t_pkey' AND conrelid = 'public.phc_notifications_setup_t'::regclass) THEN
        ALTER TABLE phc_notifications_setup_t ADD CONSTRAINT phc_notifications_setup_t_pkey PRIMARY KEY (pns_notification_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_orgs_t_pkey' AND conrelid = 'public.phc_orgs_t'::regclass) THEN
        ALTER TABLE phc_orgs_t ADD CONSTRAINT phc_orgs_t_pkey PRIMARY KEY (pos_org_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_partner_id' AND conrelid = 'public.phc_partners_t'::regclass) THEN
        ALTER TABLE phc_partners_t ADD CONSTRAINT pk_partner_id PRIMARY KEY (part_partner_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_plant_compliance_pkey' AND conrelid = 'public.phc_plant_compliance_t'::regclass) THEN
        ALTER TABLE phc_plant_compliance_t ADD CONSTRAINT phc_plant_compliance_pkey PRIMARY KEY (ppc_compliance_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_plant_equipment_pkey' AND conrelid = 'public.phc_plant_equipment_t'::regclass) THEN
        ALTER TABLE phc_plant_equipment_t ADD CONSTRAINT phc_plant_equipment_pkey PRIMARY KEY (ppm_equipment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_plant_master_pkey' AND conrelid = 'public.phc_plant_master_t'::regclass) THEN
        ALTER TABLE phc_plant_master_t ADD CONSTRAINT phc_plant_master_pkey PRIMARY KEY (ppm_plant_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_alt_names_pkey' AND conrelid = 'public.phc_prod_alt_names_t'::regclass) THEN
        ALTER TABLE phc_prod_alt_names_t ADD CONSTRAINT phc_prod_alt_names_pkey PRIMARY KEY (pan_alt_name_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_details_t_pkey' AND conrelid = 'public.phc_prod_attributes_template_details_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_details_t ADD CONSTRAINT phc_prod_attributes_template_details_t_pkey PRIMARY KEY (ppatd_attributes_detail_temp_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_master_t_pkey' AND conrelid = 'public.phc_prod_attributes_template_master_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_master_t ADD CONSTRAINT phc_prod_attributes_template_master_t_pkey PRIMARY KEY (ppatm_attributes_template_master_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_lifecycle_history_pkey' AND conrelid = 'public.phc_prod_lifecycle_history_t'::regclass) THEN
        ALTER TABLE phc_prod_lifecycle_history_t ADD CONSTRAINT phc_prod_lifecycle_history_pkey PRIMARY KEY (plh_lifecycle_hist_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_attributes_t_pkey' AND conrelid = 'public.phc_product_attributes_t'::regclass) THEN
        ALTER TABLE phc_product_attributes_t ADD CONSTRAINT phc_product_attributes_t_pkey PRIMARY KEY (ppatr_attributes_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_category_t_pkey' AND conrelid = 'public.phc_product_category_t'::regclass) THEN
        ALTER TABLE phc_product_category_t ADD CONSTRAINT phc_product_category_t_pkey PRIMARY KEY (ppc_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_group_t_pkey' AND conrelid = 'public.phc_product_group_t'::regclass) THEN
        ALTER TABLE phc_product_group_t ADD CONSTRAINT phc_product_group_t_pkey PRIMARY KEY (phc_product_group_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_t_pkey' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_t_pkey PRIMARY KEY (ppsku_sku_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sub_category_t_pkey' AND conrelid = 'public.phc_product_sub_category_t'::regclass) THEN
        ALTER TABLE phc_product_sub_category_t ADD CONSTRAINT phc_product_sub_category_t_pkey PRIMARY KEY (ppsc_sub_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_type_t_pkey' AND conrelid = 'public.phc_product_type_t'::regclass) THEN
        ALTER TABLE phc_product_type_t ADD CONSTRAINT phc_product_type_t_pkey PRIMARY KEY (ppt_product_type_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_variant_t_pkey' AND conrelid = 'public.phc_product_variant_t'::regclass) THEN
        ALTER TABLE phc_product_variant_t ADD CONSTRAINT phc_product_variant_t_pkey PRIMARY KEY (ppv_variant_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_master_pkey' AND conrelid = 'public.phc_products_master_t'::regclass) THEN
        ALTER TABLE phc_products_master_t ADD CONSTRAINT phc_prod_master_pkey PRIMARY KEY (pma_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_role_screen_assignment_t_pkey' AND conrelid = 'public.phc_role_screen_assignment_t'::regclass) THEN
        ALTER TABLE phc_role_screen_assignment_t ADD CONSTRAINT phc_role_screen_assignment_t_pkey PRIMARY KEY (prs_role_screen_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_roles_t_pkey' AND conrelid = 'public.phc_roles_t'::regclass) THEN
        ALTER TABLE phc_roles_t ADD CONSTRAINT phc_roles_t_pkey PRIMARY KEY (prl_role_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_rooms_t_pkey' AND conrelid = 'public.phc_rooms_t'::regclass) THEN
        ALTER TABLE phc_rooms_t ADD CONSTRAINT phc_rooms_t_pkey PRIMARY KEY (prm_room_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_screens_t_pkey' AND conrelid = 'public.phc_screens_t'::regclass) THEN
        ALTER TABLE phc_screens_t ADD CONSTRAINT phc_screens_t_pkey PRIMARY KEY (psn_screen_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_services_t_pkey' AND conrelid = 'public.phc_services_t'::regclass) THEN
        ALTER TABLE phc_services_t ADD CONSTRAINT phc_services_t_pkey PRIMARY KEY (pss_services_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_state_t_pkey' AND conrelid = 'public.phc_states_t'::regclass) THEN
        ALTER TABLE phc_states_t ADD CONSTRAINT phc_state_t_pkey PRIMARY KEY (pst_state_iso3_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pslm_storage_location_pk' AND conrelid = 'public.phc_storage_location_master_t'::regclass) THEN
        ALTER TABLE phc_storage_location_master_t ADD CONSTRAINT pslm_storage_location_pk PRIMARY KEY (pslm_storage_location_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_storage_locator_t_pkey' AND conrelid = 'public.phc_storage_locator_t'::regclass) THEN
        ALTER TABLE phc_storage_locator_t ADD CONSTRAINT phc_storage_locator_t_pkey PRIMARY KEY (psl_locator_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_uom_conversion_pkey' AND conrelid = 'public.phc_uom_conversion_t'::regclass) THEN
        ALTER TABLE phc_uom_conversion_t ADD CONSTRAINT phc_uom_conversion_pkey PRIMARY KEY (puc_conversion_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_uom_master_pkey' AND conrelid = 'public.phc_uom_master_t'::regclass) THEN
        ALTER TABLE phc_uom_master_t ADD CONSTRAINT phc_uom_master_pkey PRIMARY KEY (pum_uom_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_user_app_roles_assignment_t_pkey' AND conrelid = 'public.phc_user_app_roles_assignment_t'::regclass) THEN
        ALTER TABLE phc_user_app_roles_assignment_t ADD CONSTRAINT phc_user_app_roles_assignment_t_pkey PRIMARY KEY (pua_user_app_role_assig_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_user_group_t_pkey' AND conrelid = 'public.phc_user_group_t'::regclass) THEN
        ALTER TABLE phc_user_group_t ADD CONSTRAINT phc_user_group_t_pkey PRIMARY KEY (pug_ug_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_user_log_t_pkey' AND conrelid = 'public.phc_user_log_t'::regclass) THEN
        ALTER TABLE phc_user_log_t ADD CONSTRAINT phc_user_log_t_pkey PRIMARY KEY (pul_log_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_user_notifications_t_pkey' AND conrelid = 'public.phc_user_notifications_t'::regclass) THEN
        ALTER TABLE phc_user_notifications_t ADD CONSTRAINT phc_user_notifications_t_pkey PRIMARY KEY (pun_notification_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_user_roles_assignment_t_pkey' AND conrelid = 'public.phc_user_roles_assignment_t'::regclass) THEN
        ALTER TABLE phc_user_roles_assignment_t ADD CONSTRAINT phc_user_roles_assignment_t_pkey PRIMARY KEY (pua_user_role_assig_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_users_t_pkey' AND conrelid = 'public.phc_users_t'::regclass) THEN
        ALTER TABLE phc_users_t ADD CONSTRAINT phc_users_t_pkey PRIMARY KEY (pus_user_id);
    END IF;
END $$;

-- ===================== UNIQUE CONSTRAINTS =====================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cleaning_process_records_t_pcv_cpr_no_key' AND conrelid = 'public.pcv_cleaning_process_records_t'::regclass) THEN
        ALTER TABLE pcv_cleaning_process_records_t ADD CONSTRAINT pcv_cleaning_process_records_t_pcv_cpr_no_key UNIQUE (pcv_cpr_no);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'idx_pcv_prod_block_equip' AND conrelid = 'public.pcv_product_equipment_mapping_t'::regclass) THEN
        ALTER TABLE pcv_product_equipment_mapping_t ADD CONSTRAINT idx_pcv_prod_block_equip UNIQUE (pcv_product_id, pcv_block_no, pcv_module_no, pcv_equipment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'idx_pcv_prod_stage_code' AND conrelid = 'public.pcv_product_stages_t'::regclass) THEN
        ALTER TABLE pcv_product_stages_t ADD CONSTRAINT idx_pcv_prod_stage_code UNIQUE (pcv_product_id, pcv_stage_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_products_t_pcv_product_code_key' AND conrelid = 'public.pcv_production_products_t'::regclass) THEN
        ALTER TABLE pcv_production_products_t ADD CONSTRAINT pcv_products_t_pcv_product_code_key UNIQUE (pcv_product_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_executions_t_pcv_protocol_no_key' AND conrelid = 'public.pcv_validation_executions_t'::regclass) THEN
        ALTER TABLE pcv_validation_executions_t ADD CONSTRAINT pcv_validation_executions_t_pcv_protocol_no_key UNIQUE (pcv_protocol_no);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_reports_t_pcv_report_no_key' AND conrelid = 'public.pcv_validation_reports_t'::regclass) THEN
        ALTER TABLE pcv_validation_reports_t ADD CONSTRAINT pcv_validation_reports_t_pcv_report_no_key UNIQUE (pcv_report_no);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_setup_uk' AND conrelid = 'public.phc_approval_setup_t'::regclass) THEN
        ALTER TABLE phc_approval_setup_t ADD CONSTRAINT phc_approval_setup_uk UNIQUE (pas_approval_type, pas_appr_seq);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_building_floor_t_pfl_floor_number_key' AND conrelid = 'public.phc_building_floor_t'::regclass) THEN
        ALTER TABLE phc_building_floor_t ADD CONSTRAINT phc_building_floor_t_pfl_floor_number_key UNIQUE (pfl_floor_number);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_building_t_plc_building_number_key' AND conrelid = 'public.phc_building_t'::regclass) THEN
        ALTER TABLE phc_building_t ADD CONSTRAINT phc_building_t_plc_building_number_key UNIQUE (plc_building_number);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_countries_t_pct_country_iso2_key' AND conrelid = 'public.phc_countries_t'::regclass) THEN
        ALTER TABLE phc_countries_t ADD CONSTRAINT phc_countries_t_pct_country_iso2_key UNIQUE (pct_country_iso2);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_countries_t_pct_country_iso3_key' AND conrelid = 'public.phc_countries_t'::regclass) THEN
        ALTER TABLE phc_countries_t ADD CONSTRAINT phc_countries_t_pct_country_iso3_key UNIQUE (pct_country_iso3);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_document_number_range_setup_t_pnrm_company_id_key' AND conrelid = 'public.phc_document_number_range_setup_t'::regclass) THEN
        ALTER TABLE phc_document_number_range_setup_t ADD CONSTRAINT phc_document_number_range_setup_t_pnrm_company_id_key UNIQUE (pnrm_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phr_emp_assignments_t_pea_company_id_pea_assignment_id_key' AND conrelid = 'public.phc_emp_assignments_t'::regclass) THEN
        ALTER TABLE phc_emp_assignments_t ADD CONSTRAINT phr_emp_assignments_t_pea_company_id_pea_assignment_id_key UNIQUE (pea_company_id, pea_assignment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_emp_company_empno_uk' AND conrelid = 'public.phc_emp_t'::regclass) THEN
        ALTER TABLE phc_emp_t ADD CONSTRAINT phc_emp_company_empno_uk UNIQUE (pem_company_id, pem_emp_number);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_industries_t_ppi_industry_code_key' AND conrelid = 'public.phc_industries_t'::regclass) THEN
        ALTER TABLE phc_industries_t ADD CONSTRAINT phc_industries_t_ppi_industry_code_key UNIQUE (ppi_industry_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_cat_uk1' AND conrelid = 'public.phc_item_category_t'::regclass) THEN
        ALTER TABLE phc_item_category_t ADD CONSTRAINT phc_item_cat_uk1 UNIQUE (pic_company_id, pic_item_category_id, pic_item_category_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_category_t_pic_category_code_key' AND conrelid = 'public.phc_item_category_t'::regclass) THEN
        ALTER TABLE phc_item_category_t ADD CONSTRAINT phc_item_category_t_pic_category_code_key UNIQUE (pic_item_category_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_t_pif_item_family_code_key' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_t_pif_item_family_code_key UNIQUE (pif_item_family_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_uk1' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_uk1 UNIQUE (pif_company_id, pif_item_type_id, pif_item_family_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_master_uq1' AND conrelid = 'public.phc_item_master_t'::regclass) THEN
        ALTER TABLE phc_item_master_t ADD CONSTRAINT phc_item_master_uq1 UNIQUE (pim_company_id, pim_item_number);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_sub_cat_uk1' AND conrelid = 'public.phc_item_sub_category_t'::regclass) THEN
        ALTER TABLE phc_item_sub_category_t ADD CONSTRAINT phc_item_sub_cat_uk1 UNIQUE (pisc_company_id, pisc_item_category_id, pisc_item_sub_category_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_type_t_pit_item_type_code_key' AND conrelid = 'public.phc_item_type_t'::regclass) THEN
        ALTER TABLE phc_item_type_t ADD CONSTRAINT phc_item_type_t_pit_item_type_code_key UNIQUE (pit_item_type_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_type_uk1' AND conrelid = 'public.phc_item_type_t'::regclass) THEN
        ALTER TABLE phc_item_type_t ADD CONSTRAINT phc_item_type_uk1 UNIQUE (pit_company_id, pit_item_sub_category_id, pit_item_type_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_material_group_master_pmgm_group_code_key' AND conrelid = 'public.phc_material_group_master_t'::regclass) THEN
        ALTER TABLE phc_material_group_master_t ADD CONSTRAINT phc_material_group_master_pmgm_group_code_key UNIQUE (pmgm_group_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_material_master_pmat_material_code_key' AND conrelid = 'public.phc_material_master_t'::regclass) THEN
        ALTER TABLE phc_material_master_t ADD CONSTRAINT phc_material_master_pmat_material_code_key UNIQUE (pmat_material_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_plant_master_ppm_plant_code_key' AND conrelid = 'public.phc_plant_master_t'::regclass) THEN
        ALTER TABLE phc_plant_master_t ADD CONSTRAINT phc_plant_master_ppm_plant_code_key UNIQUE (ppm_plant_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_details_uq1' AND conrelid = 'public.phc_prod_attributes_template_details_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_details_t ADD CONSTRAINT phc_prod_attributes_template_details_uq1 UNIQUE (ppatd_attributes_template_master_id, ppatd_attributes_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_master_uq1' AND conrelid = 'public.phc_prod_attributes_template_master_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_master_t ADD CONSTRAINT phc_prod_attributes_template_master_uq1 UNIQUE (ppatm_industry_id, ppatm_attributes_master_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_attributes_uk1' AND conrelid = 'public.phc_product_attributes_t'::regclass) THEN
        ALTER TABLE phc_product_attributes_t ADD CONSTRAINT phc_product_attributes_uk1 UNIQUE (ppatr_product_id, ppatr_attributes_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_category_t_ppc_category_code_key' AND conrelid = 'public.phc_product_category_t'::regclass) THEN
        ALTER TABLE phc_product_category_t ADD CONSTRAINT phc_product_category_t_ppc_category_code_key UNIQUE (ppc_category_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_group_t_phc_product_group_code_key' AND conrelid = 'public.phc_product_group_t'::regclass) THEN
        ALTER TABLE phc_product_group_t ADD CONSTRAINT phc_product_group_t_phc_product_group_code_key UNIQUE (phc_product_group_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_group_uk' AND conrelid = 'public.phc_product_group_t'::regclass) THEN
        ALTER TABLE phc_product_group_t ADD CONSTRAINT phc_product_group_uk UNIQUE (phc_product_type_id, phc_product_group_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_t_ppsku_barcode_key' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_t_ppsku_barcode_key UNIQUE (ppsku_barcode);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_uk' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_uk UNIQUE (ppsku_company_id, ppsku_sku_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sub_cat_uk' AND conrelid = 'public.phc_product_sub_category_t'::regclass) THEN
        ALTER TABLE phc_product_sub_category_t ADD CONSTRAINT phc_product_sub_cat_uk UNIQUE (ppsc_category_id, ppsc_sub_category_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sub_category_t_ppsc_sub_category_code_key' AND conrelid = 'public.phc_product_sub_category_t'::regclass) THEN
        ALTER TABLE phc_product_sub_category_t ADD CONSTRAINT phc_product_sub_category_t_ppsc_sub_category_code_key UNIQUE (ppsc_sub_category_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_type_t_ppt_product_type_code_key' AND conrelid = 'public.phc_product_type_t'::regclass) THEN
        ALTER TABLE phc_product_type_t ADD CONSTRAINT phc_product_type_t_ppt_product_type_code_key UNIQUE (ppt_product_type_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_type_uk' AND conrelid = 'public.phc_product_type_t'::regclass) THEN
        ALTER TABLE phc_product_type_t ADD CONSTRAINT phc_product_type_uk UNIQUE (ppt_sub_category_id, ppt_product_type_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_variant_t_ppv_variant_code_key' AND conrelid = 'public.phc_product_variant_t'::regclass) THEN
        ALTER TABLE phc_product_variant_t ADD CONSTRAINT phc_product_variant_t_ppv_variant_code_key UNIQUE (ppv_variant_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_variant_uk' AND conrelid = 'public.phc_product_variant_t'::regclass) THEN
        ALTER TABLE phc_product_variant_t ADD CONSTRAINT phc_product_variant_uk UNIQUE (ppv_product_id, ppv_variant_name);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_master_pma_global_product_code_key' AND conrelid = 'public.phc_products_master_t'::regclass) THEN
        ALTER TABLE phc_products_master_t ADD CONSTRAINT phc_prod_master_pma_global_product_code_key UNIQUE (pma_global_product_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_rooms_t_prm_room_number_key' AND conrelid = 'public.phc_rooms_t'::regclass) THEN
        ALTER TABLE phc_rooms_t ADD CONSTRAINT phc_rooms_t_prm_room_number_key UNIQUE (prm_room_number);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pslm_storage_location_code_uk' AND conrelid = 'public.phc_storage_location_master_t'::regclass) THEN
        ALTER TABLE phc_storage_location_master_t ADD CONSTRAINT pslm_storage_location_code_uk UNIQUE (pslm_storage_location_code, pslm_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_storage_locator_t_psl_bin_barcode_key' AND conrelid = 'public.phc_storage_locator_t'::regclass) THEN
        ALTER TABLE phc_storage_locator_t ADD CONSTRAINT phc_storage_locator_t_psl_bin_barcode_key UNIQUE (psl_bin_barcode);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_storage_locator_t_psl_locator_code_key' AND conrelid = 'public.phc_storage_locator_t'::regclass) THEN
        ALTER TABLE phc_storage_locator_t ADD CONSTRAINT phc_storage_locator_t_psl_locator_code_key UNIQUE (psl_locator_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'psl_locator_coordinate_uk' AND conrelid = 'public.phc_storage_locator_t'::regclass) THEN
        ALTER TABLE phc_storage_locator_t ADD CONSTRAINT psl_locator_coordinate_uk UNIQUE (psl_room_id, psl_aisle_code, psl_rack_code, psl_shelf_level, psl_bin_position);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_uom_master_pum_uom_code_key' AND conrelid = 'public.phc_uom_master_t'::regclass) THEN
        ALTER TABLE phc_uom_master_t ADD CONSTRAINT phc_uom_master_pum_uom_code_key UNIQUE (pum_uom_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pus_user-name_uk' AND conrelid = 'public.phc_users_t'::regclass) THEN
        ALTER TABLE phc_users_t ADD CONSTRAINT "pus_user-name_uk" UNIQUE (pus_company_id, pus_user_name);
    END IF;
END $$;

-- ===================== CHECK CONSTRAINTS =====================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cleaning_process_records_t_pcv_status_check' AND conrelid = 'public.pcv_cleaning_process_records_t'::regclass) THEN
        ALTER TABLE pcv_cleaning_process_records_t ADD CONSTRAINT pcv_cleaning_process_records_t_pcv_status_check CHECK (((pcv_status)::text = ANY ((ARRAY['In-Progress'::character varying, 'Completed'::character varying, 'Approved'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cpr_execution_steps_t_pcv_observation_type_check' AND conrelid = 'public.pcv_cpr_execution_steps_t'::regclass) THEN
        ALTER TABLE pcv_cpr_execution_steps_t ADD CONSTRAINT pcv_cpr_execution_steps_t_pcv_observation_type_check CHECK (((pcv_observation_type)::text = ANY ((ARRAY['Yes/No'::character varying, 'Done'::character varying, 'Text_Entry'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_clearance_items_t_pcv_inspected_by_pd_check' AND conrelid = 'public.pcv_equipment_clearance_items_t'::regclass) THEN
        ALTER TABLE pcv_equipment_clearance_items_t ADD CONSTRAINT pcv_equipment_clearance_items_t_pcv_inspected_by_pd_check CHECK (((pcv_inspected_by_pd)::text = ANY ((ARRAY['YES'::character varying, 'NO'::character varying, 'Satisfactory'::character varying, 'Not satisfactory'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_clearance_items_t_pcv_inspected_by_qa_check' AND conrelid = 'public.pcv_equipment_clearance_items_t'::regclass) THEN
        ALTER TABLE pcv_equipment_clearance_items_t ADD CONSTRAINT pcv_equipment_clearance_items_t_pcv_inspected_by_qa_check CHECK (((pcv_inspected_by_qa)::text = ANY ((ARRAY['YES'::character varying, 'NO'::character varying, 'Satisfactory'::character varying, 'Not satisfactory'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_sampling_locations_t_pcv_sample_type_check' AND conrelid = 'public.pcv_equipment_sampling_locations_t'::regclass) THEN
        ALTER TABLE pcv_equipment_sampling_locations_t ADD CONSTRAINT pcv_equipment_sampling_locations_t_pcv_sample_type_check CHECK (((pcv_sample_type)::text = ANY ((ARRAY['Swab'::character varying, 'Rinse'::character varying, 'Swab/Rinse'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_sampling_locations_t_pcv_test_for_check' AND conrelid = 'public.pcv_equipment_sampling_locations_t'::regclass) THEN
        ALTER TABLE pcv_equipment_sampling_locations_t ADD CONSTRAINT pcv_equipment_sampling_locations_t_pcv_test_for_check CHECK (((pcv_test_for)::text = ANY ((ARRAY['Chemical'::character varying, 'Micro'::character varying, 'Detergent'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_products_t_pcv_dosage_form_check' AND conrelid = 'public.pcv_production_products_t'::regclass) THEN
        ALTER TABLE pcv_production_products_t ADD CONSTRAINT pcv_products_t_pcv_dosage_form_check CHECK (((pcv_dosage_form)::text = ANY ((ARRAY['Tablets'::character varying, 'Capsules'::character varying, 'Liquids'::character varying, 'Sachets'::character varying, 'Injections'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_solubility_details_t_pcv_solubility_in_water_check' AND conrelid = 'public.pcv_solubility_details_t'::regclass) THEN
        ALTER TABLE pcv_solubility_details_t ADD CONSTRAINT pcv_solubility_details_t_pcv_solubility_in_water_check CHECK (((pcv_solubility_in_water)::text = ANY ((ARRAY['Very soluble'::character varying, 'Freely soluble'::character varying, 'Soluble'::character varying, 'Sparingly soluble'::character varying, 'Slightly soluble'::character varying, 'Very slightly soluble'::character varying, 'Practically insoluble'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_results_t_pcv_result_status_check' AND conrelid = 'public.pcv_test_results_t'::regclass) THEN
        ALTER TABLE pcv_test_results_t ADD CONSTRAINT pcv_test_results_t_pcv_result_status_check CHECK (((pcv_result_status)::text = ANY ((ARRAY['Pass'::character varying, 'Fail'::character varying, 'OOS'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_executions_t_pcv_current_status_check' AND conrelid = 'public.pcv_validation_executions_t'::regclass) THEN
        ALTER TABLE pcv_validation_executions_t ADD CONSTRAINT pcv_validation_executions_t_pcv_current_status_check CHECK (((pcv_current_status)::text = ANY ((ARRAY['Training'::character varying, 'CPR'::character varying, 'Equipment Clearance'::character varying, 'TRF'::character varying, 'Sampling'::character varying, 'Results Entry'::character varying, 'Equipment Release'::character varying, 'Completed'::character varying])::text[])));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_module_t_pmd_status_check' AND conrelid = 'public.phc_module_t'::regclass) THEN
        ALTER TABLE phc_module_t ADD CONSTRAINT phc_module_t_pmd_status_check CHECK (((pmd_status)::text = ANY ((ARRAY['ACT'::character varying, 'INA'::character varying])::text[])));
    END IF;
END $$;

-- ===================== INDEXES (before FKs: unique indexes may back FK targets) =====================
CREATE INDEX IF NOT EXISTS idx_audit_tbl_rec ON public.phc_audit_log_t USING btree (pal_table_name, pal_record_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON public.phc_audit_log_t USING btree (pal_timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS phc_document_number_range_setup_t_pkey1 ON public.phc_document_number_range_setup_t USING btree (pnrm_number_range_id);
CREATE UNIQUE INDEX IF NOT EXISTS phc_document_number_range_setup_uk1 ON public.phc_document_number_range_setup_t USING btree (pnrm_doc_module_code, pnrm_doc_object_code);
CREATE INDEX IF NOT EXISTS idx_phc_lookup_active ON public.phc_lookup_values_t USING btree (plv_lookup_type_code, plv_status);
CREATE INDEX IF NOT EXISTS idx_phc_role_screen_role ON public.phc_role_screen_assignment_t USING btree (prs_role_id);
CREATE INDEX IF NOT EXISTS idx_phc_role_screens_auth ON public.phc_role_screen_assignment_t USING btree (prs_role_id, prs_screen_id);
CREATE INDEX IF NOT EXISTS idx_phc_screens_mod_status ON public.phc_screens_t USING btree (psn_module_id, psn_status);
CREATE INDEX IF NOT EXISTS idx_pun_recipient ON public.phc_user_notifications_t USING btree (pun_recipient_user_id, pun_recipient_role, pun_is_read);
CREATE INDEX IF NOT EXISTS idx_phc_role_assignment_user ON public.phc_user_roles_assignment_t USING btree (pua_user_id);
CREATE INDEX IF NOT EXISTS idx_phc_user_roles_auth ON public.phc_user_roles_assignment_t USING btree (pua_user_id, pua_role_id);
CREATE INDEX IF NOT EXISTS idx_phc_users_session ON public.phc_users_t USING btree (pus_session_id);
CREATE INDEX IF NOT EXISTS idx_phc_users_username ON public.phc_users_t USING btree (pus_user_name);

-- ===================== FOREIGN KEYS =====================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cleaning_process_records_t_pcv_equipment_id_fkey' AND conrelid = 'public.pcv_cleaning_process_records_t'::regclass) THEN
        ALTER TABLE pcv_cleaning_process_records_t ADD CONSTRAINT pcv_cleaning_process_records_t_pcv_equipment_id_fkey FOREIGN KEY (pcv_equipment_id) REFERENCES pcv_equipments_t(pcv_equipment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cleaning_process_records_t_pcv_execution_id_fkey' AND conrelid = 'public.pcv_cleaning_process_records_t'::regclass) THEN
        ALTER TABLE pcv_cleaning_process_records_t ADD CONSTRAINT pcv_cleaning_process_records_t_pcv_execution_id_fkey FOREIGN KEY (pcv_execution_id) REFERENCES pcv_validation_executions_t(pcv_execution_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_cpr_execution_steps_t_pcv_cpr_id_fkey' AND conrelid = 'public.pcv_cpr_execution_steps_t'::regclass) THEN
        ALTER TABLE pcv_cpr_execution_steps_t ADD CONSTRAINT pcv_cpr_execution_steps_t_pcv_cpr_id_fkey FOREIGN KEY (pcv_cpr_id) REFERENCES pcv_cleaning_process_records_t(pcv_cpr_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_clearance_checklists_t_pcv_cpr_id_fkey' AND conrelid = 'public.pcv_equipment_clearance_checklists_t'::regclass) THEN
        ALTER TABLE pcv_equipment_clearance_checklists_t ADD CONSTRAINT pcv_equipment_clearance_checklists_t_pcv_cpr_id_fkey FOREIGN KEY (pcv_cpr_id) REFERENCES pcv_cleaning_process_records_t(pcv_cpr_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_clearance_items_t_pcv_clearance_id_fkey' AND conrelid = 'public.pcv_equipment_clearance_items_t'::regclass) THEN
        ALTER TABLE pcv_equipment_clearance_items_t ADD CONSTRAINT pcv_equipment_clearance_items_t_pcv_clearance_id_fkey FOREIGN KEY (pcv_clearance_id) REFERENCES pcv_equipment_clearance_checklists_t(pcv_clearance_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_sampling_locations_t_pcv_equipment_id_fkey' AND conrelid = 'public.pcv_equipment_sampling_locations_t'::regclass) THEN
        ALTER TABLE pcv_equipment_sampling_locations_t ADD CONSTRAINT pcv_equipment_sampling_locations_t_pcv_equipment_id_fkey FOREIGN KEY (pcv_equipment_id) REFERENCES pcv_equipments_t(pcv_equipment_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipment_surface_areas_t_pcv_equipment_id_fkey' AND conrelid = 'public.pcv_equipment_surface_areas_t'::regclass) THEN
        ALTER TABLE pcv_equipment_surface_areas_t ADD CONSTRAINT pcv_equipment_surface_areas_t_pcv_equipment_id_fkey FOREIGN KEY (pcv_equipment_id) REFERENCES pcv_equipments_t(pcv_equipment_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_equipments_t_pcv_company_id_fkey' AND conrelid = 'public.pcv_equipments_t'::regclass) THEN
        ALTER TABLE pcv_equipments_t ADD CONSTRAINT pcv_equipments_t_pcv_company_id_fkey FOREIGN KEY (pcv_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_mdd_api_details_t_pcv_mdd_id_fkey' AND conrelid = 'public.pcv_mdd_api_details_t'::regclass) THEN
        ALTER TABLE pcv_mdd_api_details_t ADD CONSTRAINT pcv_mdd_api_details_t_pcv_mdd_id_fkey FOREIGN KEY (pcv_mdd_id) REFERENCES pcv_mdd_registrations_t(pcv_mdd_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_mdd_registrations_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_mdd_registrations_t'::regclass) THEN
        ALTER TABLE pcv_mdd_registrations_t ADD CONSTRAINT pcv_mdd_registrations_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_pde_api_details_t_pcv_pde_id_fkey' AND conrelid = 'public.pcv_pde_api_details_t'::regclass) THEN
        ALTER TABLE pcv_pde_api_details_t ADD CONSTRAINT pcv_pde_api_details_t_pcv_pde_id_fkey FOREIGN KEY (pcv_pde_id) REFERENCES pcv_pde_registrations_t(pcv_pde_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_pde_registrations_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_pde_registrations_t'::regclass) THEN
        ALTER TABLE pcv_pde_registrations_t ADD CONSTRAINT pcv_pde_registrations_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_batch_sizes_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_product_batch_sizes_t'::regclass) THEN
        ALTER TABLE pcv_product_batch_sizes_t ADD CONSTRAINT pcv_product_batch_sizes_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_equipment_mapping_t_pcv_equipment_id_fkey' AND conrelid = 'public.pcv_product_equipment_mapping_t'::regclass) THEN
        ALTER TABLE pcv_product_equipment_mapping_t ADD CONSTRAINT pcv_product_equipment_mapping_t_pcv_equipment_id_fkey FOREIGN KEY (pcv_equipment_id) REFERENCES pcv_equipments_t(pcv_equipment_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_equipment_mapping_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_product_equipment_mapping_t'::regclass) THEN
        ALTER TABLE pcv_product_equipment_mapping_t ADD CONSTRAINT pcv_product_equipment_mapping_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_pack_styles_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_product_pack_styles_t'::regclass) THEN
        ALTER TABLE pcv_product_pack_styles_t ADD CONSTRAINT pcv_product_pack_styles_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_stages_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_product_stages_t'::regclass) THEN
        ALTER TABLE pcv_product_stages_t ADD CONSTRAINT pcv_product_stages_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_product_strengths_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_product_strengths_t'::regclass) THEN
        ALTER TABLE pcv_product_strengths_t ADD CONSTRAINT pcv_product_strengths_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_products_t_pcv_company_id_fkey' AND conrelid = 'public.pcv_production_products_t'::regclass) THEN
        ALTER TABLE pcv_production_products_t ADD CONSTRAINT pcv_products_t_pcv_company_id_fkey FOREIGN KEY (pcv_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_sampling_records_t_pcv_trf_id_fkey' AND conrelid = 'public.pcv_sampling_records_t'::regclass) THEN
        ALTER TABLE pcv_sampling_records_t ADD CONSTRAINT pcv_sampling_records_t_pcv_trf_id_fkey FOREIGN KEY (pcv_trf_id) REFERENCES pcv_test_request_forms_t(pcv_trf_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_solubility_details_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_solubility_details_t'::regclass) THEN
        ALTER TABLE pcv_solubility_details_t ADD CONSTRAINT pcv_solubility_details_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_methods_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_test_methods_t'::regclass) THEN
        ALTER TABLE pcv_test_methods_t ADD CONSTRAINT pcv_test_methods_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_request_forms_t_pcv_cpr_id_fkey' AND conrelid = 'public.pcv_test_request_forms_t'::regclass) THEN
        ALTER TABLE pcv_test_request_forms_t ADD CONSTRAINT pcv_test_request_forms_t_pcv_cpr_id_fkey FOREIGN KEY (pcv_cpr_id) REFERENCES pcv_cleaning_process_records_t(pcv_cpr_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_request_forms_t_pcv_equipment_id_fkey' AND conrelid = 'public.pcv_test_request_forms_t'::regclass) THEN
        ALTER TABLE pcv_test_request_forms_t ADD CONSTRAINT pcv_test_request_forms_t_pcv_equipment_id_fkey FOREIGN KEY (pcv_equipment_id) REFERENCES pcv_equipments_t(pcv_equipment_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_results_t_pcv_sampling_loc_id_fkey' AND conrelid = 'public.pcv_test_results_t'::regclass) THEN
        ALTER TABLE pcv_test_results_t ADD CONSTRAINT pcv_test_results_t_pcv_sampling_loc_id_fkey FOREIGN KEY (pcv_sampling_loc_id) REFERENCES pcv_equipment_sampling_locations_t(pcv_sampling_loc_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_test_results_t_pcv_sampling_record_id_fkey' AND conrelid = 'public.pcv_test_results_t'::regclass) THEN
        ALTER TABLE pcv_test_results_t ADD CONSTRAINT pcv_test_results_t_pcv_sampling_record_id_fkey FOREIGN KEY (pcv_sampling_record_id) REFERENCES pcv_sampling_records_t(pcv_sampling_record_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_training_attendees_t_pcv_emp_id_fkey' AND conrelid = 'public.pcv_training_attendees_t'::regclass) THEN
        ALTER TABLE pcv_training_attendees_t ADD CONSTRAINT pcv_training_attendees_t_pcv_emp_id_fkey FOREIGN KEY (pcv_emp_id) REFERENCES phc_emp_t(pem_emp_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_training_attendees_t_pcv_training_id_fkey' AND conrelid = 'public.pcv_training_attendees_t'::regclass) THEN
        ALTER TABLE pcv_training_attendees_t ADD CONSTRAINT pcv_training_attendees_t_pcv_training_id_fkey FOREIGN KEY (pcv_training_id) REFERENCES pcv_training_records_t(pcv_training_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_training_records_t_pcv_emp_id_fkey' AND conrelid = 'public.pcv_training_records_t'::regclass) THEN
        ALTER TABLE pcv_training_records_t ADD CONSTRAINT pcv_training_records_t_pcv_emp_id_fkey FOREIGN KEY (pcv_emp_id) REFERENCES phc_emp_t(pem_emp_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_training_records_t_pcv_execution_id_fkey' AND conrelid = 'public.pcv_training_records_t'::regclass) THEN
        ALTER TABLE pcv_training_records_t ADD CONSTRAINT pcv_training_records_t_pcv_execution_id_fkey FOREIGN KEY (pcv_execution_id) REFERENCES pcv_validation_executions_t(pcv_execution_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_executions_t_pcv_product_id_fkey' AND conrelid = 'public.pcv_validation_executions_t'::regclass) THEN
        ALTER TABLE pcv_validation_executions_t ADD CONSTRAINT pcv_validation_executions_t_pcv_product_id_fkey FOREIGN KEY (pcv_product_id) REFERENCES pcv_production_products_t(pcv_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pcv_validation_reports_t_pcv_execution_id_fkey' AND conrelid = 'public.pcv_validation_reports_t'::regclass) THEN
        ALTER TABLE pcv_validation_reports_t ADD CONSTRAINT pcv_validation_reports_t_pcv_execution_id_fkey FOREIGN KEY (pcv_execution_id) REFERENCES pcv_validation_executions_t(pcv_execution_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_events_t_pae_appr_setup_id_fkey' AND conrelid = 'public.phc_approval_events_t'::regclass) THEN
        ALTER TABLE phc_approval_events_t ADD CONSTRAINT phc_approval_events_t_pae_appr_setup_id_fkey FOREIGN KEY (pae_appr_setup_id) REFERENCES phc_approval_setup_t(pas_appr_setup_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_events_t_pae_notification_id_fkey' AND conrelid = 'public.phc_approval_events_t'::regclass) THEN
        ALTER TABLE phc_approval_events_t ADD CONSTRAINT phc_approval_events_t_pae_notification_id_fkey FOREIGN KEY (pae_notification_id) REFERENCES phc_notifications_setup_t(pns_notification_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_approval_setup_t_pas_approval_type_fkey' AND conrelid = 'public.phc_approval_setup_t'::regclass) THEN
        ALTER TABLE phc_approval_setup_t ADD CONSTRAINT phc_approval_setup_t_pas_approval_type_fkey FOREIGN KEY (pas_approval_type) REFERENCES phc_approval_types_t(pat_approval_type);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_apps_t_fk' AND conrelid = 'public.phc_apps_t'::regclass) THEN
        ALTER TABLE phc_apps_t ADD CONSTRAINT phc_apps_t_fk FOREIGN KEY (pap_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_building_floor_t_pfl_company_id_fkey' AND conrelid = 'public.phc_building_floor_t'::regclass) THEN
        ALTER TABLE phc_building_floor_t ADD CONSTRAINT phc_building_floor_t_pfl_company_id_fkey FOREIGN KEY (pfl_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_building_t_plc_company_id_fkey' AND conrelid = 'public.phc_building_t'::regclass) THEN
        ALTER TABLE phc_building_t ADD CONSTRAINT phc_building_t_plc_company_id_fkey FOREIGN KEY (plc_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_cities_t_pct_company_id_fkey' AND conrelid = 'public.phc_cities_t'::regclass) THEN
        ALTER TABLE phc_cities_t ADD CONSTRAINT phc_cities_t_pct_company_id_fkey FOREIGN KEY (pct_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_cities_t_pct_country_iso3_fkey' AND conrelid = 'public.phc_cities_t'::regclass) THEN
        ALTER TABLE phc_cities_t ADD CONSTRAINT phc_cities_t_pct_country_iso3_fkey FOREIGN KEY (pct_country_iso3) REFERENCES phc_countries_t(pct_country_iso3);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_cities_t_pct_state_iso3_code_fkey' AND conrelid = 'public.phc_cities_t'::regclass) THEN
        ALTER TABLE phc_cities_t ADD CONSTRAINT phc_cities_t_pct_state_iso3_code_fkey FOREIGN KEY (pct_state_iso3_code) REFERENCES phc_states_t(pst_state_iso3_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_cost_center_t_fk' AND conrelid = 'public.phc_cost_center_t'::regclass) THEN
        ALTER TABLE phc_cost_center_t ADD CONSTRAINT phc_cost_center_t_fk FOREIGN KEY (pcc_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pct_country_company_fk' AND conrelid = 'public.phc_countries_t'::regclass) THEN
        ALTER TABLE phc_countries_t ADD CONSTRAINT pct_country_company_fk FOREIGN KEY (pct_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_dept_t_fk' AND conrelid = 'public.phc_dept_t'::regclass) THEN
        ALTER TABLE phc_dept_t ADD CONSTRAINT phc_dept_t_fk FOREIGN KEY (pdp_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_document_number_range_setup_fk' AND conrelid = 'public.phc_document_number_range_setup_t'::regclass) THEN
        ALTER TABLE phc_document_number_range_setup_t ADD CONSTRAINT phc_document_number_range_setup_fk FOREIGN KEY (pnrm_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_emp_apps_grant_t_app_fk' AND conrelid = 'public.phc_emp_apps_grant_t'::regclass) THEN
        ALTER TABLE phc_emp_apps_grant_t ADD CONSTRAINT phc_emp_apps_grant_t_app_fk FOREIGN KEY (pea_apps_id) REFERENCES phc_apps_t(pap_apps_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_emp_apps_grant_t_comp_fk' AND conrelid = 'public.phc_emp_apps_grant_t'::regclass) THEN
        ALTER TABLE phc_emp_apps_grant_t ADD CONSTRAINT phc_emp_apps_grant_t_comp_fk FOREIGN KEY (pea_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phr_emp_assignments_t_pea_company_id_fkey' AND conrelid = 'public.phc_emp_assignments_t'::regclass) THEN
        ALTER TABLE phc_emp_assignments_t ADD CONSTRAINT phr_emp_assignments_t_pea_company_id_fkey FOREIGN KEY (pea_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phr_emp_assignments_t_pea_dept_id_fkey' AND conrelid = 'public.phc_emp_assignments_t'::regclass) THEN
        ALTER TABLE phc_emp_assignments_t ADD CONSTRAINT phr_emp_assignments_t_pea_dept_id_fkey FOREIGN KEY (pea_dept_id) REFERENCES phc_dept_t(pdp_dept_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phr_emp_assignments_t_pea_emp_id_fkey' AND conrelid = 'public.phc_emp_assignments_t'::regclass) THEN
        ALTER TABLE phc_emp_assignments_t ADD CONSTRAINT phr_emp_assignments_t_pea_emp_id_fkey FOREIGN KEY (pea_emp_id) REFERENCES phc_emp_t(pem_emp_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_emp_t_fk' AND conrelid = 'public.phc_emp_t'::regclass) THEN
        ALTER TABLE phc_emp_t ADD CONSTRAINT phc_emp_t_fk FOREIGN KEY (pem_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_industry_fk' AND conrelid = 'public.phc_industries_t'::regclass) THEN
        ALTER TABLE phc_industries_t ADD CONSTRAINT phc_industry_fk FOREIGN KEY (ppi_industry_id) REFERENCES phc_industries_t(ppi_industry_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_cat_compa_fk1' AND conrelid = 'public.phc_item_category_t'::regclass) THEN
        ALTER TABLE phc_item_category_t ADD CONSTRAINT phc_item_cat_compa_fk1 FOREIGN KEY (pic_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_cat_indus_fk2' AND conrelid = 'public.phc_item_category_t'::regclass) THEN
        ALTER TABLE phc_item_category_t ADD CONSTRAINT phc_item_cat_indus_fk2 FOREIGN KEY (pic_industry_id) REFERENCES phc_industries_t(ppi_industry_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_category_fk' AND conrelid = 'public.phc_item_category_t'::regclass) THEN
        ALTER TABLE phc_item_category_t ADD CONSTRAINT phc_item_category_fk FOREIGN KEY (pic_parent_item_category_id) REFERENCES phc_item_category_t(pic_item_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_cat_fk2' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_cat_fk2 FOREIGN KEY (pif_item_category_id) REFERENCES phc_item_category_t(pic_item_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_comp_fk1' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_comp_fk1 FOREIGN KEY (pif_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_fk' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_fk FOREIGN KEY (pif_item_type_id) REFERENCES phc_item_type_t(pit_item_type_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_family_sub_cat_fk3' AND conrelid = 'public.phc_item_family_t'::regclass) THEN
        ALTER TABLE phc_item_family_t ADD CONSTRAINT phc_item_family_sub_cat_fk3 FOREIGN KEY (pif_item_sub_category_id) REFERENCES phc_item_sub_category_t(pisc_item_sub_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_master_comp_fk1' AND conrelid = 'public.phc_item_master_t'::regclass) THEN
        ALTER TABLE phc_item_master_t ADD CONSTRAINT phc_item_master_comp_fk1 FOREIGN KEY (pim_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_master_family_fk4' AND conrelid = 'public.phc_item_master_t'::regclass) THEN
        ALTER TABLE phc_item_master_t ADD CONSTRAINT phc_item_master_family_fk4 FOREIGN KEY (pim_item_family_id) REFERENCES phc_item_family_t(pif_item_family_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_master_ind_fk3' AND conrelid = 'public.phc_item_master_t'::regclass) THEN
        ALTER TABLE phc_item_master_t ADD CONSTRAINT phc_item_master_ind_fk3 FOREIGN KEY (pim_industry_id) REFERENCES phc_industries_t(ppi_industry_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_master_org_fk2' AND conrelid = 'public.phc_item_master_t'::regclass) THEN
        ALTER TABLE phc_item_master_t ADD CONSTRAINT phc_item_master_org_fk2 FOREIGN KEY (pim_org_id) REFERENCES phc_orgs_t(pos_org_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_sub_cat_comp_fk1' AND conrelid = 'public.phc_item_sub_category_t'::regclass) THEN
        ALTER TABLE phc_item_sub_category_t ADD CONSTRAINT phc_item_sub_cat_comp_fk1 FOREIGN KEY (pisc_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_sub_cat_fk2' AND conrelid = 'public.phc_item_sub_category_t'::regclass) THEN
        ALTER TABLE phc_item_sub_category_t ADD CONSTRAINT phc_item_sub_cat_fk2 FOREIGN KEY (pisc_item_category_id) REFERENCES phc_item_category_t(pic_item_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_type_comp_fk1' AND conrelid = 'public.phc_item_type_t'::regclass) THEN
        ALTER TABLE phc_item_type_t ADD CONSTRAINT phc_item_type_comp_fk1 FOREIGN KEY (pit_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_item_type_sub_cat_fk2' AND conrelid = 'public.phc_item_type_t'::regclass) THEN
        ALTER TABLE phc_item_type_t ADD CONSTRAINT phc_item_type_sub_cat_fk2 FOREIGN KEY (pit_item_sub_category_id) REFERENCES phc_item_sub_category_t(pisc_item_sub_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_locations_t_fk' AND conrelid = 'public.phc_locations_t'::regclass) THEN
        ALTER TABLE phc_locations_t ADD CONSTRAINT phc_locations_t_fk FOREIGN KEY (plc_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_lookup_types_fk' AND conrelid = 'public.phc_lookup_types_t'::regclass) THEN
        ALTER TABLE phc_lookup_types_t ADD CONSTRAINT phc_lookup_types_fk FOREIGN KEY (plt_lookup_type_code_primary) REFERENCES phc_lookup_types_t(plt_lookup_type_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_lookup_values_t_con' AND conrelid = 'public.phc_lookup_values_t'::regclass) THEN
        ALTER TABLE phc_lookup_values_t ADD CONSTRAINT phc_lookup_values_t_con FOREIGN KEY (plv_lookup_type_code) REFERENCES phc_lookup_types_t(plt_lookup_type_code);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_material_group_master_pmgm_parent_group_id_fkey' AND conrelid = 'public.phc_material_group_master_t'::regclass) THEN
        ALTER TABLE phc_material_group_master_t ADD CONSTRAINT phc_material_group_master_pmgm_parent_group_id_fkey FOREIGN KEY (pmgm_parent_group_id) REFERENCES phc_material_group_master_t(pmgm_material_group_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_material_master_pmat_material_group_id_fkey' AND conrelid = 'public.phc_material_master_t'::regclass) THEN
        ALTER TABLE phc_material_master_t ADD CONSTRAINT phc_material_master_pmat_material_group_id_fkey FOREIGN KEY (pmat_material_group_id) REFERENCES phc_material_group_master_t(pmgm_material_group_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_orgs_t_fk' AND conrelid = 'public.phc_orgs_t'::regclass) THEN
        ALTER TABLE phc_orgs_t ADD CONSTRAINT phc_orgs_t_fk FOREIGN KEY (pos_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'part_company_id_fk' AND conrelid = 'public.phc_partners_t'::regclass) THEN
        ALTER TABLE phc_partners_t ADD CONSTRAINT part_company_id_fk FOREIGN KEY (part_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_plant_compliance_ppc_plant_id_fkey' AND conrelid = 'public.phc_plant_compliance_t'::regclass) THEN
        ALTER TABLE phc_plant_compliance_t ADD CONSTRAINT phc_plant_compliance_ppc_plant_id_fkey FOREIGN KEY (ppc_plant_id) REFERENCES phc_plant_master_t(ppm_plant_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_plant_equipment_ppm_plant_id_fkey' AND conrelid = 'public.phc_plant_equipment_t'::regclass) THEN
        ALTER TABLE phc_plant_equipment_t ADD CONSTRAINT phc_plant_equipment_ppm_plant_id_fkey FOREIGN KEY (ppm_plant_id) REFERENCES phc_plant_master_t(ppm_plant_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_alt_names_pan_product_id_fkey' AND conrelid = 'public.phc_prod_alt_names_t'::regclass) THEN
        ALTER TABLE phc_prod_alt_names_t ADD CONSTRAINT phc_prod_alt_names_pan_product_id_fkey FOREIGN KEY (pan_product_id) REFERENCES phc_products_master_t(pma_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_details_fk1' AND conrelid = 'public.phc_prod_attributes_template_details_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_details_t ADD CONSTRAINT phc_prod_attributes_template_details_fk1 FOREIGN KEY (ppatd_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_details_fk2' AND conrelid = 'public.phc_prod_attributes_template_details_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_details_t ADD CONSTRAINT phc_prod_attributes_template_details_fk2 FOREIGN KEY (ppatd_attributes_template_master_id) REFERENCES phc_prod_attributes_template_master_t(ppatm_attributes_template_master_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_master_fk1' AND conrelid = 'public.phc_prod_attributes_template_master_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_master_t ADD CONSTRAINT phc_prod_attributes_template_master_fk1 FOREIGN KEY (ppatm_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_attributes_template_master_fk2' AND conrelid = 'public.phc_prod_attributes_template_master_t'::regclass) THEN
        ALTER TABLE phc_prod_attributes_template_master_t ADD CONSTRAINT phc_prod_attributes_template_master_fk2 FOREIGN KEY (ppatm_industry_id) REFERENCES phc_industries_t(ppi_industry_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_lifecycle_history_plh_product_id_fkey' AND conrelid = 'public.phc_prod_lifecycle_history_t'::regclass) THEN
        ALTER TABLE phc_prod_lifecycle_history_t ADD CONSTRAINT phc_prod_lifecycle_history_plh_product_id_fkey FOREIGN KEY (plh_product_id) REFERENCES phc_products_master_t(pma_product_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_attributes_fk1' AND conrelid = 'public.phc_product_attributes_t'::regclass) THEN
        ALTER TABLE phc_product_attributes_t ADD CONSTRAINT phc_product_attributes_fk1 FOREIGN KEY (ppatr_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_attributes_fk2' AND conrelid = 'public.phc_product_attributes_t'::regclass) THEN
        ALTER TABLE phc_product_attributes_t ADD CONSTRAINT phc_product_attributes_fk2 FOREIGN KEY (ppatr_product_id) REFERENCES phc_products_master_t(pma_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_attributes_fk3' AND conrelid = 'public.phc_product_attributes_t'::regclass) THEN
        ALTER TABLE phc_product_attributes_t ADD CONSTRAINT phc_product_attributes_fk3 FOREIGN KEY (ppatr_attributes_detail_temp_id) REFERENCES phc_prod_attributes_template_details_t(ppatd_attributes_detail_temp_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_category_fk' AND conrelid = 'public.phc_product_category_t'::regclass) THEN
        ALTER TABLE phc_product_category_t ADD CONSTRAINT phc_product_category_fk FOREIGN KEY (ppc_parent_category_id) REFERENCES phc_product_category_t(ppc_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_group_fk' AND conrelid = 'public.phc_product_group_t'::regclass) THEN
        ALTER TABLE phc_product_group_t ADD CONSTRAINT phc_product_group_fk FOREIGN KEY (phc_product_type_id) REFERENCES phc_product_type_t(ppt_product_type_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_fk1' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_fk1 FOREIGN KEY (ppsku_product_id) REFERENCES phc_products_master_t(pma_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_fk2' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_fk2 FOREIGN KEY (ppsku_variant_id) REFERENCES phc_product_variant_t(ppv_variant_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_fk3' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_fk3 FOREIGN KEY (ppsku_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_fk4' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_fk4 FOREIGN KEY (ppsku_purchase_uom_id) REFERENCES phc_uom_master_t(pum_uom_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_fk5' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_fk5 FOREIGN KEY (ppsku_sales_uom_id) REFERENCES phc_uom_master_t(pum_uom_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sku_fk6' AND conrelid = 'public.phc_product_sku_t'::regclass) THEN
        ALTER TABLE phc_product_sku_t ADD CONSTRAINT phc_product_sku_fk6 FOREIGN KEY (ppsku_inventory_uom_id) REFERENCES phc_uom_master_t(pum_uom_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_sub_cat_fk' AND conrelid = 'public.phc_product_sub_category_t'::regclass) THEN
        ALTER TABLE phc_product_sub_category_t ADD CONSTRAINT phc_product_sub_cat_fk FOREIGN KEY (ppsc_category_id) REFERENCES phc_product_category_t(ppc_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_type_fk' AND conrelid = 'public.phc_product_type_t'::regclass) THEN
        ALTER TABLE phc_product_type_t ADD CONSTRAINT phc_product_type_fk FOREIGN KEY (ppt_sub_category_id) REFERENCES phc_product_sub_category_t(ppsc_sub_category_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_variant_comp_fk2' AND conrelid = 'public.phc_product_variant_t'::regclass) THEN
        ALTER TABLE phc_product_variant_t ADD CONSTRAINT phc_product_variant_comp_fk2 FOREIGN KEY (ppv_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_product_variant_fk' AND conrelid = 'public.phc_product_variant_t'::regclass) THEN
        ALTER TABLE phc_product_variant_t ADD CONSTRAINT phc_product_variant_fk FOREIGN KEY (ppv_product_id) REFERENCES phc_products_master_t(pma_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_master_attr_temp_fk2' AND conrelid = 'public.phc_products_master_t'::regclass) THEN
        ALTER TABLE phc_products_master_t ADD CONSTRAINT phc_prod_master_attr_temp_fk2 FOREIGN KEY (pma_attributes_template_master_id) REFERENCES phc_prod_attributes_template_master_t(ppatm_attributes_template_master_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_master_pma_parent_product_id_fkey' AND conrelid = 'public.phc_products_master_t'::regclass) THEN
        ALTER TABLE phc_products_master_t ADD CONSTRAINT phc_prod_master_pma_parent_product_id_fkey FOREIGN KEY (pma_parent_product_id) REFERENCES phc_products_master_t(pma_product_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_prod_master_prod_group_fk' AND conrelid = 'public.phc_products_master_t'::regclass) THEN
        ALTER TABLE phc_products_master_t ADD CONSTRAINT phc_prod_master_prod_group_fk FOREIGN KEY (pma_product_group_id) REFERENCES phc_product_group_t(phc_product_group_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_roles_t_fk' AND conrelid = 'public.phc_roles_t'::regclass) THEN
        ALTER TABLE phc_roles_t ADD CONSTRAINT phc_roles_t_fk FOREIGN KEY (prl_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_rooms_t_prm_building_id_fkey' AND conrelid = 'public.phc_rooms_t'::regclass) THEN
        ALTER TABLE phc_rooms_t ADD CONSTRAINT phc_rooms_t_prm_building_id_fkey FOREIGN KEY (prm_building_id) REFERENCES phc_building_t(plc_building_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_rooms_t_prm_company_id_fkey' AND conrelid = 'public.phc_rooms_t'::regclass) THEN
        ALTER TABLE phc_rooms_t ADD CONSTRAINT phc_rooms_t_prm_company_id_fkey FOREIGN KEY (prm_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_rooms_t_prm_floor_id_fkey' AND conrelid = 'public.phc_rooms_t'::regclass) THEN
        ALTER TABLE phc_rooms_t ADD CONSTRAINT phc_rooms_t_prm_floor_id_fkey FOREIGN KEY (prm_floor_id) REFERENCES phc_building_floor_t(pfl_floor_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_screens_t_psn_module_id_fkey' AND conrelid = 'public.phc_screens_t'::regclass) THEN
        ALTER TABLE phc_screens_t ADD CONSTRAINT phc_screens_t_psn_module_id_fkey FOREIGN KEY (psn_module_id) REFERENCES phc_module_t(pmd_module_id) ON DELETE SET NULL;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_services_t_fk' AND conrelid = 'public.phc_services_t'::regclass) THEN
        ALTER TABLE phc_services_t ADD CONSTRAINT phc_services_t_fk FOREIGN KEY (pss_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_state_t_pst_company_id_fkey' AND conrelid = 'public.phc_states_t'::regclass) THEN
        ALTER TABLE phc_states_t ADD CONSTRAINT phc_state_t_pst_company_id_fkey FOREIGN KEY (pst_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_state_t_pst_country_iso3_fkey' AND conrelid = 'public.phc_states_t'::regclass) THEN
        ALTER TABLE phc_states_t ADD CONSTRAINT phc_state_t_pst_country_iso3_fkey FOREIGN KEY (pst_country_iso3) REFERENCES phc_countries_t(pct_country_iso3);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pslm_company_id_fk' AND conrelid = 'public.phc_storage_location_master_t'::regclass) THEN
        ALTER TABLE phc_storage_location_master_t ADD CONSTRAINT pslm_company_id_fk FOREIGN KEY (pslm_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_storage_locator_t_psl_company_id_fkey' AND conrelid = 'public.phc_storage_locator_t'::regclass) THEN
        ALTER TABLE phc_storage_locator_t ADD CONSTRAINT phc_storage_locator_t_psl_company_id_fkey FOREIGN KEY (psl_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_storage_locator_t_psl_room_id_fkey' AND conrelid = 'public.phc_storage_locator_t'::regclass) THEN
        ALTER TABLE phc_storage_locator_t ADD CONSTRAINT phc_storage_locator_t_psl_room_id_fkey FOREIGN KEY (psl_room_id) REFERENCES phc_rooms_t(prm_room_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_uom_conversion_puc_from_uom_id_fkey' AND conrelid = 'public.phc_uom_conversion_t'::regclass) THEN
        ALTER TABLE phc_uom_conversion_t ADD CONSTRAINT phc_uom_conversion_puc_from_uom_id_fkey FOREIGN KEY (puc_from_uom_id) REFERENCES phc_uom_master_t(pum_uom_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_uom_conversion_puc_to_uom_id_fkey' AND conrelid = 'public.phc_uom_conversion_t'::regclass) THEN
        ALTER TABLE phc_uom_conversion_t ADD CONSTRAINT phc_uom_conversion_puc_to_uom_id_fkey FOREIGN KEY (puc_to_uom_id) REFERENCES phc_uom_master_t(pum_uom_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pua_apps_id_fk' AND conrelid = 'public.phc_user_app_roles_assignment_t'::regclass) THEN
        ALTER TABLE phc_user_app_roles_assignment_t ADD CONSTRAINT pua_apps_id_fk FOREIGN KEY (pua_apps_id) REFERENCES phc_apps_t(pap_apps_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pua_company_id_fk' AND conrelid = 'public.phc_user_app_roles_assignment_t'::regclass) THEN
        ALTER TABLE phc_user_app_roles_assignment_t ADD CONSTRAINT pua_company_id_fk FOREIGN KEY (pua_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pua_role_id_fk' AND conrelid = 'public.phc_user_app_roles_assignment_t'::regclass) THEN
        ALTER TABLE phc_user_app_roles_assignment_t ADD CONSTRAINT pua_role_id_fk FOREIGN KEY (pua_role_id) REFERENCES phc_roles_t(prl_role_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pua_user_id_fk' AND conrelid = 'public.phc_user_app_roles_assignment_t'::regclass) THEN
        ALTER TABLE phc_user_app_roles_assignment_t ADD CONSTRAINT pua_user_id_fk FOREIGN KEY (pua_user_id) REFERENCES phc_users_t(pus_user_id) ON DELETE CASCADE;
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_users_t_comp_fk' AND conrelid = 'public.phc_users_t'::regclass) THEN
        ALTER TABLE phc_users_t ADD CONSTRAINT phc_users_t_comp_fk FOREIGN KEY (pus_company_id) REFERENCES phc_companies_t(pcp_company_id);
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'phc_users_t_emp_fk' AND conrelid = 'public.phc_users_t'::regclass) THEN
        ALTER TABLE phc_users_t ADD CONSTRAINT phc_users_t_emp_fk FOREIGN KEY (pus_emp_id) REFERENCES phc_emp_t(pem_emp_id);
    END IF;
END $$;

-- ===================== FUNCTIONS =====================
CREATE OR REPLACE FUNCTION public.phc_industries_trg_func()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_pnrm_current_number     BIGINT;
    v_pnrm_current_doc_number TEXT;
BEGIN
    /*
     * Get and lock the document number range row
     */
    SELECT
        pnrm_current_number,
        COALESCE(pnrm_prefix, '')
        || COALESCE(pnrm_separator1, '')
        || CASE
            WHEN COALESCE(pnrm_year_format, '') = 'YYYY'
                THEN TO_CHAR(CURRENT_DATE, 'YYYY')
            WHEN COALESCE(pnrm_year_format, '') = 'YYY'
                THEN TO_CHAR(CURRENT_DATE, 'YYY')
            WHEN COALESCE(pnrm_year_format, '') = 'YY'
                THEN TO_CHAR(CURRENT_DATE, 'YY')
            ELSE ''
        END
        || CASE
            WHEN COALESCE(pnrm_month_format, '') = 'MMM'
                THEN TO_CHAR(CURRENT_DATE, 'MON')
            WHEN COALESCE(pnrm_month_format, '') = 'MM'
                THEN TO_CHAR(CURRENT_DATE, 'MM')
            ELSE ''
        END
        || CASE
            WHEN COALESCE(pnrm_date_format, '') = 'DD'
                THEN TO_CHAR(CURRENT_DATE, 'DD')
            ELSE ''
        END
        || COALESCE(pnrm_separator2, '')
        || COALESCE(pnrm_suffix, '')
        || COALESCE(
            pnrm_current_number + 1,
            pnrm_start_number
        )::TEXT
    INTO STRICT
        v_pnrm_current_number,
        v_pnrm_current_doc_number
    FROM phc_document_number_range_setup_t
    WHERE pnrm_doc_module_code = 'phc_industries_t'
      AND pnrm_doc_object_code = 'ppi_industry_code'
    FOR UPDATE;

    /*
     * Assign generated document number
     */
    NEW.ppi_industry_code := v_pnrm_current_doc_number;

    /*
     * Update number range
     */
    UPDATE phc_document_number_range_setup_t
       SET pnrm_current_number = COALESCE(v_pnrm_current_number + 1,pnrm_start_number),
           pnrm_current_doc_number = v_pnrm_current_doc_number
     WHERE pnrm_doc_module_code = 'phc_industries_t'
       AND pnrm_doc_object_code = 'ppi_industry_code';

    /*
     * Return the new row
     */
     
    RETURN NEW;

EXCEPTION

    /*
     * No document number setup found
     */
    WHEN NO_DATA_FOUND 
    THEN RAISE EXCEPTION
            'Document number setup not found for Module: %, Object: %',
            'phc_industries_t',
            'ppi_industry_code';

    /*
     * More than one setup record found
     */
    WHEN TOO_MANY_ROWS THEN
        RAISE EXCEPTION
            'Multiple document number setups found for Module: %, Object: %',
            'phc_industries_t',
            'ppi_industry_code';
    /*
     * Any other unexpected error
     */
    WHEN OTHERS THEN
        RAISE EXCEPTION
            'Error generating Industry Code: %',
            SQLERRM;
END;
$function$;

CREATE OR REPLACE FUNCTION public.trg_auto_update_timestamp()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- Checks for psn_modified or any column ending in _modified
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = TG_TABLE_NAME AND column_name = 'psn_modified'
        ) THEN
            NEW.psn_modified = CURRENT_TIMESTAMP;
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

-- ===================== TRIGGERS =====================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'phc_industries_trg_ins' AND tgrelid = 'public.phc_industries_t'::regclass) THEN
        CREATE TRIGGER phc_industries_trg_ins BEFORE INSERT ON public.phc_industries_t FOR EACH ROW EXECUTE FUNCTION phc_industries_trg_func();
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_screens_modified' AND tgrelid = 'public.phc_screens_t'::regclass) THEN
        CREATE TRIGGER trg_screens_modified BEFORE UPDATE ON public.phc_screens_t FOR EACH ROW EXECUTE FUNCTION trg_auto_update_timestamp();
    END IF;
END $$;

-- ===================== VIEWS =====================
CREATE OR REPLACE VIEW v_user_authorized_screens AS
 SELECT DISTINCT ura.pua_user_id,
    s.psn_screen_code,
    s.psn_screen_name,
    COALESCE(m.pmd_module_name, 'General'::character varying) AS module_name,
    m.pmd_module_id
   FROM phc_screens_t s
     JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
     JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
     LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
  WHERE s.psn_status::text = 'ACT'::text OR s.psn_status IS NULL;

CREATE OR REPLACE VIEW vw_phc_screens_modules AS
 SELECT s.psn_screen_id,
    s.psn_screen_code,
    s.psn_screen_name,
    s.psn_status,
    COALESCE(m.pmd_module_name, 'Uncategorized'::character varying) AS module_name
   FROM phc_screens_t s
     LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
  WHERE s.psn_status::text = 'ACT'::text AND (m.pmd_status::text = 'ACT'::text OR m.pmd_status IS NULL);

