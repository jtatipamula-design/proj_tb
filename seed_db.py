"""
ERP Database Seeding Module and Module Classification Rules.
Includes Cleaning Validation (Module 10) table definitions and seeding logic.
"""

import os
import asyncio
import asyncpg

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

REQUIRED_MODULES = [
    (1, 'Core Architecture', 'ACT'),
    (2, 'Chart of Accounts', 'ACT'),
    (3, 'MasterData', 'ACT'),
    (4, 'HR', 'ACT'),
    (5, 'UserMgmt', 'ACT'),
    (6, 'Purchasing', 'ACT'),
    (7, 'SupplyChain', 'ACT'),
    (8, 'WorkflowSetup', 'ACT'),
    (9, 'CRM', 'ACT'),
    (10, 'Cleaning Validation', 'ACT'),
]

CLEANING_VALIDATION_SCREENS = {
    "pcv_products_t": "Products & Dosage Master",
    "pcv_product_strengths_t": "Product Strengths",
    "pcv_product_stages_t": "Product Manufacturing Stages",
    "pcv_product_pack_styles_t": "Product Pack Styles",
    "pcv_pde_registrations_t": "PDE Registrations",
    "pcv_pde_api_details_t": "PDE API Details",
    "pcv_solubility_details_t": "Solubility Details",
    "pcv_mdd_registrations_t": "Maximum Daily Dosage (MDD)",
    "pcv_mdd_api_details_t": "MDD API Details",
    "pcv_test_methods_t": "Analytical Test Methods",
    "pcv_product_batch_sizes_t": "Product Batch Sizes",
    "pcv_equipments_t": "Equipment Master",
    "pcv_equipment_surface_areas_t": "Equipment Surface Areas",
    "pcv_equipment_sampling_locations_t": "Equipment Sampling Points",
    "pcv_product_equipment_mapping_t": "Product Equipment Mapping",
    "pcv_validation_executions_t": "Validation Executions",
    "pcv_training_records_t": "Training Records",
    "pcv_training_attendees_t": "Training Attendees",
    "pcv_cleaning_process_records_t": "Cleaning Process Records",
    "pcv_cpr_execution_steps_t": "CPR Execution Steps",
    "pcv_equipment_clearance_checklists_t": "Equipment Clearance Checklists",
    "pcv_equipment_clearance_items_t": "Equipment Clearance Items",
    "pcv_test_request_forms_t": "Test Request Forms (TRF)",
    "pcv_sampling_records_t": "QA/QC Sampling Records",
    "pcv_test_results_t": "Analytical Test Results",
    "pcv_validation_reports_t": "Validation Reports"
}

CLEANING_VALIDATION_DDL = [
    """
    CREATE TABLE IF NOT EXISTS pcv_products_t (
        pcv_product_id BIGSERIAL PRIMARY KEY,
        pcv_product_code VARCHAR(50) UNIQUE NOT NULL,
        pcv_product_name VARCHAR(200) NOT NULL,
        pcv_dosage_form VARCHAR(50) NOT NULL CHECK (pcv_dosage_form IN ('Tablets', 'Capsules', 'Liquids', 'Sachets', 'Injections')),
        pcv_grade VARCHAR(50) DEFAULT 'USP',
        pcv_description TEXT,
        pcv_is_active BOOLEAN DEFAULT TRUE,
        pcv_company_id BIGINT NOT NULL DEFAULT 1 REFERENCES phc_companies_t(pcp_company_id),
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_product_strengths_t (
        pcv_strength_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_strength_value VARCHAR(100) NOT NULL,
        pcv_uom VARCHAR(20) NOT NULL,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_product_stages_t (
        pcv_stage_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_stage_name VARCHAR(100) NOT NULL,
        pcv_stage_code VARCHAR(30) NOT NULL,
        pcv_mfc_mfr_no VARCHAR(50) NOT NULL,
        pcv_description TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System',
        CONSTRAINT idx_pcv_prod_stage_code UNIQUE (pcv_product_id, pcv_stage_code)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_product_pack_styles_t (
        pcv_pack_style_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_pack_type VARCHAR(100) NOT NULL,
        pcv_pack_count INT NOT NULL,
        pcv_pack_code VARCHAR(30) NOT NULL,
        pcv_description TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_pde_registrations_t (
        pcv_pde_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_ref_document_no VARCHAR(50) NOT NULL,
        pcv_validity_date TIMESTAMPTZ NOT NULL,
        pcv_remarks TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_pde_api_details_t (
        pcv_pde_detail_id BIGSERIAL PRIMARY KEY,
        pcv_pde_id BIGINT NOT NULL REFERENCES pcv_pde_registrations_t(pcv_pde_id) ON DELETE CASCADE,
        pcv_api_name VARCHAR(150) NOT NULL,
        pcv_pde_value NUMERIC(10, 4) NOT NULL,
        pcv_uom VARCHAR(20) NOT NULL DEFAULT 'mg/day',
        pcv_description TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_solubility_details_t (
        pcv_solubility_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_api_name VARCHAR(150) NOT NULL,
        pcv_solubility_in_water VARCHAR(50) NOT NULL CHECK (pcv_solubility_in_water IN (
            'Very soluble', 'Freely soluble', 'Soluble', 'Sparingly soluble', 
            'Slightly soluble', 'Very slightly soluble', 'Practically insoluble'
        )),
        pcv_ref_document_no VARCHAR(50) NOT NULL,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_mdd_registrations_t (
        pcv_mdd_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_product_mdd NUMERIC(10, 2) NOT NULL,
        pcv_product_mdd_uom VARCHAR(20) NOT NULL,
        pcv_ref_document_no VARCHAR(50) NOT NULL,
        pcv_remarks TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_mdd_api_details_t (
        pcv_mdd_detail_id BIGSERIAL PRIMARY KEY,
        pcv_mdd_id BIGINT NOT NULL REFERENCES pcv_mdd_registrations_t(pcv_mdd_id) ON DELETE CASCADE,
        pcv_api_name VARCHAR(150) NOT NULL,
        pcv_api_mdd NUMERIC(10, 2) NOT NULL,
        pcv_uom VARCHAR(20) NOT NULL DEFAULT 'mg',
        pcv_description TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_test_methods_t (
        pcv_method_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_test_method_no VARCHAR(50) NOT NULL,
        pcv_api_name VARCHAR(150) NOT NULL,
        pcv_lod_value NUMERIC(10, 4) NOT NULL,
        pcv_loq_value NUMERIC(10, 4) NOT NULL,
        pcv_uom VARCHAR(20) NOT NULL DEFAULT 'ppm',
        pcv_description TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_product_batch_sizes_t (
        pcv_batch_size_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_strength_value VARCHAR(100) NOT NULL,
        pcv_stage_code VARCHAR(30) NOT NULL,
        pcv_batch_size NUMERIC(12, 2) NOT NULL,
        pcv_uom VARCHAR(20) NOT NULL,
        pcv_ref_bmr_no VARCHAR(50) NOT NULL,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_equipments_t (
        pcv_equipment_id VARCHAR(50) PRIMARY KEY,
        pcv_equipment_name VARCHAR(150) NOT NULL,
        pcv_make VARCHAR(100),
        pcv_model VARCHAR(50),
        pcv_capacity NUMERIC(10, 2),
        pcv_uom VARCHAR(20),
        pcv_location_building VARCHAR(100),
        pcv_room_name VARCHAR(100),
        pcv_room_number VARCHAR(30),
        pcv_is_active BOOLEAN DEFAULT TRUE,
        pcv_description TEXT,
        pcv_company_id BIGINT NOT NULL DEFAULT 1 REFERENCES phc_companies_t(pcp_company_id),
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_equipment_surface_areas_t (
        pcv_surface_area_id BIGSERIAL PRIMARY KEY,
        pcv_equipment_id VARCHAR(50) NOT NULL REFERENCES pcv_equipments_t(pcv_equipment_id) ON DELETE CASCADE,
        pcv_product_contact_surface_area NUMERIC(12, 2) NOT NULL,
        pcv_uom VARCHAR(20) NOT NULL DEFAULT 'Sq.Cm',
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_equipment_sampling_locations_t (
        pcv_sampling_loc_id BIGSERIAL PRIMARY KEY,
        pcv_equipment_id VARCHAR(50) NOT NULL REFERENCES pcv_equipments_t(pcv_equipment_id) ON DELETE CASCADE,
        pcv_location_description TEXT NOT NULL,
        pcv_sample_type VARCHAR(30) CHECK (pcv_sample_type IN ('Swab', 'Rinse', 'Swab/Rinse')),
        pcv_test_for VARCHAR(30) CHECK (pcv_test_for IN ('Chemical', 'Micro', 'Detergent')),
        pcv_sample_identifier VARCHAR(30) NOT NULL,
        pcv_image_ref TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_product_equipment_mapping_t (
        pcv_mapping_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id) ON DELETE CASCADE,
        pcv_block_no VARCHAR(30) NOT NULL,
        pcv_module_no VARCHAR(30) NOT NULL,
        pcv_equipment_id VARCHAR(50) NOT NULL REFERENCES pcv_equipments_t(pcv_equipment_id) ON DELETE CASCADE,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System',
        CONSTRAINT idx_pcv_prod_block_equip UNIQUE (pcv_product_id, pcv_block_no, pcv_module_no, pcv_equipment_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_validation_executions_t (
        pcv_execution_id BIGSERIAL PRIMARY KEY,
        pcv_product_id BIGINT NOT NULL REFERENCES pcv_products_t(pcv_product_id),
        pcv_block_no VARCHAR(30) NOT NULL,
        pcv_module_no VARCHAR(30) NOT NULL,
        pcv_protocol_no VARCHAR(50) UNIQUE NOT NULL,
        pcv_current_status VARCHAR(50) DEFAULT 'Training' CHECK (pcv_current_status IN (
            'Training', 'CPR', 'Equipment Clearance', 'TRF', 'Sampling', 'Results Entry', 'Equipment Release', 'Completed'
        )),
        pcv_description TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_training_records_t (
        pcv_training_id BIGSERIAL PRIMARY KEY,
        pcv_execution_id BIGINT NOT NULL REFERENCES pcv_validation_executions_t(pcv_execution_id) ON DELETE CASCADE,
        pcv_trainer_name VARCHAR(100) NOT NULL,
        pcv_trainer_employee_id VARCHAR(50) NOT NULL,
        pcv_emp_id BIGINT REFERENCES phc_emp_t(pem_emp_id),
        pcv_is_submitted BOOLEAN DEFAULT FALSE,
        pcv_submitted_at TIMESTAMPTZ,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_training_attendees_t (
        pcv_attendee_id BIGSERIAL PRIMARY KEY,
        pcv_training_id BIGINT NOT NULL REFERENCES pcv_training_records_t(pcv_training_id) ON DELETE CASCADE,
        pcv_employee_name VARCHAR(100) NOT NULL,
        pcv_employee_id VARCHAR(50) NOT NULL,
        pcv_emp_id BIGINT REFERENCES phc_emp_t(pem_emp_id),
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_cleaning_process_records_t (
        pcv_cpr_id BIGSERIAL PRIMARY KEY,
        pcv_cpr_no VARCHAR(50) UNIQUE NOT NULL,
        pcv_execution_id BIGINT NOT NULL REFERENCES pcv_validation_executions_t(pcv_execution_id),
        pcv_equipment_id VARCHAR(50) NOT NULL REFERENCES pcv_equipments_t(pcv_equipment_id),
        pcv_batch_number VARCHAR(50),
        pcv_sop_no VARCHAR(50) NOT NULL,
        pcv_status VARCHAR(30) DEFAULT 'In-Progress' CHECK (pcv_status IN ('In-Progress', 'Completed', 'Approved')),
        pcv_remarks TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_cpr_execution_steps_t (
        pcv_step_id BIGSERIAL PRIMARY KEY,
        pcv_cpr_id BIGINT NOT NULL REFERENCES pcv_cleaning_process_records_t(pcv_cpr_id) ON DELETE CASCADE,
        pcv_step_number VARCHAR(10) NOT NULL,
        pcv_instruction TEXT NOT NULL,
        pcv_criteria TEXT NOT NULL,
        pcv_observation_type VARCHAR(30) CHECK (pcv_observation_type IN ('Yes/No', 'Done', 'Text_Entry')),
        pcv_is_done BOOLEAN DEFAULT FALSE,
        pcv_vacuum_cleaner_id VARCHAR(50),
        pcv_performed_by VARCHAR(50),
        pcv_performed_at TIMESTAMPTZ,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_equipment_clearance_checklists_t (
        pcv_clearance_id BIGSERIAL PRIMARY KEY,
        pcv_cpr_id BIGINT NOT NULL REFERENCES pcv_cleaning_process_records_t(pcv_cpr_id),
        pcv_status VARCHAR(30) DEFAULT 'Pending QA Inspection',
        pcv_remarks TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_equipment_clearance_items_t (
        pcv_item_id BIGSERIAL PRIMARY KEY,
        pcv_clearance_id BIGINT NOT NULL REFERENCES pcv_equipment_clearance_checklists_t(pcv_clearance_id) ON DELETE CASCADE,
        pcv_checkpoint_no INT NOT NULL,
        pcv_checkpoint_description TEXT NOT NULL,
        pcv_inspected_by_pd VARCHAR(30) CHECK (pcv_inspected_by_pd IN ('YES', 'NO', 'Satisfactory', 'Not satisfactory')),
        pcv_inspected_by_qa VARCHAR(30) CHECK (pcv_inspected_by_qa IN ('YES', 'NO', 'Satisfactory', 'Not satisfactory')),
        pcv_remarks TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_test_request_forms_t (
        pcv_trf_id BIGSERIAL PRIMARY KEY,
        pcv_cpr_id BIGINT NOT NULL REFERENCES pcv_cleaning_process_records_t(pcv_cpr_id),
        pcv_equipment_id VARCHAR(50) NOT NULL REFERENCES pcv_equipments_t(pcv_equipment_id),
        pcv_submitted_to_qa BOOLEAN DEFAULT FALSE,
        pcv_submitted_at TIMESTAMPTZ,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_sampling_records_t (
        pcv_sampling_record_id BIGSERIAL PRIMARY KEY,
        pcv_trf_id BIGINT NOT NULL REFERENCES pcv_test_request_forms_t(pcv_trf_id),
        pcv_is_sampling_done BOOLEAN DEFAULT FALSE,
        pcv_sampling_done_at TIMESTAMPTZ,
        pcv_is_acknowledged_by_qc BOOLEAN DEFAULT FALSE,
        pcv_qc_acknowledged_at TIMESTAMPTZ,
        pcv_remarks TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_test_results_t (
        pcv_result_id BIGSERIAL PRIMARY KEY,
        pcv_sampling_record_id BIGINT NOT NULL REFERENCES pcv_sampling_records_t(pcv_sampling_record_id),
        pcv_sampling_loc_id BIGINT NOT NULL REFERENCES pcv_equipment_sampling_locations_t(pcv_sampling_loc_id),
        pcv_chemical_result NUMERIC(10, 4),
        pcv_microbiological_result NUMERIC(10, 4),
        pcv_detergent_result NUMERIC(10, 4),
        pcv_result_status VARCHAR(20) DEFAULT 'Pass' CHECK (pcv_result_status IN ('Pass', 'Fail', 'OOS')),
        pcv_qa_acknowledged BOOLEAN DEFAULT FALSE,
        pcv_qa_acknowledged_at TIMESTAMPTZ,
        pcv_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pcv_validation_reports_t (
        pcv_report_id BIGSERIAL PRIMARY KEY,
        pcv_execution_id BIGINT NOT NULL REFERENCES pcv_validation_executions_t(pcv_execution_id),
        pcv_report_no VARCHAR(50) UNIQUE NOT NULL,
        pcv_generated_by VARCHAR(50) NOT NULL,
        pcv_generated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        pcv_report_pdf_url TEXT,
        pcv_summary_notes TEXT,
        pcv_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_created_by VARCHAR(50) NOT NULL DEFAULT 'System',
        pcv_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        pcv_modified_by VARCHAR(50) NOT NULL DEFAULT 'System'
    );
    """
]

def classify_table(table_name: str) -> int:
    """
    Categorizes table names by prefix or keyword to determine ERP module ID.
    Returns module ID from 1 to 10. Defaults to 3 (MasterData).
    """
    if not table_name:
        return 3
    t = table_name.lower()
    
    # 10. Cleaning Validation (pcv_ prefix or cleaning keyword)
    if t.startswith('pcv_') or 'cleaning' in t:
        return 10
    
    # 1. Core Architecture
    if any(k in t for k in ['phc_screens', 'phc_module', 'phc_system_params']):
        return 1
        
    # 2. Chart of Accounts
    if any(k in t for k in ['gl_', 'coa_', 'account_']):
        return 2
        
    # 4. HR
    if any(k in t for k in ['emp_', 'dept_', 'payroll_']):
        return 4
        
    # 5. UserMgmt
    if any(k in t for k in ['users_', 'roles_', 'role_screen_']):
        return 5
        
    # 6. Purchasing
    if any(k in t for k in ['po_', 'purchas_', 'supplier_']):
        return 6
        
    # 7. SupplyChain
    if any(k in t for k in ['inv_', 'stock_', 'so_']):
        return 7
        
    # 8. WorkflowSetup
    if any(k in t for k in ['wf_', 'approval_']):
        return 8
        
    # 9. CRM
    if any(k in t for k in ['crm_', 'opportunity_']):
        return 9
        
    # 3. Fallback MasterData
    return 3

async def seed_database():
    """
    Connects to PostgreSQL database, creates all 26 pcv_*_t tables,
    upserts Module ID 10 ("Cleaning Validation"), and registers all 26 screens.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL environment variable not set. Cannot run DDL / DB seeding.")
        return False

    print("Connecting to database for seeding...")
    conn = await asyncpg.connect(dsn=dsn)
    try:
        # 1. Create all 26 DDL tables
        print("Executing Cleaning Validation DDL creation statements...")
        for stmt in CLEANING_VALIDATION_DDL:
            await conn.execute(stmt)
        print("All 26 pcv_*_t tables verified/created successfully.")

        # 2. Upsert Module ID 10 in phc_module_t
        await conn.execute("""
            INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status, pmd_created_by, pmd_modified_by)
            VALUES (10, 'Cleaning Validation', 'ACT', 'System', 'System')
            ON CONFLICT (pmd_module_id) DO UPDATE SET pmd_module_name = EXCLUDED.pmd_module_name
        """)
        print("Module ID 10 ('Cleaning Validation') registered in phc_module_t.")

        # 3. Register all 26 screens into phc_screens_t
        print("Registering 26 Cleaning Validation screens in phc_screens_t...")
        for screen_code, screen_name in CLEANING_VALIDATION_SCREENS.items():
            exists = await conn.fetchval("SELECT psn_screen_id FROM phc_screens_t WHERE psn_screen_code = $1", screen_code)
            if not exists:
                max_scr = await conn.fetchval("SELECT COALESCE(MAX(psn_screen_id), 0) FROM phc_screens_t")
                await conn.execute("""
                    INSERT INTO phc_screens_t 
                    (psn_screen_id, psn_company_id, psn_module_id, psn_screen_code, psn_screen_name, psn_status, psn_created_by, psn_modified_by)
                    VALUES ($1, 1, 10, $2, $3, 'ACT', 'System', 'System')
                """, max_scr + 1, screen_code, screen_name)
            else:
                await conn.execute("""
                    UPDATE phc_screens_t 
                    SET psn_module_id = 10, psn_screen_name = $2, psn_status = 'ACT'
                    WHERE psn_screen_code = $1
                """, screen_code, screen_name)
        print("All 26 screens registered in phc_screens_t with Module ID 10.")
        return True
    finally:
        await conn.close()

if __name__ == '__main__':
    print("Seed DB module loaded. REQUIRED_MODULES defined:")
    for mod in REQUIRED_MODULES:
        print(f"  Module {mod[0]}: {mod[1]} ({mod[2]})")
    
    if os.environ.get("DATABASE_URL"):
        asyncio.run(seed_database())
