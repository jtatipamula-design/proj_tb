"""
ERP Database Seeding Module and Module Classification Rules.
Includes Cleaning Validation (Module 10) seeding logic.
Table DDL lives in migrations/000_base_schema.sql (single source of truth).
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
    "pcv_production_products_t": "Products & Dosage Master",  # live table name (was pcv_products_t)
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
    Connects to PostgreSQL database, upserts Module ID 10 ("Cleaning Validation"),
    and registers all 26 screens. Requires the schema from migrations/000_base_schema.sql.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL environment variable not set. Cannot run DDL / DB seeding.")
        return False

    print("Connecting to database for seeding...")
    conn = await asyncpg.connect(dsn=dsn)
    try:
        # Schema (all pcv_*_t and phc_*_t tables) is created by migrations/000_base_schema.sql,
        # applied by app/database.py setup_db. This script only seeds data.

        # 1. Upsert Module ID 10 in phc_module_t
        await conn.execute("""
            INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status, pmd_created_by, pmd_modified_by)
            VALUES (10, 'Cleaning Validation', 'ACT', 'System', 'System')
            ON CONFLICT (pmd_module_id) DO UPDATE SET pmd_module_name = EXCLUDED.pmd_module_name
        """)
        print("Module ID 10 ('Cleaning Validation') registered in phc_module_t.")

        # 2. Register all 26 screens into phc_screens_t
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
