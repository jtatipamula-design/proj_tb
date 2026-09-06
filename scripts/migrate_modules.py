"""
Standalone Administrative CLI Database Migration Tool.
Synchronizes screens and modules with the master Excel configuration.

Usage:
    python migrate_modules.py
"""

import os
import sys
import asyncio
import asyncpg

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("\n" + "="*70)
    print("ERROR: DATABASE_URL environment variable is not set in .env")
    print("="*70 + "\n")
    sys.exit(1)

EXCEL_MAPPINGS = {
    "phc_companies_t": "ERPAdmin",
    "phc_operating_orgs_t": "ERPAdmin",
    "phc_dept_t": "Chart of Accounts",
    "phc_services_t": "Chart of Accounts",
    "phc_cost_center_t": "Chart of Accounts",
    "phc_locations_t": "MasterData",
    "phc_emp_t": "HR",
    "phc_apps_t": "HR",
    "phc_emp_apps_grant_t": "HR",
    "phc_lookup_types": "ERPAdmin",
    "phc_lookup_types_t": "ERPAdmin",
    "phc_lookup_values_t": "MasterData",
    "phc_users_t": "User Management",
    "phc_screens_t": "User Management",
    "phc_roles_t": "User Management",
    "phc_role_screen_assignment_t": "User Management",
    "phc_user_roles_assignment_t": "User Management",
    "phc_user_group_t": "User Management",
    "phc_user_log_t": "User Management",
    "phc_error_log_t": "AppAdmin",
    "phc_audit_log_t": "Compliance and Documenation",
    "phc_plant_master_t": "MasterData",
    "phc_plant_compliance_t": "Compliance and Documenation",
    "phc_certifications_t": "Compliance and Documenation",
    "phc_plant_equipment_t": "Purchasing",
    "phc_equipment_locations_t": "SupplyChain",
    "phc_material_master_t": "Purchasing",
    "phc_material_group_master_t": "Purchasing",
    "phc_uom_master_t": "MasterData",
    "phc_uom_conversion_t": "MasterData",
    "phc_prod_master_t": "MasterData",
    "phc_prod_lifecycle_history_t": "MasterData",
    "phc_prod_alt_names_t": "MasterData",
    "phc_approval_types_t": "WorkflowSetup",
    "phc_approval_setup_t": "WorkflowSetup",
    "phc_notifications_setup_t": "WorkflowSetup",
    "phc_approval_events_t": "Compliance and Documenation",
    "phc_number_range_master_t": "ERPAdmin",
    "phc_storage_location_master_t": "MasterData",
    "phc_partners_t": "MasterData",
    "phc_customer_t": "CRM",
    "phc_cust_site_t": "CRM",
    "phc_cust_contact_points_t": "CRM",
    "phc_cust_site_locations_t": "CRM",
    "phc_vendors_t": "Purchasing",
    "phc_vend_sites_t": "Purchasing",
    "phc_vend_contact_points_t": "Purchasing",
    "phc_vend_site_locations_t": "Purchasing",
    "phc_prod_formulation": "Production",
    "phc_prod_ingredients": "Production",
    "phc_prod_pack_presentation": "Production",
    "phc_prod_regulatory_status": "Production",
    "phc_prod_regulatory_variations": "Production",
    "phc_prod_ectd_documents": "Production",
    "phc_product_indications": "Production",
    "phc_prod_pharmacology": "Production",
    "phc_prod_dosing_regimen": "Production",
    "phc_prod_contraindications": "Production",
    "phc_prod_warnings": "Production",
    "phc_prod_drug_interactions": "Production",
    "phc_prod_adverse_events": "Production",
    "phc_prod_special_populations": "Production",
    "phc_clinical_trials": "Production",
    "phc_prod_immunogenicity_data": "Production",
    "phc_prod_manufacturing_site": "Production",
    "phc_prod_batch_specification": "Production",
    "phc_prod_process_parameters": "Production",
    "phc_prod_finished_specifications": "Production",
    "phc_prod_reference_standards": "Production",
    "phc_prod_stability_studies": "Production",
    "phc_prod_container_closure": "Production",
    "phc_prod_deviations_capa": "Production",
    "phc_prod_gmp_certificates": "Production",
    "phc_prod_site_inspections": "Production",
    "phc_prod_packaging_spec": "Production",
    "phc_prod_labeling": "Production",
    "phc_prod_serialization": "Production",
    "phc_prod_patents": "Production",
    "phc_prod_exclusivity": "Production",
    "phc_prod_loe": "Production",
    "phc_prod_competitor_filings": "Production",
    "phc_prod_licensing": "Production",
    "phc_prod_launch": "Production",
    "phc_prod_pricing": "CRM",
    "phc_prod_reimbursement": "Production",
    "phc_prod_sales_performance": "Production",
    "phc_prod_safety_database_ref": "Production",
    "phc_prod_signal_detection": "Production",
    "phc_prod_psur_schedule": "Production",
    "phc_prod_rems_elements": "Production",
    "phc_prod_recalls": "CRM",
    "phc_prod_special_monitoring": "Production",
    "phc_prod_counterfeit_reports": "Production",
    "phc_prod_api_suppliers": "Production",
    "phc_prod_cmo": "Production",
    "phc_prod_distribution_model": "Production",
    "phc_prod_demand_forecast": "Production",
    "phc_prod_supply_risk": "Production",
    "phc_prod_heor_data": "Production",
    "phc_prod_environmental_data": "Production",
    "phc_prod_biologics_detail": "Production",
    "phc_prod_device_component": "Production",
    "phc_sop_master": "Production Compliance and Documenation",
    "phc_sop_revision_history": "Production Compliance and Documenation",
    "phc_sop_related_equipment": "Production Compliance and Documenation",
    "phc_sop_procedure_steps": "Production Compliance and Documenation",
    "phc_sop_training_records": "Production Compliance and Documenation",
    "phc_sod_authorization_matrix": "Production Compliance and Documenation",
    "phc_sod_conflict_rules": "Production Compliance and Documenation",
    "phc_sod_employee_role_assignment": "Production Compliance and Documenation",
    "phc_sod_event_actor_log": "Production Compliance and Documenation",
    "phc_cleaning_batch": "Qualtiy - Cleaning Validation",
    "phc_cleaning_request": "Qualtiy - Cleaning Validation",
    "phc_cleaning_batch_step": "Qualtiy - Cleaning Validation",
    "phc_cleaning_visual_inspection": "Qualtiy - Cleaning Validation",
    "phc_cleaning_qa_approval": "Qualtiy - Cleaning Validation",
    "phc_cleaning_release": "Qualtiy - Cleaning Validation",
    "pcv_products_t": "Cleaning Validation",
    "pcv_product_strengths_t": "Cleaning Validation",
    "pcv_product_stages_t": "Cleaning Validation",
    "pcv_product_pack_styles_t": "Cleaning Validation",
    "pcv_pde_registrations_t": "Cleaning Validation",
    "pcv_pde_api_details_t": "Cleaning Validation",
    "pcv_solubility_details_t": "Cleaning Validation",
    "pcv_mdd_registrations_t": "Cleaning Validation",
    "pcv_mdd_api_details_t": "Cleaning Validation",
    "pcv_test_methods_t": "Cleaning Validation",
    "pcv_product_batch_sizes_t": "Cleaning Validation",
    "pcv_equipments_t": "Cleaning Validation",
    "pcv_equipment_surface_areas_t": "Cleaning Validation",
    "pcv_equipment_sampling_locations_t": "Cleaning Validation",
    "pcv_product_equipment_mapping_t": "Cleaning Validation",
    "pcv_validation_executions_t": "Cleaning Validation",
    "pcv_training_records_t": "Cleaning Validation",
    "pcv_training_attendees_t": "Cleaning Validation",
    "pcv_cleaning_process_records_t": "Cleaning Validation",
    "pcv_cpr_execution_steps_t": "Cleaning Validation",
    "pcv_equipment_clearance_checklists_t": "Cleaning Validation",
    "pcv_equipment_clearance_items_t": "Cleaning Validation",
    "pcv_test_request_forms_t": "Cleaning Validation",
    "pcv_sampling_records_t": "Cleaning Validation",
    "pcv_test_results_t": "Cleaning Validation",
    "pcv_validation_reports_t": "Cleaning Validation",
    "phc_module_t": "ERPAdmin",
    "phc_screens_t": "ERPAdmin"
}

async def run_migration():
    print(f"Connecting to PostgreSQL database...")
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        async with conn.transaction():
            print("Cleaning up orphan test screens...")
            await conn.execute("DELETE FROM phc_screens_t WHERE psn_screen_name = 'Updated Screen'")
            
            print("Ensuring all modules from mapping exist...")
            unique_modules = set(EXCEL_MAPPINGS.values())
            for mod_name in unique_modules:
                exists = await conn.fetchval("SELECT pmd_module_id FROM phc_module_t WHERE pmd_module_name = $1", mod_name)
                if not exists:
                    max_id = await conn.fetchval("SELECT MAX(pmd_module_id) FROM phc_module_t")
                    await conn.execute(
                        "INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status, pmd_created_by, pmd_modified_by) VALUES ($1, $2, 'ACT', 'System', 'System')",
                        (max_id or 0) + 1, mod_name
                    )
            
            mod_rows = await conn.fetch("SELECT pmd_module_id, pmd_module_name FROM phc_module_t")
            mod_dict = {row['pmd_module_name']: row['pmd_module_id'] for row in mod_rows}

            print("Aligning screens to modules...")
            updated_count = 0
            for screen_code, module_name in EXCEL_MAPPINGS.items():
                target_mod_id = mod_dict.get(module_name)
                if target_mod_id:
                    res = await conn.execute(
                        "UPDATE phc_screens_t SET psn_module_id = $1 WHERE psn_screen_code = $2",
                        target_mod_id, screen_code
                    )
                    if not res.endswith(" 0"):
                        updated_count += 1

            print(f"Updated {updated_count} screen assignments.")

            for screen_code, screen_name in [('phc_module_t', 'Manage Modules'), ('phc_screens_t', 'Manage Screens')]:
                exists = await conn.fetchval("SELECT psn_screen_id FROM phc_screens_t WHERE psn_screen_code = $1", screen_code)
                if not exists:
                    max_scr = await conn.fetchval("SELECT MAX(psn_screen_id) FROM phc_screens_t")
                    await conn.execute("""
                        INSERT INTO phc_screens_t (psn_screen_id, psn_company_id, psn_module_id, psn_screen_code, psn_screen_name, psn_status, psn_created_by, psn_modified_by) 
                        VALUES ($1, 1, $2, $3, $4, 'ACT', 'System', 'System')
                    """, (max_scr or 0) + 1, mod_dict.get('ERPAdmin', 1), screen_code, screen_name)
                    print(f"Registered admin screen: {screen_name}")

        print("\nMigration completed successfully!")
    finally:
        await conn.close()

if __name__ == '__main__':
    asyncio.run(run_migration())
