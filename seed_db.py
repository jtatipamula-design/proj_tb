"""
seed_db.py - Database seeder for the dynamic ERP module system.
Called once on server startup to ensure modules exist and screens are mapped.
"""
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_db")

# The 9 ERP modules to seed (matching your phc_module_t schema exactly)
REQUIRED_MODULES = [
    (1, 'Core Architecture'),
    (2, 'Chart of Accounts'),
    (3, 'MasterData'),
    (4, 'HR'),
    (5, 'UserMgmt'),
    (6, 'Purchasing'),
    (7, 'SupplyChain'),
    (8, 'WorkflowSetup'),
    (9, 'CRM'),
]

# Maps table name prefixes/exact names to module IDs
TABLE_TO_MODULE = {
    # Core Architecture (1)
    'phc_companies_t': 1,
    'phc_lookup_types': 1,
    'phc_number_range_master_t': 1,
    'phc_screens_t': 1,
    'phc_module_t': 1,
    # Chart of Accounts (2)
    'phc_dept_t': 2,
    'phc_services_t': 2,
    'phc_cost_center_t': 2,
    # MasterData (3)
    'phc_locations_t': 3,
    'phc_lookup_values_t': 3,
    'phc_plant_master_t': 3,
    'phc_plant_compliance_t': 3,
    'phc_certifications_t': 3,
    'phc_uom_master_t': 3,
    'phc_uom_conversion_t': 3,
    'phc_prod_master_t': 3,
    'phc_prod_lifecycle_history_t': 3,
    'phc_prod_alt_names_t': 3,
    'phc_storage_location_master_t': 3,
    'phc_partners_t': 3,
    # HR (4)
    'phc_emp_t': 4,
    'phc_apps_t': 4,
    'phc_emp_apps_grant_t': 4,
    # UserMgmt (5)
    'phc_users_t': 5,
    'phc_roles_t': 5,
    'phc_role_screen_assignment_t': 5,
    'phc_user_roles_assignment_t': 5,
    'phc_user_group_t': 5,
    'phc_user_log_t': 5,
    'phc_error_log_t': 5,
    # Purchasing (6)
    'phc_material_group_master_t': 6,
    'phc_material_master_t': 6,
    'phc_plant_equipment_t': 6,
    # SupplyChain (7)
    'phc_equipment_locations_t': 7,
    # WorkflowSetup (8)
    'phc_approval_types_t': 8,
    'phc_approval_setup_t': 8,
    'phc_notifications_setup_t': 8,
    'phc_approval_events_t': 8,
    # CRM (9)
    'phc_customer_t': 9,
    'phc_cust_site_t': 9,
    'phc_cust_contact_points_t': 9,
    'phc_cust_site_locations_t': 9,
}


async def seed_database(conn):
    """
    Idempotent database seeder. Safe to run on every server startup.
    Uses only columns that actually exist in your Neon database.
    """
    logger.info("--- Starting Database Seeding & Schema Cleanup ---")

    async with conn.transaction():
        # Step 1: Seed the 9 ERP modules into phc_module_t
        for mod_id, mod_name in REQUIRED_MODULES:
            exists = await conn.fetchval(
                "SELECT 1 FROM phc_module_t WHERE pmd_module_id = $1", mod_id
            )
            if exists:
                await conn.execute(
                    "UPDATE phc_module_t SET pmd_module_name = $1 WHERE pmd_module_id = $2",
                    mod_name, mod_id
                )
            else:
                await conn.execute(
                    "INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status) VALUES ($1, $2, 'ACT')",
                    mod_id, mod_name
                )
        logger.info("Successfully seeded 9 ERP modules into phc_module_t.")

        # Step 2: Discover all phc_% tables and register as screens
        schema_tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_type = 'BASE TABLE'
              AND table_name ILIKE 'phc_%'
              AND table_name NOT ILIKE 'phc_operating_orgs_t'
        """)

        for row in schema_tables:
            tbl = row['table_name']
            display_name = tbl.replace('phc_', '').replace('_t', '').replace('_', ' ').title()
            exists = await conn.fetchval(
                "SELECT 1 FROM phc_screens_t WHERE psn_screen_code = $1", tbl
            )
            if not exists:
                await conn.execute(
                    "INSERT INTO phc_screens_t (psn_screen_code, psn_screen_name, psn_status) VALUES ($1, $2, 'ACT')",
                    tbl, display_name
                )

        # Step 3: Link each screen to its correct module
        all_screens = await conn.fetch("SELECT psn_screen_id, psn_screen_code FROM phc_screens_t")
        updated = 0
        for screen in all_screens:
            code = screen['psn_screen_code'].lower()
            target_mod = TABLE_TO_MODULE.get(code, 3)
            await conn.execute(
                "UPDATE phc_screens_t SET psn_module_id = $1 WHERE psn_screen_id = $2",
                target_mod, screen['psn_screen_id']
            )
            updated += 1
        logger.info(f"Linked {updated} screens to their modules.")

        # Step 4: Drop phc_operating_orgs_t and clean up references
        rsa_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'phc_role_screen_assignment_t'
            )
        """)
        if rsa_exists:
            await conn.execute("""
                DELETE FROM phc_role_screen_assignment_t 
                WHERE prs_screen_id IN (
                    SELECT psn_screen_id FROM phc_screens_t 
                    WHERE psn_screen_code ILIKE 'phc_operating_orgs_t'
                )
            """)

        await conn.execute("DELETE FROM phc_screens_t WHERE psn_screen_code ILIKE 'phc_operating_orgs_t'")
        await conn.execute("DROP TABLE IF EXISTS phc_operating_orgs_t CASCADE")
        logger.info("Dropped phc_operating_orgs_t and cleaned up all references.")

    logger.info("--- Database Seeding Complete ---")
