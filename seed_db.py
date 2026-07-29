"""
seed_db.py - Seeds the 9 base ERP modules into phc_module_t on startup.
Screen-to-module assignments are managed via psn_module_id in phc_screens_t.
The app reads module/screen structure dynamically from the database.
"""
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_db")

# The exact required ERP modules list
REQUIRED_MODULES = [
    (1, 'ERPAdmin'),
    (2, 'Chart of Accounts'),
    (3, 'MasterData'),
    (4, 'HR'),
    (5, 'User Management'),
    (6, 'AppAdmin'),
    (7, 'Compliance and Documentation'),
    (8, 'Purchasing'),
    (9, 'SupplyChain'),
    (10, 'WorkflowSetup'),
    (11, 'CRM'),
]

async def seed_database(conn):
    """
    1. Ensures the correct ERP modules exist in phc_module_t and updates names if they changed.
    2. Registers the system tables (phc_module_t, phc_screens_t) as screens under ERPAdmin.
    """
    logger.info("--- Starting Database Bootstrapping ---")

    async with conn.transaction():
        # 1. Sync Modules
        for mod_id, mod_name in REQUIRED_MODULES:
            exists = await conn.fetchval("SELECT pmd_module_name FROM phc_module_t WHERE pmd_module_id = $1", mod_id)
            if not exists:
                await conn.execute(
                    "INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status) VALUES ($1, $2, 'ACT')",
                    mod_id, mod_name
                )
                logger.info(f"Inserted module: {mod_name}")
            elif exists != mod_name:
                # Update name if it changed (e.g. UserMgmt -> User Management)
                await conn.execute(
                    "UPDATE phc_module_t SET pmd_module_name = $1 WHERE pmd_module_id = $2",
                    mod_name, mod_id
                )
                logger.info(f"Updated module name to: {mod_name}")

        # 2. Register System Screens under ERPAdmin (Module ID 1)
        # This allows Admin users to manage Modules and Screens directly from the UI
        system_screens = [
            (9901, 1, 'phc_module_t', 'Manage Modules'),
            (9902, 1, 'phc_screens_t', 'Manage Screens')
        ]
        
        for screen_id, mod_id, code, name in system_screens:
            exists = await conn.fetchval("SELECT 1 FROM phc_screens_t WHERE psn_screen_code = $1", code)
            if not exists:
                try:
                    await conn.execute(
                        """INSERT INTO phc_screens_t (psn_screen_id, psn_company_id, psn_module_id, psn_screen_code, psn_screen_name, psn_status) 
                           VALUES ($1, 1, $2, $3, $4, 'ACT')""",
                        screen_id, mod_id, code, name
                    )
                    logger.info(f"Registered system screen: {name} under ERPAdmin")
                except Exception as e:
                    logger.error(f"Could not register screen {name}: {e}")

    logger.info("--- Bootstrapping Complete ---")
