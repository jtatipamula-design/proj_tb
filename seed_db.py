"""
seed_db.py - Seeds the 9 base ERP modules into phc_module_t on startup.
Screen-to-module assignments are managed via psn_module_id in phc_screens_t.
The app reads module/screen structure dynamically from the database.
"""
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_db")

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


async def seed_database(conn):
    logger.info("--- Starting Module Seeding ---")

    async with conn.transaction():
        for mod_id, mod_name in REQUIRED_MODULES:
            exists = await conn.fetchval(
                "SELECT 1 FROM phc_module_t WHERE pmd_module_id = $1", mod_id
            )
            if not exists:
                await conn.execute(
                    "INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status) VALUES ($1, $2, 'ACT')",
                    mod_id, mod_name
                )
                logger.info(f"Inserted module: {mod_name}")
            else:
                logger.info(f"Module already exists: {mod_name}")

    logger.info("--- Module Seeding Complete ---")
