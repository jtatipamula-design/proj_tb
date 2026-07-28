import os
import sys
import asyncio
import asyncpg
import logging
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_db")

# Mandatory 9 ERP Modules (IDs 1 to 9)
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
]

# Prefix and table mapping to Module IDs
PREFIX_MODULE_MAP = [
    # Core Architecture (1)
    (('phc_screens_t', 'phc_module_t', 'phc_system_', 'phc_lookup_', 'phc_config_'), 1),
    # Chart of Accounts (2)
    (('phc_gl_', 'phc_coa_', 'phc_account_', 'phc_journal_', 'phc_ledger_', 'phc_period_'), 2),
    # HR (4)
    (('phc_dept_t', 'phc_emp', 'phc_hr', 'phc_payroll', 'phc_job', 'phc_position', 'phc_attend'), 4),
    # UserMgmt (5)
    (('phc_users_t', 'phc_roles_t', 'phc_user_roles_', 'phc_role_screen_', 'phc_auth_'), 5),
    # Purchasing (6)
    (('phc_po_', 'phc_purchas', 'phc_supplier', 'phc_req_', 'phc_rfq_'), 6),
    # SupplyChain (7)
    (('phc_inv_', 'phc_item_', 'phc_stock_', 'phc_warehouse_', 'phc_so_', 'phc_shipment_'), 7),
    # WorkflowSetup (8)
    (('phc_wf_', 'phc_workfl', 'phc_approval', 'phc_notification'), 8),
    # CRM (9)
    (('phc_crm_', 'phc_lead', 'phc_opportunity', 'phc_contact', 'phc_campaign'), 9),
    # MasterData (3)
    (('phc_companies_t', 'phc_currencies_t', 'phc_vendors_t', 'phc_locations_t', 'phc_customers_t', 'phc_branch_t', 'phc_org_t'), 3),
]


def classify_table(table_name: str) -> int:
    """
    Categorize table code to its corresponding module ID.
    Defaults to 3 (MasterData) for unmapped tables.
    """
    table_lower = table_name.lower()
    for prefixes, mod_id in PREFIX_MODULE_MAP:
        for p in prefixes:
            if table_lower.startswith(p) or table_lower == p:
                return mod_id
    return 3  # Fallback to MasterData


async def seed_database(conn: asyncpg.Connection):
    """
    Executes database seeding, screen mapping, and schema cleanup for R4.
    """
    logger.info("--- Starting Database Seeding & Schema Cleanup (R4) ---")

    async with conn.transaction():
        # Step 1: Ensure phc_module_t table exists and has necessary columns
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS phc_module_t (
                pmd_module_id INT PRIMARY KEY,
                pmd_module_name VARCHAR(100) NOT NULL UNIQUE,
                pmd_status VARCHAR(10) DEFAULT 'ACT',
                pmd_admin_only BOOLEAN DEFAULT FALSE,
                pmd_created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pmd_modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("ALTER TABLE phc_module_t ADD COLUMN IF NOT EXISTS pmd_status VARCHAR(10) DEFAULT 'ACT';")
        await conn.execute("ALTER TABLE phc_module_t ADD COLUMN IF NOT EXISTS pmd_admin_only BOOLEAN DEFAULT FALSE;")

        # Step 1.1: Upsert required 9 ERP modules
        for mod_id, mod_name, status in REQUIRED_MODULES:
            # Delete any duplicate module name with a different ID before upserting
            await conn.execute("""
                DELETE FROM phc_module_t 
                WHERE pmd_module_name = $1 AND pmd_module_id != $2;
            """, mod_name, mod_id)

            await conn.execute("""
                INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status)
                VALUES ($1, $2, $3)
                ON CONFLICT (pmd_module_id) DO UPDATE 
                SET pmd_module_name = EXCLUDED.pmd_module_name,
                    pmd_status = EXCLUDED.pmd_status,
                    pmd_modified_on = CURRENT_TIMESTAMP;
            """, mod_id, mod_name, status)
        logger.info("Successfully upserted 9 ERP modules into phc_module_t.")

        # Step 2: Ensure phc_screens_t table exists
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS phc_screens_t (
                psn_screen_id SERIAL PRIMARY KEY,
                psn_screen_code VARCHAR(100) NOT NULL UNIQUE,
                psn_screen_name VARCHAR(100) NOT NULL,
                psn_module_id INT REFERENCES phc_module_t(pmd_module_id),
                psn_status VARCHAR(10) DEFAULT 'ACT',
                psn_created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                psn_modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("ALTER TABLE phc_screens_t ADD COLUMN IF NOT EXISTS psn_module_id INT;")

        # Step 2.1: Discover all phc_% tables in PostgreSQL public schema and register in phc_screens_t
        schema_tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_type = 'BASE TABLE'
              AND table_name ILIKE 'phc_%'
              AND table_name NOT ILIKE 'phc_operating_orgs_t';
        """)

        for row in schema_tables:
            tbl = row['table_name']
            display_name = tbl.replace('phc_', '').replace('_t', '').replace('_', ' ').title()
            await conn.execute("""
                INSERT INTO phc_screens_t (psn_screen_code, psn_screen_name, psn_status)
                VALUES ($1, $2, 'ACT')
                ON CONFLICT (psn_screen_code) DO NOTHING;
            """, tbl, display_name)

        # Step 2.2: Link screens to respective module IDs in phc_screens_t
        all_screens = await conn.fetch("SELECT psn_screen_id, psn_screen_code FROM phc_screens_t;")
        updated_count = 0
        for screen in all_screens:
            code = screen['psn_screen_code']
            target_mod_id = classify_table(code)
            await conn.execute("""
                UPDATE phc_screens_t 
                SET psn_module_id = $1 
                WHERE psn_screen_id = $2;
            """, target_mod_id, screen['psn_screen_id'])
            updated_count += 1
        logger.info(f"Updated psn_module_id for {updated_count} screens in phc_screens_t.")

        # Step 3: Remove references to phc_operating_orgs_t & drop table completely
        # Check if phc_role_screen_assignment_t exists before trying to delete from it
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'phc_role_screen_assignment_t'
            );
        """)
        if table_exists:
            await conn.execute("""
                DELETE FROM phc_role_screen_assignment_t 
                WHERE prs_screen_id IN (
                    SELECT psn_screen_id FROM phc_screens_t WHERE psn_screen_code ILIKE 'phc_operating_orgs_t'
                );
            """)

        await conn.execute("DELETE FROM phc_screens_t WHERE psn_screen_code ILIKE 'phc_operating_orgs_t';")
        await conn.execute("DROP TABLE IF EXISTS phc_operating_orgs_t CASCADE;")
        logger.info("Completely dropped phc_operating_orgs_t and purged all related screen metadata and assignments.")

    logger.info("--- R4 Database Seeding & Schema Cleanup Completed Successfully ---")


async def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable is missing.")
        sys.exit(1)

    logger.info("Connecting to Neon PostgreSQL...")
    conn = await asyncpg.connect(database_url)
    try:
        await seed_database(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
