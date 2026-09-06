import os
import glob
import time
import asyncpg
from app.config import DATABASE_URL, SCHEMA_MUTATING_TABLES, LOOKUP_CACHE_TTL, logger
from app.utils import quote_ident, resolve_lookup_type

# Enterprise Performance In-Memory Caches
SCHEMA_CACHE = {
    "columns": {},
    "schema_maps": {},
    "pks": {},
    "fks": {},
    "display_cols": {},
    "cols_map": None
}

LOOKUP_CACHE = {
    "data": None,
    "expires": 0
}

# (Optional: In `app.auth` we will define USER_AUTH_CACHE. To avoid circular imports, 
# clear_schema_cache will be provided with a way to clear auth cache or we do it carefully)
from typing import Callable
_auth_cache_clear_cb: Callable = None

def register_auth_cache_clear_cb(cb: Callable):
    global _auth_cache_clear_cb
    _auth_cache_clear_cb = cb

def clear_schema_cache():
    """Flushes all schema, lookup, and auth caches."""
    SCHEMA_CACHE["columns"].clear()
    SCHEMA_CACHE["schema_maps"].clear()
    SCHEMA_CACHE["pks"].clear()
    SCHEMA_CACHE["fks"].clear()
    SCHEMA_CACHE["display_cols"].clear()
    SCHEMA_CACHE["cols_map"] = None
    SCHEMA_CACHE["pks_loaded"] = False
    LOOKUP_CACHE["data"] = None
    LOOKUP_CACHE["expires"] = 0
    if _auth_cache_clear_cb:
        _auth_cache_clear_cb()

def invalidate_caches_for_table(table_name: str = None):
    """Selective cache eviction to avoid clearing entire auth & schema caches during standard business record updates."""
    if not table_name or table_name.lower() in SCHEMA_MUTATING_TABLES:
        clear_schema_cache()
    else:
        # Business table changed: selectively invalidate column/display cache if necessary
        t_low = table_name.lower()
        if t_low in SCHEMA_CACHE["columns"]:
            SCHEMA_CACHE["columns"].pop(t_low, None)
            SCHEMA_CACHE["schema_maps"].pop(t_low, None)

async def get_all_lookups(conn, force_refresh=False):
    """Fetches and caches active lookups in memory to avoid full table scans on every page view."""
    now = time.time()
    if not force_refresh and LOOKUP_CACHE["data"] is not None and now < LOOKUP_CACHE["expires"]:
        return LOOKUP_CACHE["data"]
    
    lookup_map = {}
    try:
        l_rows = await conn.fetch("""
            SELECT upper(plv_lookup_code) as type_code, plv_lookup_value_code as id, plv_lookup_value_name as name 
            FROM phc_lookup_values_t 
            WHERE plv_status = 'ACT'
              AND CURRENT_DATE BETWEEN COALESCE(plv_start_date, CURRENT_DATE) AND COALESCE(plv_end_date, CURRENT_DATE + interval '1 day')
        """)
        for lr in l_rows:
            tc = lr['type_code']
            if tc:
                if tc not in lookup_map:
                    lookup_map[tc] = {}
                lookup_map[tc][str(lr['id'])] = str(lr['name'])
    except Exception:
        try:
            l_rows = await conn.fetch("""
                SELECT upper(plv_lookup_type_code) as type_code, plv_lookup_value_code as id, plv_lookup_value_name as name 
                FROM phc_lookup_values_t 
                WHERE plv_status = 'ACT'
                  AND CURRENT_DATE BETWEEN COALESCE(plv_start_date, CURRENT_DATE) AND COALESCE(plv_end_date, CURRENT_DATE + interval '1 day')
            """)
            for lr in l_rows:
                tc = lr['type_code']
                if tc:
                    if tc not in lookup_map:
                        lookup_map[tc] = {}
                    lookup_map[tc][str(lr['id'])] = str(lr['name'])
        except Exception:
            lookup_map = {}

    LOOKUP_CACHE["data"] = lookup_map
    LOOKUP_CACHE["expires"] = now + LOOKUP_CACHE_TTL
    return lookup_map

async def get_table_columns(conn, table_name: str):
    """Fetches and caches table column metadata to avoid repeated information_schema queries."""
    if table_name in SCHEMA_CACHE["columns"]:
        return SCHEMA_CACHE["columns"][table_name]
    query = """
        SELECT column_name, data_type, is_nullable, character_maximum_length, column_default 
        FROM information_schema.columns 
        WHERE table_name = $1 AND table_schema = 'public'
        ORDER BY ordinal_position
    """
    rows = await conn.fetch(query, table_name)
    cols = [dict(r) for r in rows]
    SCHEMA_CACHE["columns"][table_name] = cols
    SCHEMA_CACHE["schema_maps"][table_name] = {c['column_name']: c for c in cols}
    return cols

async def get_pk_column(conn, table_name):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    query = """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1::regclass AND i.indisprimary;
    """
    try:
        pk = await conn.fetchval(query, table_name)
        if pk:
            SCHEMA_CACHE["pks"][table_name] = pk
            return pk
    except Exception:
        pass

    # Heuristic fallback if table lacks formal primary key constraint
    try:
        cols = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)
        col_names = [c['column_name'] for c in cols]
        for c in col_names:
            if c.endswith('_id') or c.endswith('_code') or c == 'id':
                SCHEMA_CACHE["pks"][table_name] = c
                return c
        if col_names:
            SCHEMA_CACHE["pks"][table_name] = col_names[0]
            return col_names[0]
    except Exception:
        pass
    return None

async def get_fk_map(conn, table_name):
    if "fks" not in SCHEMA_CACHE: SCHEMA_CACHE["fks"] = {}
    if table_name in SCHEMA_CACHE["fks"]: return SCHEMA_CACHE["fks"][table_name]
    query = """
        SELECT kcu.column_name AS col, ccu.table_name AS f_table, ccu.column_name AS f_col
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1
    """
    try:
        fks = await conn.fetch(query, table_name)
        fk_map = {row['col']: {'table': row['f_table'], 'pk': row['f_col']} for row in fks}
        SCHEMA_CACHE["fks"][table_name] = fk_map
        return fk_map
    except Exception: return {}

async def preload_all_pks(conn):
    if SCHEMA_CACHE.get("pks_loaded", False): return
    query = """
        SELECT c.relname as table_name, a.attname as column_name
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.indisprimary AND n.nspname NOT IN ('information_schema', 'pg_catalog')
    """
    try:
        rows = await conn.fetch(query)
        for r in rows:
            SCHEMA_CACHE["pks"][r['table_name']] = r['column_name']
        SCHEMA_CACHE["pks_loaded"] = True
    except Exception as e:
        logger.error(f"Error preloading PKs: {e}")

async def resolve_fk_details(conn, table_name, column_name):
    fk_map = await get_fk_map(conn, table_name)
    if column_name in fk_map: return fk_map[column_name]['table'], fk_map[column_name]['pk']
    
    await preload_all_pks(conn)
    if not column_name.endswith('_id'):
        return None, None
        
    def get_base_name(name):
        parts = name.split('_', 1)
        if len(parts) == 2 and len(parts[0]) <= 4 and len(parts[1]) >= 3:
            return parts[1]
        return name

    col_stripped = get_base_name(column_name)
            
    for t_name, pk_col in SCHEMA_CACHE["pks"].items():
        if pk_col == column_name:
            return t_name, pk_col
        pk_stripped = get_base_name(pk_col)
        if col_stripped == pk_stripped:
            return t_name, pk_col
            
    return None, None

async def get_fk_display_dict(conn, f_table, f_pk, specific_ids=None):
    if "display_cols" not in SCHEMA_CACHE: SCHEMA_CACHE["display_cols"] = {}
    if f_table not in SCHEMA_CACHE["display_cols"]:
        cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1", f_table)
        col_names = [c['column_name'] for c in cols]
        display_col = f_pk
        for c in col_names:
            if c.endswith('_name') or c == 'name':
                display_col = c
                break
        status_col = None
        for c in col_names:
            if c.endswith('_status') or c == 'status':
                status_col = c
                break
        SCHEMA_CACHE["display_cols"][f_table] = {"display": display_col, "status": status_col}
        
    info = SCHEMA_CACHE["display_cols"][f_table]
    display_col = info["display"]
    status_col = info["status"]
    
    q_table = quote_ident(f_table)
    q_pk = quote_ident(f_pk)
    q_display = quote_ident(display_col)

    if specific_ids is not None:
        if not specific_ids: return {}
        q = f"SELECT {q_pk} as id, {q_display} as name FROM {q_table} WHERE {q_pk} = ANY($1)"
        rows = await conn.fetch(q, list(specific_ids))
        return {r['id']: r['name'] for r in rows}
    else:
        q = f"SELECT {q_pk} as id, {q_display} as name FROM {q_table}"
        if status_col: q += f" WHERE {quote_ident(status_col)} = 'ACT'"
        rows = await conn.fetch(q)
        return [{"id": str(r['id']), "name": f"{r['name']} (ID: {r['id']})"} for r in rows]

async def get_dropdown_options(conn, table_name, column_name, preloaded_lookups=None):
    if column_name.endswith('_org_id') or column_name == 'pos_org_id':
        return []
        
    # 1. Dynamic Canonical Lookup System Check (Cached / In-Memory)
    lookup_code = resolve_lookup_type(column_name)
    raw_upper = column_name.upper()
    
    lookup_map = preloaded_lookups if preloaded_lookups is not None else await get_all_lookups(conn)
    col_lookup = lookup_map.get(lookup_code) or lookup_map.get(raw_upper)
    if col_lookup:
        return [{"id": str(k), "name": str(v)} for k, v in col_lookup.items()]

    # 2. Foreign Key Dropdown Fallback
    f_table, f_pk = await resolve_fk_details(conn, table_name, column_name)
    if f_table and f_pk:
        try:
            return await get_fk_display_dict(conn, f_table, f_pk)
        except Exception:
            pass
    return []

async def setup_db(app, loop):
    """Initializes the optimized asyncpg connection pool without statement caching for full DDL/schema resilience."""
    if DATABASE_URL:
        app.ctx.pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=5,
            max_size=25,
            max_inactive_connection_lifetime=300.0,
            statement_cache_size=0,
            max_cached_statement_lifetime=0
        )
        try:
            async with app.ctx.pool.acquire() as conn:
                lock_acquired = await conn.fetchval("SELECT pg_try_advisory_lock(742918)")
                if not lock_acquired:
                    return
                try:
                    # 1. Migration Tracking Table
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS phc_schema_migrations_t (
                            version VARCHAR(255) PRIMARY KEY,
                            applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    
                    # 2. Find and execute missing migrations
                    migration_files = sorted(glob.glob("migrations/*.sql"))
                    for mf in migration_files:
                        version = os.path.basename(mf)
                        is_applied = await conn.fetchval("SELECT 1 FROM phc_schema_migrations_t WHERE version = $1", version)
                        if not is_applied:
                            logger.info(f"Applying migration: {version}")
                            with open(mf, 'r') as f:
                                sql = f.read()
                            async with conn.transaction():
                                await conn.execute(sql)
                                await conn.execute("INSERT INTO phc_schema_migrations_t (version) VALUES ($1)", version)
                finally:
                    await conn.execute("SELECT pg_advisory_unlock(742918)")
        except Exception as e:
            logger.warning(f"DB init safeguard non-fatal notice: {e}")
    else:
        app.ctx.pool = None

async def close_db(app, loop):
    """Gracefully closes all pooled connections on server shutdown."""
    if hasattr(app.ctx, 'pool') and app.ctx.pool:
        await app.ctx.pool.close()
