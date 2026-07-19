import os
import csv
import io
import time
import uuid
import asyncio
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from functools import wraps, partial
import json

import asyncpg
import bcrypt
import jwt
from dotenv import load_dotenv
from sanic import Sanic, response
from sanic_ext import Extend, render

# Load environment variables
load_dotenv()

app = Sanic("ERP_System")

# Security Validation
env_secret = os.environ.get("SECRET_KEY")
is_development = os.environ.get("ENVIRONMENT") == "development"

if not env_secret and not is_development:
    raise RuntimeError("CRITICAL: SECRET_KEY environment variable is required in production.")

app.config.SECRET = env_secret or "DEFAULT_FALLBACK_SECRET"
app.config.TEMPLATING_PATH_TO_TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")
Extend(app)

CLOUD_DB_URL = os.environ.get("DB_URL")

# ==========================================
#  CONSTANTS & IN-MEMORY CACHING
# ==========================================
RATE_LIMIT_WINDOW = 60
MAX_REQUESTS = 120

SCHEMA_CACHE = {
    "tables": None,
    "pks": {},
    "columns": {},
    "dropdown_lookups": {}
}

AUTH_CACHE = {} 
RBAC_CACHE = {}

WHO_COLS = {'creation_date', 'created_by', 'last_update_date', 'last_updated_by'}


# ==========================================
#  DYNAMIC PREFIX ROUTING ENGINE
# ==========================================
def get_table_modules(tables):
    mapping = {}
    
    exceptions = {
        'phc_emp_t': 'Employee',
        'phc_apps_t': 'AppSetup',
        'phc_roles_t': 'AppSetup',
        'phc_screens_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup',
        'phc_role_screen_assignment_t': 'AppSetup',
        'phc_users_t': 'AppSetup',
        'phc_user_log_t': 'AppSetup',
        'phc_user_groups_t': 'AppSetup',
        'phc_companies_t': 'MasterData',
        'phc_cost_centers_t': 'MasterData',
        'phc_cost_center_t': 'MasterData',
        'phc_dept_t': 'MasterData',
        'phc_locations_t': 'MasterData',
        'phc_orgs_t': 'MasterData',
        'phc_services_t': 'MasterData',
        'phc_lookup_types': 'MasterData',
        'phc_lookup_values_t': 'MasterData'
    }
    
    for table in tables:
        if table in exceptions:
            mapping[table] = exceptions[table]
        elif table.startswith('cv_'): mapping[table] = 'Cleaning'
        elif table.startswith('po_'): mapping[table] = 'Procurement'
        elif table.startswith('ap_'): mapping[table] = 'Payables'
        elif table.startswith('par_') or table.startswith('pra_'): mapping[table] = 'Receivables'
        elif table.startswith('pgl_'): mapping[table] = 'Ledger'
        elif table.startswith('pmd_'): mapping[table] = 'CustomerSetup'
        elif table.startswith('poe_'): mapping[table] = 'OrderMgmt'
        elif table.startswith('pa_'): mapping[table] = 'Project'
        elif table.startswith('mtl_'): mapping[table] = 'Product'
        elif table.startswith('phc_plant_') or table.startswith('phc_equ') or table.startswith('phc_cert'): mapping[table] = 'MasterData'
        elif table.startswith('phc_material_') or table.startswith('phc_uom_'): mapping[table] = 'Product' # Maps to Inventory
        elif table.startswith('phc_prod_'): mapping[table] = 'Product' # Maps to Inventory            
        else: mapping[table] = 'Other'
        
    return mapping


# ==========================================
#  SECURITY MIDDLEWARE
# ==========================================
ip_tracker = defaultdict(list)

@app.on_request
async def rate_limiter(request):
    ip = request.remote_addr or request.ip
    now = time.time()

    if len(ip_tracker) > 10000:
        ip_tracker.clear()

    ip_tracker[ip] = [t for t in ip_tracker[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(ip_tracker[ip]) >= MAX_REQUESTS:
        return response.json({"error": "Rate limit exceeded. Please slow down."}, status=429)
    ip_tracker[ip].append(now)


@app.on_response
async def add_security_headers(request, resp):
    if resp:
        resp.headers['X-Frame-Options'] = 'SAMEORIGIN'
        resp.headers['X-Content-Type-Options'] = 'nosniff'
        resp.headers['X-XSS-Protection'] = '1; mode=block'
        resp.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        resp.headers['Cache-Control'] = 'private, no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        resp.headers['Vary'] = 'Cookie, Authorization'
        resp.headers['Content-Security-Policy'] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data:;"
        )


def login_required(wrapped):
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        token = request.cookies.get("auth_token")

        def force_login_redirect():
            if request.headers.get("HX-Request"):
                resp = response.text("Session Expired")
                resp.headers["HX-Redirect"] = "/login"
                resp.delete_cookie("auth_token")
                return resp
            resp = response.redirect("/login")
            resp.delete_cookie("auth_token")
            return resp

        if not token:
            return force_login_redirect()

        try:
            payload = jwt.decode(token, app.config.SECRET, algorithms=["HS256"])
            user_id = payload.get("user_id")
            session_id = payload.get("session_id")
            now = time.time()

            cached_auth = AUTH_CACHE.get(user_id)
            if cached_auth and cached_auth['session'] == session_id and cached_auth['expires'] > now:
                request.ctx.company_id = cached_auth['company_id']
            else:
                async with app.ctx.pool.acquire() as conn:
                    db_user = await conn.fetchrow(
                        "SELECT pus_session_id, pus_company_id FROM phc_users_t WHERE pus_user_id = $1", int(user_id)
                    )
                    if not db_user or db_user['pus_session_id'] != session_id:
                        return force_login_redirect()
                    
                    AUTH_CACHE[user_id] = {
                        'session': db_user['pus_session_id'], 
                        'company_id': db_user['pus_company_id'], 
                        'expires': now + 60
                    }
                    request.ctx.company_id = db_user['pus_company_id']

            request.ctx.user_id = user_id
            request.ctx.user_type = payload.get("user_type")
            request.ctx.username = payload.get("username", "User")
            request.ctx.csrf_token = payload.get("csrf_token")

        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return force_login_redirect()
        except asyncpg.PostgresError as e:
            print(f"DB Error during auth validation: {e}")
            if request.headers.get("HX-Request"):
                return response.json({"error": "Database Error."}, status=500)
            return response.text("Database Error", status=500)

        return await wrapped(request, *args, **kwargs)
    return decorator


# ==========================================
#  DATABASE LIFECYCLE & HELPERS
# ==========================================
@app.before_server_start
async def setup_db(app_instance, loop):
    try:
        dsn = CLOUD_DB_URL or f"postgres://postgres:{os.environ.get('LOCAL_DB_PASSWORD')}@localhost/tablesproj"
        app_instance.ctx.pool = await asyncpg.create_pool(dsn=dsn, statement_cache_size=0, min_size=2, max_size=20)

        async with app_instance.ctx.pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock(1337)")
            try:
                await _run_initial_migrations(conn)
            finally:
                await conn.execute("SELECT pg_advisory_unlock(1337)")
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")
        raise SystemExit("Fatal: Database initialization failed.")


@app.after_server_stop
async def close_db(app_instance, loop):
    if hasattr(app_instance.ctx, 'pool'):
        await app_instance.ctx.pool.close()


async def _run_initial_migrations(conn):
    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_companies_t WHERE pcp_company_id = 1001)"):
        await conn.execute("""
            INSERT INTO phc_companies_t 
            (pcp_company_id, pcp_company_code, pcp_company_name, pcp_created, pcp_modified, pcp_created_by, pcp_modified_by, pcp_status) 
            OVERRIDING SYSTEM VALUE 
            VALUES (1001, 'SYS', 'System Admin Company', NOW(), NOW(), 'System', 'System', 'ACT')
        """)

    if not await conn.fetchval("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='phc_users_t' AND column_name='pus_user_type')"):
        await conn.execute("ALTER TABLE phc_users_t ADD COLUMN pus_user_type VARCHAR(3) DEFAULT 'STD'")

    if not await conn.fetchval("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='phc_users_t' AND column_name='pus_session_id')"):
        await conn.execute("ALTER TABLE phc_users_t ADD COLUMN pus_session_id VARCHAR(255)")

    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_users_t WHERE pus_user_name = 'admin')"):
        hashed = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode('utf-8')
        await conn.execute("""
            INSERT INTO phc_users_t 
            (pus_company_id, pus_user_name, pus_full_name, pus_pwd, pus_status, pus_created, pus_modified, pus_created_by, pus_modified_by, pus_start_date, pus_user_type) 
            VALUES (1001, 'admin', 'System Admin', $1, 'ACT', NOW(), NOW(), 'System', 'System', NOW(), 'ADM')
        """, hashed)
    else:
        await conn.execute("UPDATE phc_users_t SET pus_user_type = 'ADM' WHERE pus_user_name = 'admin'")

    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_phc_users_username ON phc_users_t(pus_user_name);",
        "CREATE INDEX IF NOT EXISTS idx_phc_users_session ON phc_users_t(pus_session_id);",
        "CREATE INDEX IF NOT EXISTS idx_phc_role_assignment_user ON phc_user_roles_assignment_t(pua_user_id);",
        "CREATE INDEX IF NOT EXISTS idx_phc_role_screen_role ON phc_role_screen_assignment_t(prs_role_id);"
    ]
    for query in index_queries:
        try:
            await conn.execute(query)
        except asyncpg.exceptions.UndefinedTableError:
            pass


async def log_action(conn, user_id, action_desc):
    try:
        await conn.execute("""
            INSERT INTO phc_user_log_t 
            (pul_parent, pul_description, pul_created, pul_modified, pul_created_by, pul_modified_by) 
            VALUES ($1, $2, NOW(), NOW(), 'System', 'System')
        """, int(user_id) if user_id and str(user_id).isdigit() else None, action_desc)
    except Exception as e:
        pass


def make_human_readable(text):
    text = text.replace("phc_", "").replace("_t", "")
    if len(text) > 4 and text[3] == '_':
        text = text[4:]
    return text.replace("_", " ").title()


def get_column_sort_priority(pk_column, c_name):
    name = c_name.lower()
    if c_name == pk_column: return 0
    if name.endswith('code') or name.endswith('name') or name == 'dosage_form': return 1
    if name.endswith('status'): return 2
    if name.endswith('flag'): return 3
    if name in ('created_by', 'creation_date', 'last_update_date', 'last_updated_by', 'last_update_login') or \
       name.endswith('_created_by') or name.endswith('_modified_by') or \
       name.endswith('_created') or name.endswith('_modified'):
        return 100
    if 'start_date' in name: return 80
    if 'end_date' in name: return 81
    return 50

# ==========================================
#  CENTRALIZED CACHING & RESOLUTION ENGINES
# ==========================================

async def _get_cached_schema(conn, table_name):
    """ Centralized, exhaustive schema cache to eliminate redundant DB calls. """
    if table_name not in SCHEMA_CACHE["columns"]:
        cols = await conn.fetch("""
            SELECT column_name, data_type, character_maximum_length, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
        """, table_name)
        SCHEMA_CACHE["columns"][table_name] = [dict(c) for c in cols]
    return SCHEMA_CACHE["columns"][table_name]

async def get_allowed_tables(conn, user_id, user_type):
    if SCHEMA_CACHE["tables"] is None:
        rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%_t' OR table_name = 'phc_lookup_types'")
        SCHEMA_CACHE["tables"] = [r['table_name'] for r in rows]

    all_tables = SCHEMA_CACHE["tables"]
    ui_tables = [t for t in all_tables if not t.endswith('_assignment_t')]

    if user_type == 'ADM':
        return ui_tables

    now = time.time()
    cached_rbac = RBAC_CACHE.get(user_id)
    if cached_rbac and cached_rbac['expires'] > now:
        return cached_rbac['tables']

    query = """
        SELECT DISTINCT s.psn_screen_code as table_name
        FROM phc_user_roles_assignment_t ura
        JOIN phc_roles_t r ON ura.pua_role_id = r.prl_role_id
        JOIN phc_role_screen_assignment_t rsa ON r.prl_role_id = rsa.prs_role_id
        JOIN phc_screens_t s ON rsa.prs_screen_id = s.psn_screen_id
        WHERE ura.pua_user_id = $1
          AND r.prl_status = 'ACT'
          AND rsa.prs_status = 'ACT'
          AND s.psn_status = 'ACT'
    """
    assigned_rows = await conn.fetch(query, int(user_id))
    allowed_tables = [r['table_name'].strip().lower() for r in assigned_rows]
    final_tables = [t for t in ui_tables if t in allowed_tables]
    
    RBAC_CACHE[user_id] = {'tables': final_tables, 'expires': now + 300} 
    return final_tables

def mask_sensitive_data(col_name, val, user_type):
    if val is None or user_type == 'ADM': return val
    k_lower = col_name.lower()
    v_str = str(val)
    if 'pwd' in k_lower or 'password' in k_lower: return '********'
    if 'email' in k_lower and '@' in v_str:
        parts = v_str.split('@')
        return f"{parts[0][0]}***@{parts[1]}" if len(parts[0]) > 1 else f"***@{parts[1]}"
    if 'phone' in k_lower and len(v_str) >= 4: return f"***-***-{v_str[-4:]}"
    if 'account_number' in k_lower and len(v_str) >= 4: return f"****-****-{v_str[-4:]}"
    return val

async def get_pk_column(conn, table_name):
    if table_name not in SCHEMA_CACHE["pks"]:
        pk_row = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage kcu 
            JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name 
            WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
        """, table_name)
        SCHEMA_CACHE["pks"][table_name] = pk_row['column_name'] if pk_row else None
    return SCHEMA_CACHE["pks"][table_name]


async def get_dropdown_options(conn, column_name):
    col_lower = column_name.lower()
    if not col_lower.endswith('_id') and not col_lower.endswith('_code'):
        return None

    fk_query = """
        SELECT ccu.table_name AS target_table, ccu.column_name AS target_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND kcu.column_name = $1 LIMIT 1
    """
    fk_row = await conn.fetchrow(fk_query, column_name)
    
    if fk_row:
        target_table, pk_col = fk_row['target_table'], fk_row['target_column']
    else:
        target_table = None
        if 'company_id' in col_lower: target_table = 'phc_companies_t'
        elif 'dept_id' in col_lower: target_table = 'phc_dept_t'
        elif 'role_id' in col_lower: target_table = 'phc_roles_t'
        elif 'user_id' in col_lower: target_table = 'phc_users_t'
        elif 'product_id' in col_lower: target_table = 'cv_product_registration_t'
        elif 'equipment_id' in col_lower: target_table = 'cv_equipment_registration_t'
        else: return None
        pk_col = await get_pk_column(conn, target_table)

    if not target_table or not pk_col: return None

    cache_key = f"{target_table}_lookups"
    if cache_key in SCHEMA_CACHE["dropdown_lookups"]:
        cache_entry = SCHEMA_CACHE["dropdown_lookups"][cache_key]
        if time.time() - cache_entry['time'] < 3600: 
            return cache_entry['data']

    cols = await _get_cached_schema(conn, target_table)
    name_col = pk_col

    for r in cols:
        if r['column_name'] != pk_col and not r['column_name'].endswith('_id') and not r['column_name'].endswith('_by') and not r['column_name'].endswith('_code'):
            if r['data_type'] in ('character varying', 'text', 'varchar'):
                name_col = r['column_name']
                break

    try:
        rows = await conn.fetch(f"SELECT {pk_col} as id, {name_col} as name FROM {target_table} ORDER BY {name_col} ASC LIMIT 500")
        result = [dict(row) for row in rows]
        SCHEMA_CACHE["dropdown_lookups"][cache_key] = {'time': time.time(), 'data': result}
        return result
    except asyncpg.exceptions.UndefinedTableError:
        return None

# ==========================================
#  AUTH ROUTES
# ==========================================
@app.route('/login', methods=['GET', 'POST'])
async def handle_login(request):
    if request.method == "GET":
        return await render("login.html")
        
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM phc_users_t WHERE pus_user_name = $1", username)
        
        if user:
            stored_pwd = user['pus_pwd']
            is_valid = False
            loop = asyncio.get_running_loop()
            
            try:
                is_valid = await loop.run_in_executor(None, partial(bcrypt.checkpw, password.encode('utf-8'), stored_pwd.encode('utf-8')))
            except ValueError:
                if password == stored_pwd:
                    is_valid = True
                    new_hashed = await loop.run_in_executor(None, partial(bcrypt.hashpw, password.encode('utf-8'), bcrypt.gensalt()))
                    await conn.execute("UPDATE phc_users_t SET pus_pwd = $1 WHERE pus_user_id = $2", new_hashed.decode('utf-8'), user['pus_user_id'])
                    
            if is_valid:
                session_id = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", session_id, user['pus_user_id'])
                csrf_token = str(uuid.uuid4().hex)

                payload = {
                    "user_id": user['pus_user_id'],
                    "user_type": user.get('pus_user_type') or 'STD',
                    "username": user['pus_user_name'],
                    "session_id": session_id,
                    "csrf_token": csrf_token,
                    "exp": datetime.now(timezone.utc) + timedelta(hours=12)
                }
                token = jwt.encode(payload, app.config.SECRET, algorithm="HS256")
                
                resp = response.json({"status": "success"})
                resp.add_cookie("auth_token", token, httponly=True, samesite="Strict", secure=not is_development)
                await log_action(conn, user['pus_user_id'], "User logged in")
                return resp
        
        return response.json({"error": "Invalid credentials"}, status=401)


@app.route("/logout")
async def logout(request):
    if request.headers.get("HX-Request"):
        resp = response.text("Logging out...")
        resp.headers["HX-Redirect"] = "/login"
    else:
        resp = response.redirect("/login")
    resp.delete_cookie("auth_token")
    return resp

# ==========================================
#  MAIN ROUTES
# ==========================================
@app.route("/")
@login_required
async def dashboard(request):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        stats = {
            "emp_count": await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t") if 'phc_emp_t' in allowed else "🔒",
            "comp_count": await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status = 'ACT'") if 'phc_companies_t' in allowed else "🔒",
            "dept_count": await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t") if 'phc_dept_t' in allowed else "🔒",
            "app_count": await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t") if 'phc_apps_t' in allowed else "🔒"
        }
        
    dynamic_mapping = get_table_modules(allowed)
    return await render("dashboard.html", context={
        "stats": stats, "all_tables": allowed, "table_modules": dynamic_mapping, 
        "username": request.ctx.username, "user_id": request.ctx.user_id, "csrf_token": request.ctx.csrf_token 
    })


@app.get("/table/<table_name>")
@login_required
async def show_table(request, table_name):
    try:
        async with app.ctx.pool.acquire() as conn:
            allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
            if table_name not in allowed: return response.redirect("/")

            schema_cols = await _get_cached_schema(conn, table_name)
            columns = [{"raw": r['column_name'], "label": make_human_readable(r['column_name'])} for r in schema_cols]
            pk_column = await get_pk_column(conn, table_name)
            columns = sorted(columns, key=lambda c: get_column_sort_priority(pk_column, c['raw']))

            page, limit = int(request.args.get("page", 1)), 50
            offset = (page - 1) * limit
            search_query = request.args.get("q", "").strip()
            where_clauses, params = [], []

            company_col = next((c['column_name'] for c in schema_cols if c['column_name'].lower().endswith('company_id')), None)
            if company_col:
                params.append(request.ctx.company_id)
                where_clauses.append(f"{company_col} = $1")

            valid_search_types = ('character varying', 'text', 'varchar', 'integer', 'bigint', 'numeric')
            searchable_cols = [c['column_name'] for c in schema_cols if c.get('data_type') in valid_search_types]

            # Require minimum 2 chars to search to prevent wildcard scanning slowdowns
            if len(search_query) >= 2 and searchable_cols:
                search_param_idx = len(params) + 1
                params.append(f"%{search_query}%")
                cast_clauses = [f"CAST({col} AS TEXT) ILIKE ${search_param_idx}" for col in searchable_cols]
                where_clauses.append(f"({' OR '.join(cast_clauses)})")
                
            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            order_clause = f"ORDER BY {pk_column} DESC" if pk_column else "ORDER BY 1 DESC"
            
            total_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name} {where_sql}", *params)
            total_pages = max(1, (total_count + limit - 1) // limit)
            
            start_row = min(offset + 1, total_count) if total_count > 0 else 0
            end_row = min(offset + limit, total_count)

            rows = await conn.fetch(f"SELECT * FROM {table_name} {where_sql} {order_clause} LIMIT {limit} OFFSET {offset}", *params)
            rows_dict = [dict(r) for r in rows]

            # O(1) Dictionary Lookup for Performance
            for col in columns:
                c_name = col['raw']
                if c_name == pk_column or c_name == 'address_details': continue 

                options = await get_dropdown_options(conn, c_name)
                if options:
                    lookup = {str(opt['id']): opt['name'] for opt in options}
                    for row in rows_dict:
                        val = row.get(c_name)
                        if val is not None and str(val) in lookup:
                            row[c_name] = f"{lookup[str(val)]} (ID: {val})"
                        row[c_name] = mask_sensitive_data(c_name, row.get(c_name), request.ctx.user_type)

            dynamic_mapping = get_table_modules(allowed)

            return await render("table_view.html", context={
                "table_name": table_name, "table_title": make_human_readable(table_name), "columns": columns, 
                "rows": rows_dict, "all_tables": allowed, "table_modules": dynamic_mapping, "pk_column": pk_column, 
                "user_id": request.ctx.user_id, "username": request.ctx.username, "csrf_token": request.ctx.csrf_token,
                "search_query": search_query, "page": page, "total_pages": total_pages, "total_count": total_count,
                "start_row": start_row, "end_row": end_row
            })
    except Exception as e:
        print(f"Error loading table {table_name}: {e}")
        if request.headers.get("HX-Request"): return response.json({"error": "Table could not be loaded"}, status=500)
        return response.redirect("/")


@app.route("/export/<table_name>")
@login_required
async def export_csv(request, table_name):
    pk_id = request.args.get("id")
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.text("Unauthorized", status=403)

        schema_cols = await _get_cached_schema(conn, table_name)
        where_clauses, params = [], []
        pk_column = await get_pk_column(conn, table_name)

        company_col = next((c['column_name'] for c in schema_cols if c['column_name'].lower().endswith('company_id')), None)
        if company_col:
            params.append(request.ctx.company_id)
            where_clauses.append(f"{company_col} = $1")

        if pk_id and pk_column:
            pk_type = next((r['data_type'] for r in schema_cols if r['column_name'] == pk_column), 'integer')
            parsed_pk = pk_id if pk_type in ('character varying', 'text', 'varchar') else int(pk_id)
            search_param_idx = len(params) + 1
            params.append(parsed_pk)
            where_clauses.append(f"{pk_column} = ${search_param_idx}")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        rows = await conn.fetch(f"SELECT * FROM {table_name} {where_sql}", *params)
        if not rows: return response.text("No data found")
        
        rows_dict = [dict(r) for r in rows]
        sorted_cols = sorted(schema_cols, key=lambda c: get_column_sort_priority(pk_column, c['column_name']))

        for r in sorted_cols:
            c_name = r['column_name']
            if c_name == 'address_details': continue
            options = await get_dropdown_options(conn, c_name)
            if options:
                lookup = {str(opt['id']): opt['name'] for opt in options}
                for row in rows_dict:
                    val = row.get(c_name)
                    if val is not None and str(val) in lookup: row[c_name] = f"{lookup[str(val)]} (ID: {val})"
                    row[c_name] = mask_sensitive_data(c_name, row.get(c_name), request.ctx.user_type)

        output = io.StringIO()
        writer = csv.writer(output)
        ordered_keys = [c['column_name'] for c in sorted_cols]
        writer.writerow(ordered_keys)
        for row in rows_dict: writer.writerow([row.get(k) for k in ordered_keys])

        return response.text(output.getvalue(), headers={"Content-Disposition": f'attachment; filename="{table_name}_export.csv"', "Content-Type": "text/csv"})


@app.get("/api/lookup_values/<type_code>")
@login_required
async def get_lookup_values_api(request, type_code):
    async with app.ctx.pool.acquire() as conn:
        try:
            rows = await conn.fetch("SELECT * FROM phc_lookup_values_t WHERE plv_lookup_type_code = $1 ORDER BY plv_created ASC", type_code)
            html_parts = [f"""
            <div style="background: rgba(0,0,0,0.4); padding: 12px 20px; border-bottom: 1px solid var(--glass-border); display: flex; justify-content: space-between; align-items: center;">
                <span style="font-family: var(--font-mono); font-size: 12px; color: var(--color-fog); text-transform: uppercase; letter-spacing: 0.05em;">2. Assigned Values for <span style="color: var(--color-frost-link);">{type_code}</span></span>
                <a href="/new/phc_lookup_values_t" class="action-btn" style="background: var(--color-electric-iris); color: white; border: none; padding: 4px 12px; border-radius: 4px; font-weight: 500; font-size: 13px; text-decoration: none;">+ Add Value</a>
            </div>
            <div class="table-scroll" style="flex-grow: 1; max-height: none;">
                <table style="animation: fadeIn 0.3s ease;">
                    <thead><tr><th style="width: 50px;"></th><th>Value Code</th><th>Display Name</th><th>Description</th><th>Status</th></tr></thead>
                    <tbody>
            """]
            if not rows: html_parts.append('<tr><td colspan="5" style="text-align: center; padding: 40px; color: var(--color-fog);">No values assigned to this lookup type yet.</td></tr>')
            else:
                for row in rows:
                    status = '<span style="color: var(--color-cipher-mint); background: rgba(38,150,132,0.15); padding: 4px 8px; border-radius: 4px; font-size: 12px;">Active</span>' if row['plv_status'] == 'ACT' else '<span style="color: var(--color-ember); background: rgba(228,109,76,0.15); padding: 4px 8px; border-radius: 4px; font-size: 12px;">Inactive</span>'
                    html_parts.append(f"""
                    <tr>
                        <td style="text-align: center;"><a href="/edit/phc_lookup_values_t/{row['plv_lookup_value_code']}" class="action-btn" style="background: rgba(255,255,255,0.05); color: var(--color-fog); padding: 4px 8px; border-radius: 4px; border: 1px solid var(--glass-border); text-decoration: none; font-size: 12px;">Edit</a></td>
                        <td style="font-family: var(--font-mono); color: var(--color-pebble);">{row['plv_lookup_value_code']}</td>
                        <td style="font-weight: 500; color: var(--color-glacier);">{row['plv_lookup_value_name'] or ''}</td>
                        <td style="color: var(--color-moonlight);">{row['plv_lookup_value_desc'] or ''}</td>
                        <td>{status}</td>
                    </tr>""")
            html_parts.append("</tbody></table></div>")
            return response.html("".join(html_parts))
        except Exception as e:
            return response.html('<div style="padding: 20px; color: var(--color-ember);">Error loading details.</div>')


@app.get("/edit/<table_name>/<pk_val>")
@login_required
async def show_edit_form(request, table_name, pk_val):
    try:
        async with app.ctx.pool.acquire() as conn:
            allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
            if table_name not in allowed: return response.redirect("/")

            pk_column = await get_pk_column(conn, table_name)
            col_rows = await _get_cached_schema(conn, table_name)
            pk_type = next((r['data_type'] for r in col_rows if r['column_name'] == pk_column), 'integer')
            parsed_pk = str(pk_val) if pk_type in ('character varying', 'text', 'varchar') else int(pk_val)
            
            # SECURE IDOR FIX: Verify Tenant on Fetch
            company_col = next((c['column_name'] for c in col_rows if c['column_name'].lower().endswith('company_id')), None)
            if company_col:
                record = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1 AND {company_col} = $2", parsed_pk, request.ctx.company_id)
                if not record: return response.text("Unauthorized or Record Not Found", status=403)
            else:
                record = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", parsed_pk)
                
            columns = []
            for r in col_rows:
                c_name = r['column_name']
                if c_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by')) or c_name.lower() in WHO_COLS: continue
                is_pk = (c_name == pk_column)
                
                if r['data_type'] in ('json', 'jsonb'):
                    val = record[c_name]
                    if isinstance(val, str):
                        try: val = json.loads(val)
                        except Exception: val = {}
                    val = json.dumps(val) if val else "{}"
                else:
                    val = mask_sensitive_data(c_name, record[c_name], request.ctx.user_type)
                    if isinstance(val, (date, datetime)): val = val.strftime('%Y-%m-%d')

                options = await get_dropdown_options(conn, c_name) if not is_pk else None
                is_req = (r['is_nullable'] == 'NO') and not is_pk
                columns.append({"column_name": c_name, "label": make_human_readable(c_name), "required": is_req, "value": val, "data_type": r['data_type'], "options": options, "is_pk": is_pk})

            columns = sorted(columns, key=lambda c: get_column_sort_priority(pk_column, c['column_name']))

            if table_name == 'phc_roles_t':
                screens = await conn.fetch("SELECT psn_screen_id as id, psn_screen_code as code, psn_screen_name as name FROM phc_screens_t WHERE psn_status = 'ACT'")
                assignments = await conn.fetch("SELECT prs_screen_id FROM phc_role_screen_assignment_t WHERE prs_role_id = $1", parsed_pk)
                assigned_val = ",".join([str(a['prs_screen_id']) for a in assignments])
                columns.append({"column_name": "pr_allowed_tables", "label": "Assigned Screens", "required": False, "value": assigned_val, "data_type": "virtual_checkbox", "options": [dict(r) for r in screens], "is_pk": False})

            if table_name == 'phc_users_t':
                roles = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status = 'ACT'")
                assignments = await conn.fetch("SELECT pua_role_id FROM phc_user_roles_assignment_t WHERE pua_user_id = $1", parsed_pk)
                assigned_val = ",".join([str(a['pua_role_id']) for a in assignments])
                columns.append({"column_name": "pu_assigned_roles", "label": "Assigned Roles", "required": False, "value": assigned_val, "data_type": "virtual_checkbox", "options": [dict(r) for r in roles], "is_pk": False})

            return await render("form_view.html", context={
                "table_name": table_name, "table_title": f"Edit {make_human_readable(table_name)}", "columns": columns, 
                "all_tables": allowed, "table_modules": get_table_modules(allowed), "pk_val": pk_val, "mode": "edit", 
                "user_id": request.ctx.user_id, "username": request.ctx.username, "csrf_token": request.ctx.csrf_token, "current_user_type": request.ctx.user_type
            })
    except Exception as e:
        if request.headers.get("HX-Request"): return response.json({"error": "Error loading record."}, status=500)
        return response.redirect(f"/table/{table_name}")


@app.get("/new/<table_name>")
@login_required
async def show_add_form(request, table_name):
    try:
        async with app.ctx.pool.acquire() as conn:
            allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
            if table_name not in allowed: return response.redirect("/")

            col_rows = await _get_cached_schema(conn, table_name)
            pk_column = await get_pk_column(conn, table_name)

            columns = []
            for r in col_rows:
                c_name = r['column_name']
                if c_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by')) or c_name.lower() in WHO_COLS: continue
                is_pk = (c_name == pk_column)
                options = await get_dropdown_options(conn, c_name) if not is_pk else None
                is_req = (r['is_nullable'] == 'NO') and not is_pk
                
                val = ""
                if ('date' in r['data_type'] or 'timestamp' in r['data_type']) and 'end' not in c_name.lower() and not is_pk:
                    val = datetime.now().strftime('%Y-%m-%d')
                if r['data_type'] in ('json', 'jsonb'): val = "{}"
                    
                columns.append({"column_name": c_name, "label": make_human_readable(c_name), "required": is_req, "value": val, "data_type": r['data_type'], "options": options, "is_pk": is_pk})

            columns = sorted(columns, key=lambda c: get_column_sort_priority(pk_column, c['column_name']))

            if table_name == 'phc_roles_t':
                screens = await conn.fetch("SELECT psn_screen_id as id, psn_screen_code as code, psn_screen_name as name FROM phc_screens_t WHERE psn_status = 'ACT'")
                columns.append({"column_name": "pr_allowed_tables", "label": "Assigned Screens", "required": False, "value": "", "data_type": "virtual_checkbox", "options": [dict(r) for r in screens], "is_pk": False})

            if table_name == 'phc_users_t':
                roles = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status = 'ACT'")
                columns.append({"column_name": "pu_assigned_roles", "label": "Assigned Roles", "required": False, "value": "", "data_type": "virtual_checkbox", "options": [dict(r) for r in roles], "is_pk": False})

            return await render("form_view.html", context={
                "table_name": table_name, "table_title": f"New {make_human_readable(table_name)}", "columns": columns, 
                "all_tables": allowed, "table_modules": get_table_modules(allowed), "mode": "create", 
                "user_id": request.ctx.user_id, "username": request.ctx.username, "csrf_token": request.ctx.csrf_token, "current_user_type": request.ctx.user_type
            })
    except Exception as e:
        if request.headers.get("HX-Request"): return response.json({"error": "Error creating record."}, status=500)
        return response.redirect(f"/table/{table_name}")

async def _sanitize_payload(data, schema_map, pk_column, current_user_id, request_method):
    clean_data = {}
    loop = asyncio.get_running_loop()

    virtual_screens = data.pop("pr_allowed_tables", None)
    if isinstance(virtual_screens, list): data["pr_allowed_tables"] = ",".join(virtual_screens)
    virtual_roles = data.pop("pu_assigned_roles", None)
    if isinstance(virtual_roles, list): data["pu_assigned_roles"] = ",".join(virtual_roles)

    for r in schema_map.values():
        c_name = r['column_name']
        if not data.get(c_name) and ('date' in r['data_type'] or 'timestamp' in r['data_type']):
            if 'end' not in c_name.lower(): data[c_name] = datetime.now().strftime('%Y-%m-%d')

    for k, v in data.items():
        if v in ("", None) or k == pk_column: continue
        if k.lower().endswith(('_created', '_modified', '_created_by', '_modified_by')) or k.lower() in WHO_COLS: continue

        if k == 'pus_pwd':
            salt = bcrypt.gensalt()
            v = (await loop.run_in_executor(None, partial(bcrypt.hashpw, v.encode('utf-8'), salt))).decode('utf-8')

        col_info = schema_map.get(k, {})
        target_type = col_info.get('data_type', '').lower()
        max_len = col_info.get('character_maximum_length')
        
        if target_type in ('json', 'jsonb'):
            if isinstance(v, str):
                try: v = json.loads(v)
                except Exception: v = {}
            clean_data[k] = json.dumps(v)
            continue

        if 'date' in target_type or 'timestamp' in target_type or (isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-'):
            if isinstance(v, str) and v:
                try: v = datetime.strptime(v, '%Y-%m-%d')
                except ValueError:
                    try: v = datetime.fromisoformat(v)
                    except ValueError: pass

        if isinstance(v, str) and max_len is not None:
            if len(v) > max_len:
                if "status" in k and v.lower() == "active": v = "ACT"
                elif "status" in k and v.lower() == "inactive": v = "INA"
                else: v = v[:max_len]

        if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str) and v.strip().isdigit(): clean_data[k] = int(v)
        else: clean_data[k] = v

    for col_name in schema_map:
        c_lower = col_name.lower()
        if c_lower.endswith(('_created_by', '_modified_by', 'created_by', 'last_updated_by')):
            target_type = schema_map.get(col_name, {}).get('data_type', '').lower()
            clean_data[col_name] = int(current_user_id) if target_type in ('integer', 'bigint', 'numeric', 'smallint') and str(current_user_id).isdigit() else str(current_user_id)
        if request_method == "POST" and (c_lower.endswith('_created') or c_lower == 'creation_date'): clean_data[col_name] = datetime.now()
        if c_lower.endswith('_modified') or c_lower == 'last_update_date': clean_data[col_name] = datetime.now()

    return clean_data


@app.post("/api/<table_name>", name="create_row")
@app.put("/api/<table_name>/<pk_val>", name="update_row")
@login_required
async def save_data(request, table_name, pk_val=None):
    if request.headers.get("X-CSRFToken") != request.ctx.csrf_token: return response.json({"error": "Invalid CSRF"}, status=403)

    data = request.json
    current_user_id, user_type, company_id = request.ctx.user_id, request.ctx.user_type, int(request.ctx.company_id)

    if user_type != 'ADM':
        for key in ['pus_user_type', 'pus_status', 'pus_company_id', 'pu_assigned_roles', 'pr_allowed_tables']: data.pop(key, None)
        if table_name in ['phc_roles_t', 'phc_screens_t', 'phc_role_screen_assignment_t', 'phc_user_roles_assignment_t', 'phc_companies_t']: return response.json({"error": "Unauthorized"}, status=403)
        if table_name == 'phc_users_t' and request.method == "PUT":
            if str(pk_val) != str(current_user_id): return response.json({"error": "Unauthorized"}, status=403)
            data.pop('pus_pwd', None) 

    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, current_user_id, user_type)
        if table_name not in allowed: return response.json({"error": "Unauthorized"}, status=403)

        virtual_screens, virtual_roles = data.pop("pr_allowed_tables", None), data.pop("pu_assigned_roles", None)
        pk_column = await get_pk_column(conn, table_name)
        schema_rows = await _get_cached_schema(conn, table_name)
        schema_map = {r['column_name']: r for r in schema_rows}
        company_col = next((k for k in schema_map.keys() if k.lower().endswith('company_id')), None)
        
        clean_data = await _sanitize_payload(data, schema_map, pk_column, current_user_id, request.method)

        async with conn.transaction():
            if request.method == "POST":
                if company_col: clean_data[company_col] = company_id
                max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                target_id = (int(max_val) + 1) if max_val else 1
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                clean_data[pk_column] = str(target_id) if pk_type in ('character varying', 'text', 'varchar') else target_id
                
                cols, vals = list(clean_data.keys()), list(clean_data.values())
                placeholders = ', '.join([f"${i+1}::jsonb" if schema_map.get(c, {}).get('data_type') in ('json', 'jsonb') else f"${i+1}" for i, c in enumerate(cols)])
                await conn.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})", *vals)
            else:
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                target_id = str(pk_val) if pk_type in ('character varying', 'text', 'varchar') else int(pk_val)
                
                set_clauses = [f"{k} = ${i+2}::jsonb" if schema_map.get(k, {}).get('data_type') in ('json', 'jsonb') else f"{k} = ${i+2}" for i, k in enumerate(clean_data.keys())]
                vals = list(clean_data.values())
                
                if set_clauses:
                    if company_col:
                        vals.append(company_id)
                        await conn.execute(f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = $1 AND {company_col} = ${len(vals)}", target_id, *vals)
                    else:
                        await conn.execute(f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = $1", target_id, *vals)

            # Role/Screen assignments logic remains same
            if table_name == 'phc_roles_t' and virtual_screens is not None:
                await conn.execute("DELETE FROM phc_role_screen_assignment_t WHERE prs_role_id = $1", target_id)
                if virtual_screens:
                    prs_pk_col = await get_pk_column(conn, 'phc_role_screen_assignment_t')
                    max_prs = await conn.fetchval(f"SELECT MAX({prs_pk_col}) FROM phc_role_screen_assignment_t") if prs_pk_col else 0
                    next_prs = (int(max_prs) + 1) if max_prs else 1
                    for s_id in (virtual_screens if isinstance(virtual_screens, list) else str(virtual_screens).split(',')):
                        if str(s_id).strip() and prs_pk_col:
                            await conn.execute(f"INSERT INTO phc_role_screen_assignment_t ({prs_pk_col}, prs_company_id, prs_role_id, prs_screen_id, prs_start_date, prs_status, prs_created_by, prs_modified_by, prs_created, prs_modified) VALUES ($1, $2, $3, $4, CURRENT_DATE, 'ACT', $5, $5, NOW(), NOW())", next_prs, company_id, target_id, int(str(s_id).strip()), str(current_user_id))
                            next_prs += 1

            if table_name == 'phc_users_t' and virtual_roles is not None:
                await conn.execute("DELETE FROM phc_user_roles_assignment_t WHERE pua_user_id = $1", target_id)
                if virtual_roles:
                    pua_pk_col = await get_pk_column(conn, 'phc_user_roles_assignment_t')
                    max_pua = await conn.fetchval(f"SELECT MAX({pua_pk_col}) FROM phc_user_roles_assignment_t") if pua_pk_col else 0
                    next_pua = (int(max_pua) + 1) if max_pua else 1
                    for r_id in (virtual_roles if isinstance(virtual_roles, list) else str(virtual_roles).split(',')):
                        if str(r_id).strip() and pua_pk_col:
                            await conn.execute(f"INSERT INTO phc_user_roles_assignment_t ({pua_pk_col}, pua_company_id, pua_user_id, pua_role_id, pua_start_date, pua_status, pua_created_by, pua_modified_by, pua_created, pua_modified) VALUES ($1, $2, $3, $4, CURRENT_DATE, 'ACT', $5, $5, NOW(), NOW())", next_pua, company_id, target_id, int(str(r_id).strip()), str(current_user_id))
                            next_pua += 1

        if request.headers.get("HX-Request"):
            return response.html(f"<script>sessionStorage.setItem('pendingToast', JSON.stringify({{\"msg\": \"Record saved successfully!\", \"type\": \"success\"}})); localStorage.removeItem('draft_{table_name}'); window.location.href = '/table/{table_name}';</script>")
        return response.json({"status": "success"})


@app.delete("/api/<table_name>/<pk_val>", name="delete_row")
@login_required
async def delete_data(request, table_name, pk_val):
    if request.headers.get("X-CSRFToken") != request.ctx.csrf_token: return response.json({"error": "Invalid CSRF Token."}, status=403)

    current_user_id, user_type, company_id = request.ctx.user_id, request.ctx.user_type, int(request.ctx.company_id)
    
    if user_type != 'ADM':
        if table_name in ['phc_roles_t', 'phc_screens_t', 'phc_role_screen_assignment_t', 'phc_user_roles_assignment_t', 'phc_companies_t']: return response.json({"error": "Unauthorized"}, status=403)
        if table_name == 'phc_users_t' and str(pk_val) != str(current_user_id): return response.json({"error": "Unauthorized"}, status=403)

    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, current_user_id, user_type)
        if table_name not in allowed: return response.json({"error": "Unauthorized API Access"}, status=403)

        pk_column = await get_pk_column(conn, table_name)
        schema_rows = await _get_cached_schema(conn, table_name)
        pk_type = next((r['data_type'] for r in schema_rows if r['column_name'] == pk_column), 'integer')
        target_id = str(pk_val) if pk_type in ('character varying', 'text', 'varchar') else int(pk_val)

        company_col = next((c['column_name'] for c in schema_rows if c['column_name'].lower().endswith('company_id')), None)
        status_col = next((c['column_name'] for c in schema_rows if c['column_name'].lower().endswith('status')), None)

        async with conn.transaction():
            where_clause, params = f"WHERE {pk_column} = $1", [target_id]
            if company_col:
                where_clause += " AND " + f"{company_col} = $2"
                params.append(company_id)

            if table_name in ['phc_role_screen_assignment_t', 'phc_user_roles_assignment_t']:
                res = await conn.execute(f"DELETE FROM {table_name} {where_clause}", *params)
                if res == "DELETE 0": return response.json({"error": "Record not found or Tenant violation."}, status=403)
                msg = "Record deleted permanently."
            elif status_col:
                res = await conn.execute(f"UPDATE {table_name} SET {status_col} = 'INA' {where_clause}", *params)
                if res == "UPDATE 0": return response.json({"error": "Record not found or Tenant violation."}, status=403)
                msg = "Record successfully archived (Soft Delete)."
            else:
                return response.json({"error": "Hard deletions disabled. Table lacks a status column."}, status=403)

            await log_action(conn, current_user_id, f"Archived/Deleted record {target_id} from {table_name}")

        if request.headers.get("HX-Request"): return response.html(f'<div id="toast-container" hx-swap-oob="beforeend"><div class="toast" style="animation: slideIn Toast 0.4s ease, fadeOutToast 0.4s ease 3.5s forwards;"><i data-lucide="check-circle-2" style="color: var(--color-cipher-mint)"></i> {msg}</div></div>')
        return response.json({"status": "success"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)
