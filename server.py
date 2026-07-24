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
        'phc_lookup_values_t': 'MasterData',
        'phc_plant_master': 'MasterData',
        'phc_plant_compliance': 'MasterData',
        'phc_certifications': 'MasterData',
        'phc_plant_equipment': 'MasterData',
        'phc_equipment_locations': 'MasterData',
        'phc_material_group_master': 'MasterData',
        'phc_material_master': 'MasterData',
        'phc_uom_master': 'MasterData',
        'phc_uom_conversion': 'MasterData',
        'phc_prod_master': 'MasterData',
        'phc_prod_lifecycle_history': 'MasterData',
        'phc_prod_alt_names': 'MasterData'
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
    if len(ip_tracker) > 10000: ip_tracker.clear()
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

        return await wrapped(request, *args, **kwargs)
    return decorator

# ==========================================
#  DATABASE LIFECYCLE
# ==========================================
@app.before_server_start
async def setup_db(app_instance, loop):
    try:
        dsn = CLOUD_DB_URL or f"postgres://postgres:{os.environ.get('LOCAL_DB_PASSWORD')}@localhost/tablesproj"
        app_instance.ctx.pool = await asyncpg.create_pool(dsn=dsn, statement_cache_size=0, min_size=2, max_size=20)
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")
        raise SystemExit("Fatal: Database initialization failed.")

@app.after_server_stop
async def close_db(app_instance, loop):
    if hasattr(app_instance.ctx, 'pool'):
        await app_instance.ctx.pool.close()

# ==========================================
#  HELPERS & CACHING
# ==========================================
def make_human_readable(text):
    text = text.replace("phc_", "").replace("_t", "")
    if len(text) > 4 and text[3] == '_': text = text[4:]
    return text.replace("_", " ").title()

def get_column_sort_priority(pk_column, c_name):
    name = c_name.lower()
    if c_name == pk_column: return 0
    if name.endswith('code') or name.endswith('name') or name == 'dosage_form': return 1
    if name.endswith('status'): return 2
    if name.endswith('flag'): return 3
    if name in ('created_by', 'creation_date', 'last_update_date', 'last_updated_by', 'last_update_login') or name.endswith('_created_by') or name.endswith('_modified_by') or name.endswith('_created') or name.endswith('_modified'): return 100
    if 'start_date' in name: return 80
    if 'end_date' in name: return 81
    return 50

async def _get_cached_schema(conn, table_name):
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
        rows = await conn.fetch("SELECT psn_screen_code as table_name FROM phc_screens_t WHERE psn_status = 'ACT'")
        SCHEMA_CACHE["tables"] = [r['table_name'] for r in rows]

    all_tables = SCHEMA_CACHE["tables"]
    ui_tables = [t for t in all_tables if not t.endswith('_assignment_t')]

    if user_type == 'ADM': return ui_tables

    now = time.time()
    cached_rbac = RBAC_CACHE.get(user_id)
    if cached_rbac and cached_rbac['expires'] > now: return cached_rbac['tables']

    query = """
        SELECT DISTINCT s.psn_screen_code as table_name
        FROM phc_user_roles_assignment_t ura
        JOIN phc_roles_t r ON ura.pua_role_id = r.prl_role_id
        JOIN phc_role_screen_assignment_t rsa ON r.prl_role_id = rsa.prs_role_id
        JOIN phc_screens_t s ON rsa.prs_screen_id = s.psn_screen_id
        WHERE ura.pua_user_id = $1 AND r.prl_status = 'ACT' AND rsa.prs_status = 'ACT' AND s.psn_status = 'ACT'
    """
    assigned_rows = await conn.fetch(query, int(user_id))
    allowed_tables = [r['table_name'].strip().lower() for r in assigned_rows]
    final_tables = [t for t in ui_tables if t in allowed_tables]
    RBAC_CACHE[user_id] = {'tables': final_tables, 'expires': now + 300} 
    return final_tables

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
    
    try:
        lov_rows = await conn.fetch("""
            SELECT plv_lookup_value_code as id, plv_lookup_value_name as name 
            FROM phc_lookup_values_t 
            WHERE UPPER(plv_lookup_type_code) = $1 AND plv_status = 'ACT'
            ORDER BY plv_lookup_value_name ASC
        """, col_lower.upper())
        if lov_rows: return [dict(row) for row in lov_rows]
    except asyncpg.exceptions.UndefinedTableError:
        pass 

    if not col_lower.endswith('_id') and not col_lower.endswith('_code') and not col_lower.endswith('_by'): return None

    fk_row = await conn.fetchrow("""
        SELECT ccu.table_name AS target_table, ccu.column_name AS target_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND kcu.column_name = $1 LIMIT 1
    """, column_name)
    
    target_table, pk_col = None, None
    if fk_row:
        target_table, pk_col = fk_row['target_table'], fk_row['target_column']
    else:
        fallback_map = {
            'company_id': 'phc_companies_t',
            'dept_id': 'phc_dept_t',
            'role_id': 'phc_roles_t',
            'user_id': 'phc_users_t',
            'created_by': 'phc_users_t',
            'modified_by': 'phc_users_t',
            'last_updated_by': 'phc_users_t',
            'plant_id': 'phc_plant_master',
            'org_id': 'phc_orgs_t',
            'location_id': 'pmd_locations_t',
            'equipment_id': 'phc_plant_equipment',
            'material_group_id': 'phc_material_group_master',
            'material_id': 'phc_material_master',
            'base_uom_id': 'phc_uom_master',
            'alt_uom_id': 'phc_uom_master',
            'uom_id': 'phc_uom_master',
            'product_id': 'phc_prod_master',
            'cost_center_id': 'phc_cost_center_t',
            'services_id': 'phc_services_t'
        }
        for key, t_name in fallback_map.items():
            if key in col_lower:
                target_table = t_name
                break
        if target_table: pk_col = await get_pk_column(conn, target_table)

    if not target_table or not pk_col: return None
    try:
        exists = await conn.fetchval("SELECT to_regclass($1)", target_table)
        if not exists: return None
    except Exception: return None

    cache_key = f"{target_table}_lookups"
    if cache_key in SCHEMA_CACHE["dropdown_lookups"]:
        cache_entry = SCHEMA_CACHE["dropdown_lookups"][cache_key]
        if time.time() - cache_entry['time'] < 3600: return cache_entry['data']

    cols = await _get_cached_schema(conn, target_table)
    name_col = pk_col
    for r in cols:
        c_name = r['column_name']
        if c_name != pk_col and r['data_type'] in ('character varying', 'text', 'varchar') and 'name' in c_name.lower():
            name_col = c_name
            break
            
    if name_col == pk_col:
        for r in cols:
            c_name = r['column_name']
            if c_name != pk_col and r['data_type'] in ('character varying', 'text', 'varchar') and not c_name.endswith('_id') and not c_name.endswith('_by'):
                name_col = c_name
                break

    try:
        rows = await conn.fetch(f"SELECT {pk_col} as id, {name_col} as name FROM {target_table} ORDER BY {name_col} ASC LIMIT 500")
        result = [dict(row) for row in rows]
        SCHEMA_CACHE["dropdown_lookups"][cache_key] = {'time': time.time(), 'data': result}
        return result
    except asyncpg.exceptions.UndefinedTableError: return None

async def _sanitize_payload(data, schema_map, pk_column, current_user_id, current_username, request_method):
    clean_data = {}
    loop = asyncio.get_running_loop()

    virtual_screens = data.pop("pr_allowed_tables", None)
    if isinstance(virtual_screens, list): data["pr_allowed_tables"] = ",".join(virtual_screens)
    virtual_roles = data.pop("pu_assigned_roles", None)
    if isinstance(virtual_roles, list): data["pu_assigned_roles"] = ",".join(virtual_roles)

    pk_type = schema_map.get(pk_column, {}).get('data_type', '')
    is_string_pk = pk_type in ('character varying', 'text', 'varchar')

    for r in schema_map.values():
        c_name = r['column_name']
        if not data.get(c_name) and ('date' in r['data_type'] or 'timestamp' in r['data_type']):
            if 'end' not in c_name.lower(): data[c_name] = datetime.now().strftime('%Y-%m-%d')

    for k, v in data.items():
        if v in ("", None, "Pending..."): continue
        if k == pk_column and not is_string_pk: continue
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
            if target_type in ('integer', 'bigint', 'numeric', 'smallint'):
                clean_data[col_name] = int(current_user_id) if str(current_user_id).isdigit() else None
            else:
                clean_data[col_name] = current_username
        if request_method == "POST" and (c_lower.endswith('_created') or c_lower == 'creation_date'): clean_data[col_name] = datetime.now()
        if c_lower.endswith('_modified') or c_lower == 'last_update_date': clean_data[col_name] = datetime.now()

    return clean_data


# ==========================================
#  FRONTEND RENDERING ROUTES (HTML)
# ==========================================

@app.get("/login")
async def login_page(request):
    return await render("login.html")

@app.post("/login")
async def handle_login(request):
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM phc_users_t WHERE pus_user_name = $1", username)
        
        if user:
            stored_pwd = user['pus_pwd']
            is_valid = False
            try:
                if bcrypt.checkpw(password.encode('utf-8'), stored_pwd.encode('utf-8')): is_valid = True
            except ValueError:
                if password == stored_pwd: is_valid = True
                    
            if is_valid:
                session_id = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", session_id, user['pus_user_id'])
                token = jwt.encode({
                    "user_id": user['pus_user_id'], "username": user['pus_user_name'],
                    "user_type": user.get('pus_user_type', 'STD'), "session_id": session_id,
                    "csrf_token": str(uuid.uuid4())
                }, app.config.SECRET, algorithm="HS256")
                
                resp = response.json({"status": "success", "message": "Login successful"})
                resp.cookies["auth_token"] = token
                resp.cookies["auth_token"]["httponly"] = True
                resp.cookies["auth_token"]["samesite"] = "Lax"
                return resp
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.get("/logout")
async def logout(request):
    resp = response.redirect("/login")
    resp.delete_cookie("auth_token")
    return resp

@app.get("/")
@login_required
async def dashboard(request):
    async with app.ctx.pool.acquire() as conn:
        # FIX: Fetch allowed tables FIRST so the SCHEMA_CACHE is guaranteed to be populated!
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        all_db_tables = SCHEMA_CACHE.get("tables") or []
        
        emp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t WHERE pem_status='ACT'") if 'phc_emp_t' in all_db_tables else 0
        comp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status='ACT'") if 'phc_companies_t' in all_db_tables else 0
        dept_count = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t WHERE pdp_status='ACT'") if 'phc_dept_t' in all_db_tables else 0
        app_count = await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t WHERE pap_status='ACT'") if 'phc_apps_t' in all_db_tables else 0
        
        return await render("dashboard.html", context={
            "username": request.ctx.username, "user_id": request.ctx.user_id,
            "stats": {"emp_count": emp_count, "comp_count": comp_count, "dept_count": dept_count, "app_count": app_count},
            "all_tables": allowed, "table_modules": get_table_modules(allowed), "csrf_token": request.ctx.csrf_token
        })

@app.get("/table/<table_name>")
@login_required
async def show_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.redirect("/")
        
        pk_column = await get_pk_column(conn, table_name)
        schema = await _get_cached_schema(conn, table_name)
        
        search_query = request.args.get("q", "").strip()
        page = int(request.args.get("page", 1))
        limit = 50
        offset = (page - 1) * limit
        
        where_clauses, params = [], []
        if search_query:
            text_cols = [c['column_name'] for c in schema if c['data_type'] in ('character varying', 'text', 'varchar')]
            if text_cols:
                clauses = [f"{c} ILIKE ${i+1}" for i, c in enumerate(text_cols)]
                where_clauses.append("(" + " OR ".join(clauses) + ")")
                params = [f"%{search_query}%"] * len(text_cols)
                
        where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name} {where_str}", *params)
        order_by = f"ORDER BY {pk_column} DESC" if pk_column else ""
        
        rows = await conn.fetch(f"SELECT * FROM {table_name} {where_str} {order_by} LIMIT {limit} OFFSET {offset}", *params)
        columns = [{"raw": c['column_name'], "label": make_human_readable(c['column_name'])} for c in schema]
        columns.sort(key=lambda x: get_column_sort_priority(pk_column, x['raw']))

        if table_name == 'phc_lookup_values_t':
            return await render("table_view.html", context={
                "table_name": table_name, "table_title": "Enterprise Lookups",
                "rows": [], "columns": columns, "pk_column": pk_column,
                "all_tables": allowed, "table_modules": get_table_modules(allowed),
                "username": request.ctx.username, "user_id": request.ctx.user_id, "csrf_token": request.ctx.csrf_token
            })

        return await render("table_view.html", context={
            "table_name": table_name, "table_title": make_human_readable(table_name),
            "rows": [dict(r) for r in rows], "columns": columns, "pk_column": pk_column,
            "page": page, "total_pages": max(1, (total_count + limit - 1) // limit), "total_count": total_count,
            "start_row": offset + 1 if total_count else 0, "end_row": min(offset + limit, total_count),
            "search_query": search_query, "all_tables": allowed, "table_modules": get_table_modules(allowed),
            "username": request.ctx.username, "user_id": request.ctx.user_id, "csrf_token": request.ctx.csrf_token
        })

@app.get("/new/<table_name>", name="new_form")
@app.get("/edit/<table_name>/<pk_val>", name="edit_form")
@login_required
async def show_form(request, table_name, pk_val=None):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.redirect("/")
        
        pk_column = await get_pk_column(conn, table_name)
        schema = await _get_cached_schema(conn, table_name)
        
        existing_data = {}
        if pk_val:
            pk_type = next((r['data_type'] for r in schema if r['column_name'] == pk_column), 'integer')
            target_id = str(pk_val) if pk_type in ('character varying', 'text', 'varchar') else int(pk_val)
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", target_id)
            if row: existing_data = dict(row)
            
        columns = []
        for c in schema:
            col_name = c['column_name']
            columns.append({
                "column_name": col_name, "label": make_human_readable(col_name), "data_type": c['data_type'],
                "is_pk": col_name == pk_column, "required": c['is_nullable'] == 'NO',
                "max_length": c.get('character_maximum_length'), "value": existing_data.get(col_name),
                "options": await get_dropdown_options(conn, col_name)
            })
        columns.sort(key=lambda x: get_column_sort_priority(pk_column, x['column_name']))
        
        return await render("form_view.html", context={
            "table_name": table_name, "table_title": make_human_readable(table_name), "columns": columns,
            "pk_val": pk_val, "is_update": pk_val is not None, "all_tables": allowed,
            "table_modules": get_table_modules(allowed), "username": request.ctx.username,
            "user_id": request.ctx.user_id, "csrf_token": request.ctx.csrf_token
        })


# ==========================================
#  HTMX COMPONENT ENDPOINTS
# ==========================================

@app.get("/api/components/lookup_types_master")
@login_required
async def get_lookup_types_master(request):
    async with app.ctx.pool.acquire() as conn:
        types = await conn.fetch("SELECT * FROM phc_lookup_types ORDER BY plt_lookup_type_code ASC")
        return await render("lookup_master_partial.html", context={"types": [dict(t) for t in types]})

@app.get("/api/lookup_values/<type_code>")
@login_required
async def get_lookup_values(request, type_code):
    async with app.ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM phc_lookup_values_t WHERE plv_lookup_type_code = $1 ORDER BY plv_lookup_value_name ASC", type_code)
        return await render("lookups_partial.html", context={"rows": [dict(r) for r in rows], "type_code": type_code})


# ==========================================
#  API ROUTES (SAVE & DELETE)
# ==========================================

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
        
        clean_data = await _sanitize_payload(data, schema_map, pk_column, current_user_id, request.ctx.username, request.method)

        async with conn.transaction():
            if request.method == "POST":
                if company_col: clean_data[company_col] = company_id
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                is_string_pk = pk_type in ('character varying', 'text', 'varchar')
                
                if is_string_pk:
                    if pk_column not in clean_data: return response.json({"error": f"Primary Key ({pk_column}) is required."}, status=400)
                    target_id = clean_data[pk_column]
                else:
                    max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                    target_id = (int(max_val) + 1) if max_val else 1
                    clean_data[pk_column] = target_id
                
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

        if request.headers.get("HX-Request"): return response.html(f'<div id="toast-container" hx-swap-oob="beforeend"><div class="toast" style="animation: slideIn Toast 0.4s ease, fadeOutToast 0.4s ease 3.5s forwards;"><i data-lucide="check-circle-2" style="color: var(--color-cipher-mint)"></i> {msg}</div></div>')
        return response.json({"status": "success"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)
