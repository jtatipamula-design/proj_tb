import os
import uuid
import time
from datetime import datetime
from functools import wraps
import bcrypt
import jwt
from sanic import Sanic, response
from sanic.exceptions import NotFound
import asyncpg
from jinja2 import Environment, FileSystemLoader, select_autoescape

app = Sanic("ERP_System")

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL")
JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key-change-in-prod")
PORT = int(os.environ.get("PORT", 10000))

env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# --- ENTERPRISE PERFORMANCE CACHE ---
USER_AUTH_CACHE = {}
CACHE_TTL = 30  # Live checking every 30 seconds
SCHEMA_CACHE = {"pks": {}, "fks": {}, "display_cols": {}}

def quote_ident(name: str) -> str:
    """Safely quotes SQL identifiers (table names, column names)."""
    if not name:
        return '""'
    return '"' + str(name).replace('"', '""') + '"'

def safe_cast_pk(val, data_type='integer'):
    """Safely converts primary key values based on column target type."""
    if val is None:
        return None
    val_str = str(val).strip()
    if not val_str:
        return None
    if data_type in ('integer', 'bigint', 'smallint', 'numeric'):
        try:
            return int(val_str)
        except (ValueError, TypeError):
            return None
    return val_str

def prune_user_auth_cache():
    """Prunes expired entries and caps USER_AUTH_CACHE size."""
    now = time.time()
    expired = [k for k, v in USER_AUTH_CACHE.items() if now >= v.get("expires", 0)]
    for k in expired:
        USER_AUTH_CACHE.pop(k, None)
    if len(USER_AUTH_CACHE) > 500:
        keys = list(USER_AUTH_CACHE.keys())[:100]
        for k in keys:
            USER_AUTH_CACHE.pop(k, None)

def clear_schema_cache():
    """Flushes schema and auth caches when schema mutations occur."""
    SCHEMA_CACHE["pks"].clear()
    if "fks" in SCHEMA_CACHE:
        SCHEMA_CACHE["fks"].clear()
    if "display_cols" in SCHEMA_CACHE:
        SCHEMA_CACHE["display_cols"].clear()
    USER_AUTH_CACHE.clear()

def build_modules_tree(all_tables, table_modules):
    modules_tree = {}
    for tbl_code, tbl_name in all_tables.items():
        mod_name = table_modules.get(tbl_code, 'Uncategorized')
        if mod_name not in modules_tree:
            modules_tree[mod_name] = []
        modules_tree[mod_name].append({"code": tbl_code, "name": tbl_name})
    return dict(sorted(modules_tree.items()))

def render_template(template_name, request=None, **context):
    default_context = {
        "username": "",
        "user_id": None,
        "user_role": "STD",
        "modules_tree": {},
        "all_tables": [],
        "table_modules": {},
        "lookup_categories": [],
        "type_filter": ""
    }
    if request and hasattr(request, 'ctx'):
        all_tables = getattr(request.ctx, "all_tables", {})
        if isinstance(all_tables, dict):
            all_tables_list = list(all_tables.keys())
        elif isinstance(all_tables, list):
            all_tables_list = all_tables
        else:
            all_tables_list = []

        default_context.update({
            "username": getattr(request.ctx, "username", ""),
            "user_id": getattr(request.ctx, "user_id", None),
            "user_role": getattr(request.ctx, "role", "STD"),
            "modules_tree": getattr(request.ctx, "modules_tree", {}),
            "all_tables": all_tables_list,
            "table_modules": getattr(request.ctx, "table_modules", {})
        })
    default_context.update(context)
    template = env.get_template(template_name)
    html = template.render(**default_context)
    return add_security_headers(response.html(html))

async def get_authorized_tables(conn, user_id, role):
    if role == 'ADM':
        query = """
            SELECT s.psn_screen_code, s.psn_screen_name, COALESCE(m.pmd_module_name, 'System Config') as module_name
            FROM phc_screens_t s
            LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
            WHERE s.psn_screen_code ILIKE 'phc_%' OR s.psn_screen_code ILIKE 'pcv_%'
        """
        rows = await conn.fetch(query)
    else:
        query = """
            SELECT DISTINCT s.psn_screen_code, s.psn_screen_name, COALESCE(m.pmd_module_name, 'Uncategorized') as module_name 
            FROM phc_screens_t s
            JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
            JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
            LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
            WHERE ura.pua_user_id = $1 
              AND (s.psn_screen_code ILIKE 'phc_%' OR s.psn_screen_code ILIKE 'pcv_%')
        """
        rows = await conn.fetch(query, user_id)
        
        valid_rows = []
        for r in rows:
            if r['module_name'].lower() == 'erpadmin':
                continue
            valid_rows.append(r)
        rows = valid_rows
        
    auth_tables = {}
    table_modules = {}
    for r in rows:
        code = r['psn_screen_code'].lower()
        auth_tables[code] = r['psn_screen_name']
        table_modules[code] = r['module_name']
        
    return auth_tables, table_modules

@app.before_server_start
async def setup_db(app, loop):
    if DATABASE_URL:
        app.ctx.pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=20)
    else:
        print("WARNING: DATABASE_URL not set. Running without db pool.")
        app.ctx.pool = None

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'pool') and app.ctx.pool:
        await app.ctx.pool.close()

def add_security_headers(res):
    res.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    res.headers["Pragma"] = "no-cache"
    return res

@app.middleware('request')
async def setup_request_context(request):
    if not hasattr(request.ctx, 'user_id'):
        request.ctx.user_id = None
    if not hasattr(request.ctx, 'username'):
        request.ctx.username = ''
    if not hasattr(request.ctx, 'role'):
        request.ctx.role = 'STD'
    if not hasattr(request.ctx, 'all_tables'):
        request.ctx.all_tables = {}
    if not hasattr(request.ctx, 'table_modules'):
        request.ctx.table_modules = {}
    if not hasattr(request.ctx, 'modules_tree'):
        request.ctx.modules_tree = {}

def check_auth(f):
    @wraps(f)
    async def decorated_function(request, *args, **kwargs):
        prune_user_auth_cache()

        def unauth_response(req):
            if req.path.startswith('/api/'):
                return response.json({"error": "Unauthorized"}, status=401)
            res = response.redirect("/login")
            res.delete_cookie("auth_token")
            return add_security_headers(res)

        token = request.cookies.get("auth_token")
        if not token:
            return unauth_response(request)
        
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except Exception as e:
            print(f"JWT Auth Error: {e}")
            return unauth_response(request)

        user_id = payload.get("user_id")
        session_id = payload.get("session_id")
        
        now = time.time()
        cached = USER_AUTH_CACHE.get(user_id)
        
        if cached and now < cached["expires"] and "all_tables" in cached:
            if cached["session_id"] != session_id:
                return unauth_response(request)
            request.ctx.user_id = user_id
            request.ctx.username = payload.get("username")
            request.ctx.role = cached["role"]
            request.ctx.all_tables = cached.get("all_tables", {})
            request.ctx.table_modules = cached.get("table_modules", {})
            request.ctx.modules_tree = cached.get("modules_tree", {})
        else:
            try:
                async with app.ctx.pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT pus_session_id, pus_user_type FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if not user or user["pus_session_id"] != session_id:
                        return unauth_response(request)
                    
                    role = user["pus_user_type"] or "STD"
                    auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
                    modules_tree = build_modules_tree(auth_tables, table_modules)

                    USER_AUTH_CACHE[user_id] = {
                        "session_id": session_id,
                        "role": role,
                        "all_tables": auth_tables,
                        "table_modules": table_modules,
                        "modules_tree": modules_tree,
                        "expires": now + CACHE_TTL
                    }
                    request.ctx.user_id = user_id
                    request.ctx.username = payload.get("username")
                    request.ctx.role = role
                    request.ctx.all_tables = auth_tables
                    request.ctx.table_modules = table_modules
                    request.ctx.modules_tree = modules_tree
            except Exception as db_err:
                print(f"Database Auth Check Error: {db_err}")
                return response.text(f"Database connection error: {db_err}", status=500)

        return await f(request, *args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        return render_template('login.html', request=request)
    
    data = request.json or {}
    username = data.get("username", "")
    password = data.get("password", "")
    
    if not username or not password:
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

    async with app.ctx.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM phc_users_t WHERE pus_user_name = $1", username)
        
        if user:
            stored_pwd = user['pus_pwd'] or ""
            is_valid = False
            if stored_pwd:
                try:
                    if bcrypt.checkpw(password.encode('utf-8'), stored_pwd.encode('utf-8')):
                        is_valid = True
                except (ValueError, TypeError):
                    pass
                    
            if is_valid:
                session_id = str(uuid.uuid4())
                async with conn.transaction():
                    await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", session_id, user['pus_user_id'])
                
                token_payload = {
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "session_id": session_id,
                    "exp": time.time() + 86400
                }
                token = jwt.encode(token_payload, JWT_SECRET, algorithm="HS256")
                USER_AUTH_CACHE.pop(user['pus_user_id'], None)
                
                res = response.json({"status": "success", "message": "Login successful"})
                res.add_cookie("auth_token", token, httponly=True, samesite="Lax")
                return add_security_headers(res)
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout', methods=['GET'])
@check_auth
async def logout(request):
    user_id = getattr(request.ctx, 'user_id', None)
    if user_id:
        USER_AUTH_CACHE.pop(user_id, None)
        if hasattr(app.ctx, 'pool') and app.ctx.pool:
            try:
                async with app.ctx.pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute("UPDATE phc_users_t SET pus_session_id = NULL WHERE pus_user_id = $1", user_id)
            except Exception as e:
                print(f"Logout session clear error: {e}")
    res = response.redirect("/login")
    res.delete_cookie("auth_token")
    return add_security_headers(res)

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
        return None

# --- SMART FOREIGN KEY RESOLUTION ---
FK_HEURISTICS = {
    'company_id': 'phc_companies_t',
    'dept_id': 'phc_dept_t',
    'services_id': 'phc_services_t',
    'cost_center_id': 'phc_cost_center_t',
    'module_id': 'phc_module_t',
    'screen_id': 'phc_screens_t',
    'role_id': 'phc_roles_t',
    'user_id': 'phc_users_t',
    'product_id': 'pcv_products_t',
    'equipment_id': 'pcv_equipments_t',
    'execution_id': 'pcv_validation_executions_t',
    'cpr_id': 'pcv_cleaning_process_records_t',
    'trf_id': 'pcv_test_request_forms_t',
    'sampling_record_id': 'pcv_sampling_records_t',
    'pde_id': 'pcv_pde_registrations_t',
    'mdd_id': 'pcv_mdd_registrations_t',
    'training_id': 'pcv_training_records_t',
    'clearance_id': 'pcv_equipment_clearance_checklists_t',
    'sampling_loc_id': 'pcv_equipment_sampling_locations_t'
}

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

async def resolve_fk_details(conn, table_name, column_name):
    fk_map = await get_fk_map(conn, table_name)
    if column_name in fk_map: return fk_map[column_name]['table'], fk_map[column_name]['pk']
    for suffix, target_table in FK_HEURISTICS.items():
        if column_name.endswith(suffix):
            target_pk = await get_pk_column(conn, target_table)
            if target_pk: return target_table, target_pk
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

async def get_dropdown_options(conn, table_name, column_name):
    if column_name.endswith('_org_id') or column_name == 'pos_org_id':
        return []
        
    # 1. Dynamic Lookup System Check
    # If a user creates a Lookup Type that EXACTLY matches the column name (case-insensitive),
    # this will instantly wire up those values as a dropdown for this field!
    try:
        query = """
            SELECT plv_lookup_value_code as id, plv_lookup_value_name as name 
            FROM phc_lookup_values_t 
            WHERE upper(plv_lookup_type_code) = upper($1) AND plv_status = 'ACT'
            ORDER BY plv_lookup_value_name
        """
        rows = await conn.fetch(query, column_name)
        if rows:
            return [{"id": str(r['id']), "name": str(r['name'])} for r in rows]
    except Exception:
        pass

    # 2. Foreign Key Dropdown Fallback
    f_table, f_pk = await resolve_fk_details(conn, table_name, column_name)
    if f_table and f_pk:
        try:
            return await get_fk_display_dict(conn, f_table, f_pk)
        except Exception: pass
    return []

def _is_password_column(col_name: str) -> bool:
    if not col_name:
        return False
    c = str(col_name).lower()
    return c in ('pus_pwd', 'password', 'pus_password') or 'password' in c or c.endswith('pwd')

def _sanitize_payload(data, pk_column, schema_map, is_update=False):
    clean_data = {}
    for k, v in data.items():
        if k == pk_column:
            continue 
        if 'created' in k.lower() or 'modified' in k.lower() or 'edited' in k.lower():
            continue

        if is_update and (v == "" or v is None):
            if _is_password_column(k):
                continue
            clean_data[k] = None
            continue
        elif not is_update and (v == "" or v is None):
            continue

        if _is_password_column(k) and v:
            if isinstance(v, str) and not v.startswith(('$2b$', '$2a$')):
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')

        col_info = schema_map.get(k, {})
        target_type = col_info.get('data_type', '').lower()
        max_len = col_info.get('character_maximum_length')
        
        if 'date' in target_type or 'timestamp' in target_type or (isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-'):
            if isinstance(v, str) and v:
                try:
                    parsed_dt = datetime.strptime(v, '%Y-%m-%d')
                    v = parsed_dt.date() if target_type == 'date' else parsed_dt
                except ValueError:
                    try:
                        parsed_dt = datetime.fromisoformat(v)
                        v = parsed_dt.date() if target_type == 'date' else parsed_dt
                    except ValueError:
                        pass
            elif isinstance(v, datetime) and target_type == 'date':
                v = v.date()

        if isinstance(v, str) and max_len is not None:
            if len(v) > max_len:
                if "status" in k and v.lower() == "active": v = "ACT"
                elif "status" in k and v.lower() == "inactive": v = "INA"
                else: v = v[:max_len]
        
        if target_type in ('integer', 'bigint', 'smallint'):
            if isinstance(v, bool):
                clean_data[k] = v
            else:
                try:
                    clean_data[k] = int(float(v))
                except (ValueError, TypeError, OverflowError):
                    clean_data[k] = None
        elif target_type == 'numeric' and isinstance(v, str):
            try:
                clean_data[k] = float(v) if '.' in v else int(v)
            except (ValueError, TypeError):
                clean_data[k] = None
        else:
            clean_data[k] = v

    return clean_data


@app.route('/')
@check_auth
async def dashboard(request):
    stats = {}
    return render_template('dashboard.html', request=request, stats=stats)

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except (ValueError, TypeError):
        page = 1

    per_page = 50
    offset = (page - 1) * per_page
    search_query = request.args.get("q", "").strip()
    type_filter = request.args.get("type_filter", "").strip()

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with app.ctx.pool.acquire() as conn:
        if auth_tables is None or table_modules is None:
            auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            raise NotFound("Table configuration error: No Primary Key")

        columns_data = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)

        if not columns_data:
            raise NotFound("Table does not exist")

        columns = []
        date_columns = []
        company_col_def = None
        for c in columns_data:
            cname = c['column_name']
            if cname in (pk_column, 'psn_screen_id'): continue
            
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
                    company_col_def = {"raw": cname, "column_name": cname, "label": clean_label}
                continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            col_def = {"raw": cname, "column_name": cname, "label": clean_label}
            if 'date' in c['data_type'] or 'timestamp' in c['data_type']:
                date_columns.append(col_def)
            else:
                columns.append(col_def)
                
        columns.extend(date_columns)
        if company_col_def:
            columns.append(company_col_def)

        lookup_categories = []
        if table_name == 'phc_lookup_values_t':
            try:
                lookup_categories = await conn.fetch(
                    "SELECT plt_lookup_type_code as code, plt_lookup_type_name as name FROM phc_lookup_types_t WHERE plt_status = 'ACT' ORDER BY plt_lookup_type_name"
                )
            except Exception:
                lookup_categories = []

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)
        
        base_query = f"SELECT * FROM {q_table}"
        count_query = f"SELECT COUNT(*) FROM {q_table}"
        params = []
        where_clauses = []

        if table_name == 'phc_lookup_values_t' and type_filter:
            params.append(type_filter)
            where_clauses.append(f"{quote_ident('plv_lookup_type_code')} = ${len(params)}")

        if search_query:
            text_cols = [c['column_name'] for c in columns_data if c['data_type'] in ('character varying', 'text', 'character')]
            if text_cols:
                search_clauses = []
                for col in text_cols:
                    params.append(f"%{search_query}%")
                    search_clauses.append(f"{quote_ident(col)} ILIKE ${len(params)}")
                where_clauses.append("(" + " OR ".join(search_clauses) + ")")

        if where_clauses:
            where_str = " WHERE " + " AND ".join(where_clauses)
            base_query += where_str
            count_query += where_str

        base_query += f" ORDER BY {q_pk} DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
        
        total_count = await conn.fetchval(count_query, *params)
        total_count = total_count or 0
        raw_rows = await conn.fetch(base_query, *(params + [per_page, offset]))

        resolved_rows = [dict(r) for r in raw_rows]
        
        # We cache lookup values across the table to avoid N+1 queries
        dynamic_lookups_cache = {}
        
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
            # 1. Check for Foreign Key Relations
            f_table, f_pk = await resolve_fk_details(conn, table_name, cname)
            if f_table and f_pk:
                ids = set(r[cname] for r in resolved_rows if r[cname] is not None)
                if ids:
                    try:
                        lookup_dict = await get_fk_display_dict(conn, f_table, f_pk, specific_ids=ids)
                        for r in resolved_rows:
                            val = r[cname]
                            if val in lookup_dict:
                                r[cname] = f"{lookup_dict[val]} (ID: {val})"
                    except Exception as e:
                        print(f"Error resolving table FK {cname}: {e}")
                        
            # 2. Check for Dynamic Lookups
            else:
                try:
                    if cname not in dynamic_lookups_cache:
                        l_query = "SELECT plv_lookup_value_code as id, plv_lookup_value_name as name FROM phc_lookup_values_t WHERE upper(plv_lookup_type_code) = upper($1)"
                        l_rows = await conn.fetch(l_query, cname)
                        if l_rows:
                            dynamic_lookups_cache[cname] = {str(r['id']): str(r['name']) for r in l_rows}
                        else:
                            dynamic_lookups_cache[cname] = {}
                            
                    if dynamic_lookups_cache[cname]:
                        lookup_dict = dynamic_lookups_cache[cname]
                        for r in resolved_rows:
                            val_str = str(r[cname]) if r[cname] is not None else None
                            if val_str and val_str in lookup_dict:
                                r[cname] = f"{lookup_dict[val_str]}"
                except Exception:
                    pass
                    
        rows = resolved_rows

    total_pages = max(1, (total_count + per_page - 1) // per_page)
    start_row = offset + 1 if total_count > 0 else 0
    end_row = min(offset + per_page, total_count)

    return render_template(
        'table_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        rows=rows,
        pk_column=pk_column,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        start_row=start_row,
        end_row=end_row,
        search_query=search_query,
        lookup_categories=lookup_categories,
        type_filter=type_filter
    )

@app.route('/new/<table_name>', methods=['GET'])
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

@app.route('/form/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_form_view_alias(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

async def render_form(request, table_name, is_update=False, pk_val=None):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with app.ctx.pool.acquire() as conn:
        if auth_tables is None or table_modules is None:
            auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)

        columns_data = await conn.fetch("""
            SELECT column_name, data_type, is_nullable, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)

        schema_map = {c['column_name']: dict(c) for c in columns_data}
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer')

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)

        row_data = {}
        if is_update:
            cast_pk = safe_cast_pk(pk_val, pk_type)
            if cast_pk is None:
                raise NotFound("Invalid primary key format")
            row_data = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1", cast_pk)
            if not row_data:
                raise NotFound("Record not found")

        columns = []
        company_form_def = None
        for c in columns_data:
            cname = c['column_name']
            if 'created' in cname.lower() or 'modified' in cname.lower() or 'edited' in cname.lower():
                continue
                
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if not (table_name == 'phc_screens_t' and role == 'ADM'):
                    continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            val = row_data.get(cname, '') if is_update else ''
            options = await get_dropdown_options(conn, table_name, cname)

            json_options = None
            if table_name == 'phc_role_screen_assignment_t' and cname == 'prs_screen_id' and not is_update:
                json_options = await get_dropdown_options(conn, table_name, cname)
                options = [] 
                
            col_def = {
                "column_name": cname,
                "label": clean_label,
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO' and 'default' not in cname.lower(),
                "is_pk": cname == pk_column,
                "value": val,
                "options": options,
                "json_options": json_options
            }
            if is_company_col:
                company_form_def = col_def
            else:
                columns.append(col_def)

        if company_form_def:
            columns.append(company_form_def)

    return render_template(
        'form_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        is_update=is_update,
        pk_val=pk_val
    )

@app.route('/export/<table_name>')
@check_auth
async def export_table_csv(request, table_name):
    import csv
    import io
    
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    async with app.ctx.pool.acquire() as conn:
        auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")

        pk_column = await get_pk_column(conn, table_name)

        columns_data = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)

        export_cols = []
        company_csv_col = None
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    company_csv_col = cname
                continue
            export_cols.append(cname)
        if company_csv_col:
            export_cols.append(company_csv_col)

        if not export_cols:
            return response.text("No exportable columns found.", status=400)

        col_list = ", ".join(quote_ident(c) for c in export_cols)
        q_table = quote_ident(table_name)
        order_clause = f" ORDER BY {quote_ident(pk_column)} DESC" if pk_column else ""
        rows = await conn.fetch(f"SELECT {col_list} FROM {q_table}{order_clause}")

    output = io.StringIO()
    writer = csv.writer(output)

    header = [col.split('_', 1)[-1].replace('_', ' ').title() for col in export_cols]
    writer.writerow(header)

    for row in rows:
        csv_row = []
        for col in export_cols:
            val = row[col]
            if val is None:
                csv_row.append('')
            elif isinstance(val, datetime):
                csv_row.append(val.strftime('%Y-%m-%d'))
            else:
                csv_row.append(str(val))
        writer.writerow(csv_row)

    csv_content = output.getvalue()
    output.close()

    table_title = auth_tables.get(table_name, table_name)
    filename = f"{table_title.replace(' ', '_')}_Export.csv"

    res = response.text(csv_content, content_type="text/csv")
    res.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return add_security_headers(res)

@app.route('/api/<table_name>', methods=['POST'], name="api_create")
@check_auth
async def api_create_record(request, table_name):
    return await process_api_action(request, table_name, None)

@app.route('/api/<table_name>/<pk_val>', methods=['PUT', 'DELETE', 'POST'], name="api_update_delete")
@check_auth
async def api_modify_record(request, table_name, pk_val):
    return await process_api_action(request, table_name, pk_val)

async def process_api_action(request, table_name, pk_val):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    
    method = request.method
    if request.form and request.form.get('_method'):
        method = request.form.get('_method')[0].upper()

    async with app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            return response.json({"error": "Unauthorized"}, status=403)
        
        pk_column = await get_pk_column(conn, table_name)
        columns_info = await conn.fetch("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public'
        """, table_name)
        schema_map = {c['column_name']: dict(c) for c in columns_info}
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer')

        cast_pk = safe_cast_pk(pk_val, pk_type)
        if method in ('PUT', 'DELETE') and cast_pk is None:
            return response.json({"error": "Invalid primary key format"}, status=400)

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)

        if method == 'DELETE':
            try:
                async with conn.transaction():
                    res = await conn.execute(f"DELETE FROM {q_table} WHERE {q_pk} = $1", cast_pk)
                if res.endswith(" 0"):
                    return response.json({"error": "Record not found"}, status=404)
                return response.json({"status": "success"})
            except Exception as e:
                return response.json({"error": str(e)}, status=400)

        try:
            data = request.form if request.form else request.json
            if not data:
                return response.json({"error": "No data provided"}, status=400)
            data_dict = {k: v[0] if isinstance(v, list) else v for k, v in data.items() if k != '_method'}
        except Exception:
            return response.json({"error": "Invalid or malformed payload"}, status=400)

        if request.files:
            upload_dir = os.path.join(os.getcwd(), 'uploads')
            os.makedirs(upload_dir, exist_ok=True)
            for file_key, file_objs in request.files.items():
                file_obj = file_objs[0] if isinstance(file_objs, list) else file_objs
                if file_obj and hasattr(file_obj, 'body') and file_obj.body:
                    fname = f"{uuid.uuid4().hex}_{getattr(file_obj, 'name', 'file')}"
                    fpath = os.path.join(upload_dir, fname)
                    with open(fpath, 'wb') as f:
                        f.write(file_obj.body)
                    data_dict[file_key] = f"uploads/{fname}"

        clean_data = _sanitize_payload(data_dict, pk_column, schema_map, is_update=(method == 'PUT'))

        company_col = next((c for c in schema_map if c.endswith('_company_id') or c == 'company_id'), None)
        if company_col:
            if not (table_name == 'phc_screens_t' and role == 'ADM') or company_col not in clean_data or not clean_data[company_col]:
                user_company = await conn.fetchval("SELECT pus_company_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
                if user_company:
                    clean_data[company_col] = user_company

        who_cols = [c for c in schema_map if 'created' in c.lower() or 'modified' in c.lower() or 'edited' in c.lower()]
        for wc in who_cols:
            if ('modified' in wc or 'edited' in wc) and 'by' not in wc: clean_data[wc] = datetime.now()
            elif ('modified' in wc or 'edited' in wc) and 'by' in wc: clean_data[wc] = "System"
            elif method == 'POST':
                if 'created' in wc and 'by' not in wc: clean_data[wc] = datetime.now()
                elif 'created' in wc and 'by' in wc: clean_data[wc] = "System"

        try:
            async with conn.transaction():
                if method == 'POST':
                    if table_name == 'phc_role_screen_assignment_t' and 'prs_screen_id' in data_dict:
                        raw_scr = data_dict['prs_screen_id']
                        scr_list = []
                        if isinstance(raw_scr, str) and raw_scr.startswith('['):
                            try:
                                import json
                                scr_list = json.loads(raw_scr)
                            except Exception:
                                scr_list = [raw_scr]
                        elif isinstance(raw_scr, list):
                            scr_list = raw_scr
                        else:
                            scr_list = [raw_scr]

                        for sid in scr_list:
                            row_clean = clean_data.copy()
                            row_clean['prs_screen_id'] = int(sid)
                            cols = [quote_ident(c) for c in row_clean.keys()]
                            vals = list(row_clean.values())
                            placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
                            q = f"INSERT INTO {q_table} ({', '.join(cols)}) VALUES ({placeholders})"
                            await conn.execute(q, *vals)
                    else:
                        cols = [quote_ident(c) for c in clean_data.keys()]
                        vals = list(clean_data.values())
                        placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
                        q = f"INSERT INTO {q_table} ({', '.join(cols)}) VALUES ({placeholders})"
                        await conn.execute(q, *vals)

                elif method == 'PUT':
                    if not clean_data:
                        return response.json({"error": "No update fields provided"}, status=400)
                    cols = list(clean_data.keys())
                    vals = list(clean_data.values())
                    set_clause = ", ".join([f"{quote_ident(c)} = ${i+1}" for i, c in enumerate(cols)])
                    q = f"UPDATE {q_table} SET {set_clause} WHERE {q_pk} = ${len(vals)+1}"
                    res = await conn.execute(q, *(vals + [cast_pk]))
                    if res.endswith(" 0"):
                        return response.json({"error": "Record not found"}, status=404)

            if request.headers.get("HX-Request"):
                res = response.json({"status": "success"})
                res.headers["HX-Redirect"] = f"/table/{table_name}"
                return add_security_headers(res)
            else:
                return response.redirect(f"/table/{table_name}")
            
        except Exception as e:
            return response.json({"error": str(e)}, status=400)

@app.route('/fixdb', methods=['GET'])
@check_auth
async def fixdb_route(request):
    """Temporary route to perfectly sync the database to UI Built Status.xlsx"""
    if getattr(request.ctx, 'role', 'STD') != 'ADM':
        return response.json({"error": "Forbidden: Admin access required"}, status=403)

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

    try:
        async with app.ctx.pool.acquire() as conn:
            async with conn.transaction():
                # 1. Nuke the ghost testing screens
                await conn.execute("DELETE FROM phc_screens_t WHERE psn_screen_name = 'Updated Screen'")
                
                # 2. Get/Create all Modules from Excel
                unique_modules = set(EXCEL_MAPPINGS.values())
                for mod_name in unique_modules:
                    exists = await conn.fetchval("SELECT pmd_module_id FROM phc_module_t WHERE pmd_module_name = $1", mod_name)
                    if not exists:
                        max_id = await conn.fetchval("SELECT MAX(pmd_module_id) FROM phc_module_t")
                        await conn.execute("INSERT INTO phc_module_t (pmd_module_id, pmd_module_name, pmd_status, pmd_created_by, pmd_modified_by) VALUES ($1, $2, 'ACT', 'System', 'System')", (max_id or 0) + 1, mod_name)
                        
                # 3. Fetch module mapping dictionary
                mod_rows = await conn.fetch("SELECT pmd_module_id, pmd_module_name FROM phc_module_t")
                mod_dict = {row['pmd_module_name']: row['pmd_module_id'] for row in mod_rows}

                # 4. Map screens to exact modules
                for screen_code, module_name in EXCEL_MAPPINGS.items():
                    target_mod_id = mod_dict.get(module_name)
                    if target_mod_id:
                        await conn.execute("UPDATE phc_screens_t SET psn_module_id = $1 WHERE psn_screen_code = $2", target_mod_id, screen_code)

                # 5. Insert Manage Modules and Manage Screens if they don't exist yet
                for screen_code, screen_name in [('phc_module_t', 'Manage Modules'), ('phc_screens_t', 'Manage Screens')]:
                    exists = await conn.fetchval("SELECT psn_screen_id FROM phc_screens_t WHERE psn_screen_code = $1", screen_code)
                    if not exists:
                        max_scr = await conn.fetchval("SELECT MAX(psn_screen_id) FROM phc_screens_t")
                        await conn.execute("""
                            INSERT INTO phc_screens_t (psn_screen_id, psn_company_id, psn_module_id, psn_screen_code, psn_screen_name, psn_status, psn_created_by, psn_modified_by) 
                            VALUES ($1, 1, $2, $3, $4, 'ACT', 'System', 'System')
                        """, (max_scr or 0) + 1, mod_dict['ERPAdmin'], screen_code, screen_name)

        clear_schema_cache()
        return response.html("<h1>Database Fix Applied!</h1><p>Every screen has been perfectly mapped to the Excel spreadsheet layout. Go back to the <a href='/'>dashboard</a> and hit refresh.</p>")
    except Exception as e:
        return response.html(f"<h1>Error</h1><p>{e}</p>")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=PORT, single_process=True)