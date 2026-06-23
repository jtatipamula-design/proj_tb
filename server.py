import os
import csv
import io
import time
import uuid
import asyncio
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from functools import wraps, partial

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
#  CONSTANTS & IN-MEMORY CACHING (ENTERPRISE SPEC)
# ==========================================
RATE_LIMIT_WINDOW = 60
MAX_REQUESTS = 120

# Reduced DB load by caching Schema, Sessions, and Permissions
SCHEMA_CACHE = {
    "tables": None,
    "pks": {},
    "columns": {},
    "dropdown_lookups": {}
}

# 60-second TTL caches to prevent 10,000+ DB hits per minute from 1000 users
AUTH_CACHE = {} 
RBAC_CACHE = {}

KEYWORD_MAP = {
    'company': 'phc_companies_t', 'dept': 'phc_dept_t', 'department': 'phc_dept_t',
    'user': 'phc_users_t', 'emp': 'phc_emp_t', 'employee': 'phc_emp_t',
    'app': 'phc_apps_t', 'org': 'phc_orgs_t', 'organization': 'phc_orgs_t',
    'role': 'phc_roles_t', 'screen': 'phc_screens_t',
    'location': 'phc_locations_t', 'service': 'phc_services_t',
    'group': 'phc_user_groups_t', 'center': 'phc_cost_centers_t'
}

WHO_COLS = {'creation_date', 'created_by', 'last_update_date', 'last_updated_by'}


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
        if not token:
            return response.redirect("/login")

        try:
            payload = jwt.decode(token, app.config.SECRET, algorithms=["HS256"])
            user_id = payload.get("user_id")
            session_id = payload.get("session_id")
            now = time.time()

            # ULTRA-FAST AUTH CACHING (Bypasses DB if checked within last 60 seconds)
            cached_auth = AUTH_CACHE.get(user_id)
            if cached_auth and cached_auth['session'] == session_id and cached_auth['expires'] > now:
                pass # Cache Hit - Valid!
            else:
                # Cache Miss - Ping DB
                async with app.ctx.pool.acquire() as conn:
                    db_session = await conn.fetchval(
                        "SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id
                    )
                    if db_session != session_id:
                        resp = response.redirect("/login")
                        resp.delete_cookie("auth_token")
                        return resp
                    # Store in cache for 60 seconds
                    AUTH_CACHE[user_id] = {'session': db_session, 'expires': now + 60}

            request.ctx.user_id = user_id
            request.ctx.user_type = payload.get("user_type")
            request.ctx.username = payload.get("username", "User")

        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return response.redirect("/login")
        except asyncpg.PostgresError as e:
            print(f"DB Error during auth validation: {e}")
            return response.text("Internal Database Error", status=500)

        return await wrapped(request, *args, **kwargs)
    return decorator


# ==========================================
#  DATABASE LIFECYCLE & HELPERS
# ==========================================
@app.before_server_start
async def setup_db(app_instance, loop):
    """Initializes the database connection pool for each individual worker."""
    try:
        dsn = CLOUD_DB_URL or f"postgres://postgres:{os.environ.get('LOCAL_DB_PASSWORD')}@localhost/tablesproj"
        # ENTERPRISE FIX: Expanded pool size significantly for high concurrency
        app_instance.ctx.pool = await asyncpg.create_pool(
            dsn=dsn, statement_cache_size=0, min_size=10, max_size=100
        )

        # ENTERPRISE FIX: PostgreSQL Advisory Lock completely eliminates deadlocks 
        # by forcing the 8 workers to queue sequentially when verifying tables/indexes
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


# ==========================================
#  DATA RESOLUTION & RBAC ENGINES
# ==========================================
async def get_allowed_tables(conn, user_id, user_type):
    if SCHEMA_CACHE["tables"] is None:
        rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%_t'")
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
    
    RBAC_CACHE[user_id] = {'tables': final_tables, 'expires': now + 300} # Cache for 5 mins
    return final_tables


def mask_sensitive_data(col_name, val, user_type):
    if val is None or user_type == 'ADM':
        return val

    k_lower = col_name.lower()
    v_str = str(val)

    if 'pwd' in k_lower or 'password' in k_lower:
        return '********'
    if 'email' in k_lower and '@' in v_str:
        parts = v_str.split('@')
        return f"{parts[0][0]}***@{parts[1]}" if len(parts[0]) > 1 else f"***@{parts[1]}"
    if 'phone' in k_lower and len(v_str) >= 4:
        return f"***-***-{v_str[-4:]}"
    if 'account_number' in k_lower and len(v_str) >= 4:
        return f"****-****-{v_str[-4:]}"

    return val


async def find_target_table(conn, column_name):
    if not column_name.endswith('_id'):
        return None

    if SCHEMA_CACHE["tables"] is None:
        rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%_t'")
        SCHEMA_CACHE["tables"] = [r['table_name'] for r in rows]

    all_tables = SCHEMA_CACHE["tables"]

    for key, table in KEYWORD_MAP.items():
        if key in column_name and table in all_tables:
            return table

    parts = column_name.split('_')
    if len(parts) >= 3:
        base_name = parts[-2]
        for pt in [f"phc_{base_name}_t", f"phc_{base_name}s_t", f"phc_{base_name}es_t"]:
            if pt in all_tables:
                return pt
    return None


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
    target_table = await find_target_table(conn, column_name)
    if not target_table:
        return None

    cache_key = f"{target_table}_lookups"
    if cache_key in SCHEMA_CACHE["dropdown_lookups"]:
        cache_entry = SCHEMA_CACHE["dropdown_lookups"][cache_key]
        if time.time() - cache_entry['time'] < 3600: # Cache dropdowns for an hour
            return cache_entry['data']

    pk_col = await get_pk_column(conn, target_table)
    if not pk_col:
        return None

    if target_table not in SCHEMA_CACHE["columns"]:
        cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", target_table)
        SCHEMA_CACHE["columns"][target_table] = [dict(c) for c in cols]

    cols = SCHEMA_CACHE["columns"][target_table]
    name_col = pk_col

    for r in cols:
        if r['column_name'] != pk_col and not r['column_name'].endswith('_id') and not r['column_name'].endswith('_by'):
            if r['data_type'] in ('character varying', 'text', 'varchar'):
                name_col = r['column_name']
                break

    rows = await conn.fetch(f"SELECT {pk_col} as id, {name_col} as name FROM {target_table} ORDER BY {name_col} ASC LIMIT 500")
    result = [dict(row) for row in rows]
    SCHEMA_CACHE["dropdown_lookups"][cache_key] = {'time': time.time(), 'data': result}
    
    return result


# ==========================================
#  AUTH ROUTES
# ==========================================
@app.route("/login", methods=["GET", "POST"])
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
                is_valid = await loop.run_in_executor(
                    None, partial(bcrypt.checkpw, password.encode('utf-8'), stored_pwd.encode('utf-8'))
                )
            except ValueError:
                if password == stored_pwd:
                    is_valid = True
                    new_hashed_bytes = await loop.run_in_executor(None, partial(bcrypt.hashpw, password.encode('utf-8'), bcrypt.gensalt()))
                    await conn.execute("UPDATE phc_users_t SET pus_pwd = $1 WHERE pus_user_id = $2", new_hashed_bytes.decode('utf-8'), user['pus_user_id'])

            if is_valid:
                session_id = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", session_id, user['pus_user_id'])

                payload = {
                    "user_id": user['pus_user_id'],
                    "user_type": user.get('pus_user_type') or 'STD',
                    "username": user['pus_user_name'],
                    "session_id": session_id,
                    "exp": datetime.now(timezone.utc) + timedelta(hours=12)
                }
                token = jwt.encode(payload, app.config.SECRET, algorithm="HS256")
                
                resp = response.json({"status": "success"})
                is_secure = not is_development
                resp.add_cookie("auth_token", token, httponly=True, samesite="Strict", secure=is_secure)

                await log_action(conn, user['pus_user_id'], "User logged in")
                return resp

    return response.json({"error": "Invalid Creds"}, status=401)


@app.route("/logout")
async def logout(request):
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
    return await render("dashboard.html", context={"stats": stats, "all_tables": allowed, "username": request.ctx.username, "user_id": request.ctx.user_id})


@app.get("/table/<table_name>")
@login_required
async def show_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed:
            return response.redirect("/")

        if table_name not in SCHEMA_CACHE["columns"]:
            cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
            SCHEMA_CACHE["columns"][table_name] = [dict(c) for c in cols]

        columns = [{"raw": r['column_name'], "label": make_human_readable(r['column_name'])} for r in SCHEMA_CACHE["columns"][table_name]]
        pk_column = await get_pk_column(conn, table_name)

        def get_col_priority(c_name):
            name = c_name.lower()
            if c_name == pk_column: return 0
            if name.endswith('status'): return 1
            if 'start_date' in name: return 2
            if 'end_date' in name: return 3
            if name in ('created_by', 'creation_date', 'last_update_date', 'last_updated_by', 'last_update_login') or \
               name.endswith('_created_by') or name.endswith('_modified_by') or \
               name.endswith('_created') or name.endswith('_modified'):
                return 100
            return 50

        columns = sorted(columns, key=lambda c: get_col_priority(c['raw']))

        search_query = request.args.get("q", "").strip()
        where_clause = ""
        params = []

        if search_query:
            params.append(f"%{search_query}%")
            cast_clauses = [f"CAST({c['raw']} AS TEXT) ILIKE $1" for c in columns]
            where_clause = f"WHERE {' OR '.join(cast_clauses)}"
            
        order_clause = f"ORDER BY {pk_column} DESC" if pk_column else "ORDER BY 1 DESC"
        
        rows = await conn.fetch(f"SELECT * FROM {table_name} {where_clause} {order_clause} LIMIT 100", *params)
        rows_dict = [dict(r) for r in rows]

        for col in columns:
            c_name = col['raw']
            if c_name == pk_column: continue

            options = await get_dropdown_options(conn, c_name)
            lookup = {str(opt['id']): opt['name'] for opt in options} if options else None

            for row in rows_dict:
                val = row.get(c_name)
                if val is not None and lookup and str(val) in lookup:
                    row[c_name] = f"{lookup[str(val)]} (ID: {val})"
                row[c_name] = mask_sensitive_data(c_name, row.get(c_name), request.ctx.user_type)

        return await render("table_view.html", context={"table_name": table_name, "table_title": make_human_readable(table_name), "columns": columns, "rows": rows_dict, "all_tables": allowed, "pk_column": pk_column, "user_id": request.ctx.user_id})


@app.route("/export/<table_name>")
@login_required
async def export_csv(request, table_name):
    pk_id = request.args.get("id")
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed:
            return response.text("Unauthorized", status=403)

        query = f"SELECT * FROM {table_name}"
        params = []
        pk_column = await get_pk_column(conn, table_name)

        if table_name not in SCHEMA_CACHE["columns"]:
            cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
            SCHEMA_CACHE["columns"][table_name] = [dict(c) for c in cols]

        if pk_id and pk_column:
            query += f" WHERE {pk_column} = $1"
            pk_type = next((r['data_type'] for r in SCHEMA_CACHE["columns"][table_name] if r['column_name'] == pk_column), 'integer')
            parsed_pk = pk_id if pk_type in ('character varying', 'text', 'varchar') else int(pk_id)
            params.append(parsed_pk)

        rows = await conn.fetch(query, *params)
        if not rows:
            return response.text("No data found")
        
        rows_dict = [dict(r) for r in rows]
        
        def get_col_priority(c_name):
            name = c_name.lower()
            if c_name == pk_column: return 0
            if name.endswith('status'): return 1
            if 'start_date' in name: return 2
            if 'end_date' in name: return 3
            if name in ('created_by', 'creation_date', 'last_update_date', 'last_updated_by', 'last_update_login') or \
               name.endswith('_created_by') or name.endswith('_modified_by') or \
               name.endswith('_created') or name.endswith('_modified'):
                return 100
            return 50
            
        sorted_cols = sorted(SCHEMA_CACHE["columns"][table_name], key=lambda c: get_col_priority(c['column_name']))

        for r in sorted_cols:
            c_name = r['column_name']
            options = await get_dropdown_options(conn, c_name)
            lookup = {str(opt['id']): opt['name'] for opt in options} if options else None

            for row in rows_dict:
                val = row.get(c_name)
                if val is not None and lookup and str(val) in lookup:
                    row[c_name] = f"{lookup[str(val)]} (ID: {val})"
                row[c_name] = mask_sensitive_data(c_name, row.get(c_name), request.ctx.user_type)

        output = io.StringIO()
        writer = csv.writer(output)
        ordered_keys = [c['column_name'] for c in sorted_cols]
        writer.writerow(ordered_keys)
        
        for row in rows_dict:
            writer.writerow([row.get(k) for k in ordered_keys])

        return response.text(output.getvalue(), headers={"Content-Disposition": f'attachment; filename="{table_name}_export.csv"', "Content-Type": "text/csv"})


# ==========================================
#  EDIT & CREATE ROUTES
# ==========================================
@app.get("/edit/<table_name>/<pk_val>")
@login_required
async def show_edit_form(request, table_name, pk_val):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed:
            return response.redirect("/")

        pk_column = await get_pk_column(conn, table_name)
        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        
        pk_type = next((r['data_type'] for r in col_rows if r['column_name'] == pk_column), 'integer')
        parsed_pk = str(pk_val) if pk_type in ('character varying', 'text', 'varchar') else int(pk_val)
        
        record = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", parsed_pk)
        columns = []

        for r in col_rows:
            c_name = r['column_name']
            if c_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by')) or c_name.lower() in WHO_COLS:
                continue

            is_pk = (c_name == pk_column)
            val = mask_sensitive_data(c_name, record[c_name], request.ctx.user_type)
            if isinstance(val, (date, datetime)):
                val = val.strftime('%Y-%m-%d')

            options = await get_dropdown_options(conn, c_name) if not is_pk else None
            is_req = (r['is_nullable'] == 'NO') and not is_pk

            columns.append({
                "column_name": c_name, "label": make_human_readable(c_name), "required": is_req, 
                "value": val, "data_type": r['data_type'], "options": options, "is_pk": is_pk
            })

        if table_name == 'phc_roles_t':
            screens = await conn.fetch("SELECT psn_screen_id as id, psn_screen_name as name FROM phc_screens_t WHERE psn_status = 'ACT'")
            assignments = await conn.fetch("SELECT prs_screen_id FROM phc_role_screen_assignment_t WHERE prs_role_id = $1", parsed_pk)
            assigned_val = ",".join([str(a['prs_screen_id']) for a in assignments])
            columns.append({"column_name": "pr_allowed_tables", "label": "Assigned Screens", "required": False, "value": assigned_val, "data_type": "virtual_checkbox", "options": [dict(r) for r in screens], "is_pk": False})

        if table_name == 'phc_users_t':
            roles = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status = 'ACT'")
            assignments = await conn.fetch("SELECT pua_role_id FROM phc_user_roles_assignment_t WHERE pua_user_id = $1", parsed_pk)
            assigned_val = ",".join([str(a['pua_role_id']) for a in assignments])
            columns.append({"column_name": "pu_assigned_roles", "label": "Assigned Roles", "required": False, "value": assigned_val, "data_type": "virtual_checkbox", "options": [dict(r) for r in roles], "is_pk": False})

        return await render("form_view.html", context={"table_name": table_name, "table_title": f"Edit {make_human_readable(table_name)}", "columns": columns, "all_tables": allowed, "pk_val": pk_val, "mode": "edit", "user_id": request.ctx.user_id})


@app.get("/new/<table_name>")
@login_required
async def show_add_form(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed:
            return response.redirect("/")

        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        pk_column = await get_pk_column(conn, table_name)

        columns = []
        for r in col_rows:
            c_name = r['column_name']
            if c_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by')) or c_name.lower() in WHO_COLS:
                continue

            is_pk = (c_name == pk_column)
            options = await get_dropdown_options(conn, c_name) if not is_pk else None
            is_req = (r['is_nullable'] == 'NO') and not is_pk
            
            val = ""
            if ('date' in r['data_type'] or 'timestamp' in r['data_type']) and 'end' not in c_name.lower() and not is_pk:
                val = datetime.now().strftime('%Y-%m-%d')
                
            columns.append({
                "column_name": c_name, "label": make_human_readable(c_name), "required": is_req, 
                "value": val, "data_type": r['data_type'], "options": options, "is_pk": is_pk
            })

        if table_name == 'phc_roles_t':
            screens = await conn.fetch("SELECT psn_screen_id as id, psn_screen_name as name FROM phc_screens_t WHERE psn_status = 'ACT'")
            columns.append({"column_name": "pr_allowed_tables", "label": "Assigned Screens", "required": False, "value": "", "data_type": "virtual_checkbox", "options": [dict(r) for r in screens], "is_pk": False})

        if table_name == 'phc_users_t':
            roles = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status = 'ACT'")
            columns.append({"column_name": "pu_assigned_roles", "label": "Assigned Roles", "required": False, "value": "", "data_type": "virtual_checkbox", "options": [dict(r) for r in roles], "is_pk": False})

        return await render("form_view.html", context={"table_name": table_name, "table_title": f"New {make_human_readable(table_name)}", "columns": columns, "all_tables": allowed, "mode": "create", "user_id": request.ctx.user_id})


async def _sanitize_payload(data, schema_map, pk_column, current_user_id, request_method):
    """Helper function to parse, clean, and cast HTMX JSON payload into database-ready dictionary."""
    clean_data = {}
    loop = asyncio.get_running_loop()

    virtual_screens = data.pop("pr_allowed_tables", None)
    if isinstance(virtual_screens, list): data["pr_allowed_tables"] = ",".join(virtual_screens)
    
    virtual_roles = data.pop("pu_assigned_roles", None)
    if isinstance(virtual_roles, list): data["pu_assigned_roles"] = ",".join(virtual_roles)

    for r in schema_map.values():
        c_name = r['column_name']
        if not data.get(c_name) and ('date' in r['data_type'] or 'timestamp' in r['data_type']):
            if 'end' not in c_name.lower():
                data[c_name] = datetime.now().strftime('%Y-%m-%d')

    for k, v in data.items():
        if v in ("", None) or k == pk_column:
            continue

        if k.lower().endswith(('_created', '_modified', '_created_by', '_modified_by')) or k.lower() in WHO_COLS:
            continue

        if k == 'pus_pwd':
            pwd_bytes = v.encode('utf-8')
            hashed_bytes = await loop.run_in_executor(None, partial(bcrypt.hashpw, pwd_bytes, bcrypt.gensalt()))
            v = hashed_bytes.decode('utf-8')

        col_info = schema_map.get(k, {})
        target_type = col_info.get('data_type', '').lower()
        max_len = col_info.get('character_maximum_length')

        if 'date' in target_type or 'timestamp' in target_type or (isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-'):
            if isinstance(v, str) and v:
                try:
                    v = datetime.strptime(v, '%Y-%m-%d')
                except ValueError:
                    try:
                        v = datetime.fromisoformat(v)
                    except ValueError:
                        pass

        if isinstance(v, str) and max_len is not None:
            if len(v) > max_len:
                if "status" in k and v.lower() == "active":
                    v = "ACT"
                elif "status" in k and v.lower() == "inactive":
                    v = "INA"
                else:
                    v = v[:max_len]

        if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str) and v.strip().isdigit():
            clean_data[k] = int(v)
        else:
            clean_data[k] = v

    for col_name in schema_map:
        c_lower = col_name.lower()
        
        if c_lower.endswith(('_created_by', '_modified_by', 'created_by', 'last_updated_by')):
            target_type = schema_map.get(col_name, {}).get('data_type', '').lower()
            if target_type in ('integer', 'bigint', 'numeric', 'smallint'):
                clean_data[col_name] = int(current_user_id) if str(current_user_id).isdigit() else None
            else:
                clean_data[col_name] = str(current_user_id)
                
        if request_method == "POST" and (c_lower.endswith('_created') or c_lower == 'creation_date'):
            clean_data[col_name] = datetime.now()
        if c_lower.endswith('_modified') or c_lower == 'last_update_date':
            clean_data[col_name] = datetime.now()

    return clean_data


@app.post("/api/<table_name>", name="create_row")
@app.put("/api/<table_name>/<pk_val>", name="update_row")
@login_required
async def save_data(request, table_name, pk_val=None):
    data = request.json
    current_user_id = request.ctx.user_id
    user_type = request.ctx.user_type

    if user_type != 'ADM':
        data.pop('pus_user_type', None)
        if table_name == 'phc_users_t' and request.method == "PUT" and str(pk_val) != str(current_user_id):
            data.pop('pus_pwd', None)

    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, current_user_id, user_type)
        if table_name not in allowed:
            return response.json({"error": "Unauthorized API Access"}, status=403)

        virtual_screens = data.pop("pr_allowed_tables", None)
        virtual_roles = data.pop("pu_assigned_roles", None)
        pk_column = await get_pk_column(conn, table_name)

        schema_rows = await conn.fetch("SELECT column_name, data_type, character_maximum_length, is_nullable FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {r['column_name']: r for r in schema_rows}
        
        clean_data = await _sanitize_payload(data, schema_map, pk_column, current_user_id, request.method)

        async with conn.transaction():
            if request.method == "POST":
                max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                target_id = (int(max_val) + 1) if max_val else 1
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                
                clean_data[pk_column] = str(target_id) if pk_type in ('character varying', 'text', 'varchar') else target_id
                
                cols = list(clean_data.keys())
                vals = list(clean_data.values())
                placeholders = ', '.join([f'${i+1}' for i in range(len(vals))])
                await conn.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})", *vals)
            else:
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                target_id = str(pk_val) if pk_type in ('character varying', 'text', 'varchar') else int(pk_val)
                
                set_clauses = [f"{k} = ${i+2}" for i, k in enumerate(clean_data.keys())]
                vals = list(clean_data.values())
                if set_clauses:
                    await conn.execute(f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = $1", target_id, *vals)

            if table_name == 'phc_roles_t' and virtual_screens is not None:
                await conn.execute("DELETE FROM phc_role_screen_assignment_t WHERE prs_role_id = $1", target_id)
                if virtual_screens:
                    prs_pk_col = await get_pk_column(conn, 'phc_role_screen_assignment_t')
                    max_prs = await conn.fetchval(f"SELECT MAX({prs_pk_col}) FROM phc_role_screen_assignment_t") if prs_pk_col else 0
                    next_prs = (int(max_prs) + 1) if max_prs else 1
                    for s_id in virtual_screens.split(','):
                        if s_id.strip() and prs_pk_col:
                            await conn.execute("""
                                INSERT INTO phc_role_screen_assignment_t 
                                ({0}, prs_company_id, prs_role_id, prs_screen_id, prs_start_date, prs_status, prs_created_by, prs_modified_by, prs_created, prs_modified) 
                                VALUES ($1, 1001, $2, $3, CURRENT_DATE, 'ACT', $4, $4, NOW(), NOW())
                            """.format(prs_pk_col), next_prs, target_id, int(s_id), str(current_user_id))
                            next_prs += 1

            if table_name == 'phc_users_t' and virtual_roles is not None:
                await conn.execute("DELETE FROM phc_user_roles_assignment_t WHERE pua_user_id = $1", target_id)
                if virtual_roles:
                    pua_pk_col = await get_pk_column(conn, 'phc_user_roles_assignment_t')
                    max_pua = await conn.fetchval(f"SELECT MAX({pua_pk_col}) FROM phc_user_roles_assignment_t") if pua_pk_col else 0
                    next_pua = (int(max_pua) + 1) if max_pua else 1
                    for r_id in virtual_roles.split(','):
                        if r_id.strip() and pua_pk_col:
                            await conn.execute("""
                                INSERT INTO phc_user_roles_assignment_t 
                                ({0}, pua_company_id, pua_user_id, pua_role_id, pua_start_date, pua_status, pua_created_by, pua_modified_by, pua_created, pua_modified) 
                                VALUES ($1, 1001, $2, $3, CURRENT_DATE, 'ACT', $4, $4, NOW(), NOW())
                            """.format(pua_pk_col), next_pua, target_id, int(r_id), str(current_user_id))
                            next_pua += 1

        if request.headers.get("HX-Request"):
            return response.html(f"""
                <script>
                    sessionStorage.setItem('pendingToast', JSON.stringify({{"msg": "Record saved successfully!", "type": "success"}}));
                    localStorage.removeItem('draft_{table_name}');
                    window.location.href = "/table/{table_name}";
                </script>
            """)

        return response.json({"status": "success"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # ENTERPRISE FIX: Enables Fast mode which spins up multiple CPU workers automatically for massive concurrency
    app.run(host="0.0.0.0", port=port, debug=is_development, fast=True)
