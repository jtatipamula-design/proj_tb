from sanic import Sanic, response
from sanic_ext import Extend, render
import asyncpg
import os
import bcrypt
import csv
import io
import time
import jwt
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from functools import wraps
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Sanic("ERP_System")
app.config.SECRET = os.environ.get("SECRET_KEY", "DEFAULT_FALLBACK_SECRET")
app.config.TEMPLATING_PATH_TO_TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")
Extend(app)

CLOUD_DB_URL = os.environ.get("DB_URL")

# ==========================================
#  SECURITY MODULE 1: RATE LIMITER
# ==========================================
RATE_LIMIT_WINDOW = 60  
MAX_REQUESTS = 120      
ip_tracker = defaultdict(list)

@app.on_request
async def rate_limiter(request):
    ip = request.remote_addr or request.ip
    now = time.time()
    ip_tracker[ip] = [t for t in ip_tracker[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(ip_tracker[ip]) >= MAX_REQUESTS:
        return response.json({"error": "Rate limit exceeded. Please slow down."}, status=429)
    ip_tracker[ip].append(now)

# ==========================================
#  SECURITY MODULE 2: SECURE HEADERS
# ==========================================
@app.on_response
async def add_security_headers(request, resp):
    if resp:
        resp.headers['X-Frame-Options'] = 'SAMEORIGIN'
        resp.headers['X-Content-Type-Options'] = 'nosniff'
        resp.headers['X-XSS-Protection'] = '1; mode=block'
        resp.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'

# ==========================================
# SECURITY MODULE 3: JWT & RBAC AUTH
# ==========================================
def login_required(wrapped):
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        token = request.cookies.get("auth_token")
        if not token: return response.redirect("/login")
        try:
            payload = jwt.decode(token, app.config.SECRET, algorithms=["HS256"])
            request.ctx.user_id = payload.get("user_id") 
            request.ctx.user_type = payload.get("user_type") 
            request.ctx.username = payload.get("username", "User")
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return response.redirect("/login")
        return await wrapped(request, *args, **kwargs)
    return decorator


# --- DB SETUP & AUTO-MIGRATION ---
@app.before_server_start
async def setup_db(app, loop):
    try:
        if CLOUD_DB_URL:
            app.ctx.pool = await asyncpg.create_pool(dsn=CLOUD_DB_URL, statement_cache_size=0)
        else:
            local_password = os.environ.get("LOCAL_DB_PASSWORD")
            app.ctx.pool = await asyncpg.create_pool(
                user='postgres', password=local_password, database='tablesproj', host='localhost', statement_cache_size=0
            )
        
        async with app.ctx.pool.acquire() as conn:
            # Ensure basic company exists
            if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_companies_t WHERE pcp_company_id = 1001)"):
                await conn.execute("INSERT INTO phc_companies_t (pcp_company_id, pcp_company_code, pcp_company_name, pcp_created, pcp_modified, pcp_created_by, pcp_modified_by, pcp_status) OVERRISING SYSTEM VALUE VALUES (1001, 'SYS', 'System Admin Company', NOW(), NOW(), 'System', 'System', 'ACT')")
            
            # Upgrade the users table to have user type if it's missing
            type_col_exists = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='phc_users_t' AND column_name='pus_user_type')")
            if not type_col_exists:
                await conn.execute("ALTER TABLE phc_users_t ADD COLUMN pus_user_type VARCHAR(3) DEFAULT 'STD'")

            # Ensure the Admin user exists and has the 'ADM' type
            if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_users_t WHERE pus_user_name = 'admin')"):
                hashed = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode('utf-8')
                await conn.execute("INSERT INTO phc_users_t (pus_company_id, pus_user_name, pus_full_name, pus_pwd, pus_status, pus_created, pus_modified, pus_created_by, pus_modified_by, pus_start_date, pus_user_type) VALUES (1001, 'admin', 'System Admin', $1, 'ACT', NOW(), NOW(), 'System', 'System', NOW(), 'ADM')", hashed)
            else:
                await conn.execute("UPDATE phc_users_t SET pus_user_type = 'ADM' WHERE pus_user_name = 'admin'")

            # Clean up legacy columns from the old testing phase
            try: await conn.execute("ALTER TABLE phc_users_t DROP COLUMN IF EXISTS pus_role")
            except: pass
            try: await conn.execute("ALTER TABLE phc_roles_t DROP COLUMN IF EXISTS pr_allowed_tables")
            except: pass
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'pool'): await app.ctx.pool.close()

# --- HELPERS ---
async def log_action(conn, user_id, action_desc):
    try:
        await conn.execute("INSERT INTO phc_user_log_t (pul_parent, pul_description, pul_created, pul_modified, pul_created_by, pul_modified_by) VALUES ($1, $2, NOW(), NOW(), 'System', 'System')", int(user_id) if user_id else None, action_desc)
    except: pass

def make_human_readable(text):
    text = text.replace("phc_", "").replace("_t", "")
    if len(text) > 4 and text[3] == '_': text = text[4:]
    return text.replace("_", " ").title()

# ==========================================
#  SECURITY MODULE 4: RELATIONAL RBAC ENGINE
# ==========================================
async def get_allowed_tables(conn, user_id, user_type):
    rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'phc_%'")
    all_tables = [r['table_name'] for r in rows]

    if user_type == 'ADM': 
        return all_tables

    query = """
        SELECT LOWER(s.psn_screen_code) as table_name
        FROM phc_user_roles_assignment_t ura
        JOIN phc_roles_t r ON ura.pua_role_id = r.prl_role_id
        JOIN phc_role_screen_assignment_t rsa ON r.prl_role_id = rsa.prs_role_id
        JOIN phc_screens_t s ON rsa.prs_screen_id = s.psn_screen_id
        WHERE ura.pua_user_id = $1
          AND ura.pua_status = 'ACT'
          AND r.prl_status = 'ACT'
          AND rsa.prs_status = 'ACT'
          AND s.psn_status = 'ACT'
    """
    assigned_rows = await conn.fetch(query, int(user_id))
    allowed_tables = [r['table_name'] for r in assigned_rows]
    return [t for t in all_tables if t in allowed_tables]


# --- SMART DROPDOWN HELPER ---
async def get_dropdown_options(conn, column_name):
    if not column_name.endswith('_id'): return None
    
    keyword_map = {
        'company': 'phc_companies_t', 'dept': 'phc_dept_t', 'department': 'phc_dept_t', 
        'user': 'phc_users_t', 'emp': 'phc_emp_t', 'employee': 'phc_emp_t',
        'app': 'phc_apps_t', 'org': 'phc_orgs_t', 'organization': 'phc_orgs_t',
        'role': 'phc_roles_t', 'screen': 'phc_screens_t'
    }
    
    target_table = None
    for key, table in keyword_map.items():
        if key in column_name: target_table = table; break
    if not target_table: return None
    
    exists = await conn.fetchval("SELECT to_regclass($1)", target_table)
    if not exists: return None
    
    pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", target_table)
    if not pk_row: return None
    pk_col = pk_row['column_name']
    
    cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND data_type IN ('character varying', 'text') ORDER BY ordinal_position", target_table)
    name_col = pk_col 
    for r in cols:
        if r['column_name'] != pk_col and not r['column_name'].endswith('_id') and not r['column_name'].endswith('_by'):
            name_col = r['column_name']; break 
            
    rows = await conn.fetch(f"SELECT {pk_col} as id, {name_col} as name FROM {target_table} ORDER BY {name_col} ASC LIMIT 100")
    return [dict(row) for row in rows]

# --- AUTH ROUTES ---
@app.route("/login", methods=["GET", "POST"])
async def handle_login(request):
    if request.method == "GET": return await render("login.html")
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT pus_user_id, pus_pwd, pus_user_type, pus_user_name FROM phc_users_t WHERE pus_user_name = $1", username)
        
        if user:
            stored_pwd = user['pus_pwd']
            is_valid = False
            
            try:
                # 1. Try standard bcrypt comparison
                if bcrypt.checkpw(password.encode('utf-8'), stored_pwd.encode('utf-8')):
                    is_valid = True
            except ValueError:
                # 2. Catch the "Invalid salt" error if DB contains plain text
                if password == stored_pwd:
                    is_valid = True
                    
            if is_valid:
                payload = {
                    "user_id": user['pus_user_id'], 
                    "user_type": user.get('pus_user_type') or 'STD', 
                    "username": user['pus_user_name'],
                    "exp": datetime.now(timezone.utc) + timedelta(hours=12)
                }
                token = jwt.encode(payload, app.config.SECRET, algorithm="HS256")
                resp = response.json({"status": "success"})
                resp.add_cookie("auth_token", token, httponly=True, samesite="Lax")
                await log_action(conn, user['pus_user_id'], f"User logged in")
                return resp
            
    return response.json({"error": "Invalid Creds"}, status=401)

@app.route("/logout")
async def logout(request):
    resp = response.redirect("/login")
    resp.delete_cookie("auth_token")
    return resp

# --- DASHBOARD & MAIN ROUTES ---
@app.route("/")
@login_required
async def dashboard(request):
    async with app.ctx.pool.acquire() as conn:
        stats = {
            "emp_count": await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t"),
            "comp_count": await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status = 'ACT'"),
            "dept_count": await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t"),
            "app_count": await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t")
        }
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
    return await render("dashboard.html", context={"stats": stats, "all_tables": allowed, "username": request.ctx.username})

@app.get("/table/<table_name>")
@login_required
async def show_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.redirect("/")
        
        col_rows = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        columns = [{"raw": r['column_name'], "label": make_human_readable(r['column_name'])} for r in col_rows]
        
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None
        
        rows = await conn.fetch(f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT 100")
        return await render("table_view.html", context={"table_name": table_name, "table_title": make_human_readable(table_name), "columns": columns, "rows": rows, "all_tables": allowed, "pk_column": pk_column})

@app.route("/export/<table_name>")
@login_required
async def export_csv(request, table_name):
    pk_id = request.args.get("id")
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.text("Unauthorized", status=403)
        
        query = f"SELECT * FROM {table_name}"
        params = []
        if pk_id:
            pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
            if pk_row:
                query += f" WHERE {pk_row['column_name']} = $1"
                params.append(int(pk_id))

        rows = await conn.fetch(query, *params)
        if not rows: return response.text("No data found")
        output = io.StringIO(); writer = csv.writer(output); writer.writerow(rows[0].keys())
        for row in rows: writer.writerow(row.values())
        return response.text(output.getvalue(), headers={"Content-Disposition": f'attachment; filename="{table_name}_export.csv"', "Content-Type": "text/csv"})


# --- EDIT & CREATE ROUTES ---
@app.get("/edit/<table_name>/<pk_val>")
@login_required
async def show_edit_form(request, table_name, pk_val):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.redirect("/")
        
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name']
        
        record = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", int(pk_val))
        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        columns = []
        
        for r in col_rows:
            c_name = r['column_name']
            if c_name == pk_column or c_name.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            val = record[c_name]
            if isinstance(val, (date, datetime)): val = val.strftime('%Y-%m-%d')
            options = await get_dropdown_options(conn, c_name)
            is_req = (r['is_nullable'] == 'NO')
            columns.append({"column_name": c_name, "label": make_human_readable(c_name), "required": is_req, "value": val, "data_type": r['data_type'], "options": options})

        if table_name == 'phc_roles_t':
            screens = await conn.fetch("SELECT psn_screen_id as id, psn_screen_name as name FROM phc_screens_t WHERE psn_status = 'ACT'")
            assignments = await conn.fetch("SELECT prs_screen_id FROM phc_role_screen_assignment_t WHERE prs_role_id = $1", int(pk_val))
            assigned_val = ",".join([str(a['prs_screen_id']) for a in assignments])
            columns.append({
                "column_name": "pr_allowed_tables", "label": "Assigned Screens", "required": False,
                "value": assigned_val, "data_type": "virtual_checkbox", "options": [dict(r) for r in screens]
            })

        if table_name == 'phc_users_t':
            roles = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status = 'ACT'")
            assignments = await conn.fetch("SELECT pua_role_id FROM phc_user_roles_assignment_t WHERE pua_user_id = $1", int(pk_val))
            assigned_val = ",".join([str(a['pua_role_id']) for a in assignments])
            columns.append({
                "column_name": "pu_assigned_roles", "label": "Assigned Roles", "required": False,
                "value": assigned_val, "data_type": "virtual_checkbox", "options": [dict(r) for r in roles]
            })

        return await render("form_view.html", context={"table_name": table_name, "table_title": f"Edit {make_human_readable(table_name)}", "columns": columns, "all_tables": allowed, "pk_val": pk_val, "mode": "edit"})

@app.get("/new/<table_name>")
@login_required
async def show_add_form(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, request.ctx.user_id, request.ctx.user_type)
        if table_name not in allowed: return response.redirect("/")
        
        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None

        columns = []

        for r in col_rows:
            c_name = r['column_name']
            if c_name == pk_column or c_name.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            options = await get_dropdown_options(conn, c_name)
            is_req = (r['is_nullable'] == 'NO')
            columns.append({"column_name": c_name, "label": make_human_readable(c_name), "required": is_req, "value": "", "data_type": r['data_type'], "options": options})
            
        if table_name == 'phc_roles_t':
            screens = await conn.fetch("SELECT psn_screen_id as id, psn_screen_name as name FROM phc_screens_t WHERE psn_status = 'ACT'")
            columns.append({
                "column_name": "pr_allowed_tables", "label": "Assigned Screens", "required": False,
                "value": "", "data_type": "virtual_checkbox", "options": [dict(r) for r in screens]
            })

        if table_name == 'phc_users_t':
            roles = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status = 'ACT'")
            columns.append({
                "column_name": "pu_assigned_roles", "label": "Assigned Roles", "required": False,
                "value": "", "data_type": "virtual_checkbox", "options": [dict(r) for r in roles]
            })

        return await render("form_view.html", context={"table_name": table_name, "table_title": f"New {make_human_readable(table_name)}", "columns": columns, "all_tables": allowed, "mode": "create"})

# --- API SAVE ---
@app.post("/api/<table_name>", name="create_row")
@app.put("/api/<table_name>/<pk_val>", name="update_row")
@login_required
async def save_data(request, table_name, pk_val=None):
    data = request.json
    current_user_id = request.ctx.user_id 
    user_type = request.ctx.user_type
    
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn, current_user_id, user_type)
        if table_name not in allowed: return response.json({"error": "Unauthorized API Access"}, status=403)

        virtual_screens = data.pop("pr_allowed_tables", None)
        virtual_roles = data.pop("pu_assigned_roles", None)

        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name']
        
        schema_rows = await conn.fetch("SELECT column_name, data_type, character_maximum_length, is_nullable FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {r['column_name']: r for r in schema_rows}
        clean_data = {}
        
        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column: continue 
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue

            if k == 'pus_pwd':
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')

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
                    if "status" in k and v.lower() == "active": v = "ACT"
                    elif "status" in k and v.lower() == "inactive": v = "INA"
                    else: v = v[:max_len]
            
            if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v

        for col_name in schema_map:
            if col_name.endswith(('_created_by', '_modified_by')): clean_data[col_name] = str(current_user_id)
            if col_name.endswith('_created') and request.method == "POST": clean_data[col_name] = datetime.now()
            if col_name.endswith('_modified'): clean_data[col_name] = datetime.now() 

        if request.method == "POST":
            if pk_column:
                max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                new_id = (int(max_val) + 1) if max_val else 1
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                if pk_type in ('character varying', 'text', 'varchar'): clean_data[pk_column] = str(new_id)
                else: clean_data[pk_column] = new_id
            
            cols = list(clean_data.keys()); vals = list(clean_data.values())
            await conn.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join([f'${i+1}' for i in range(len(vals))])})", *vals)
        else:
            set_clauses = [f"{k} = ${i+2}" for i, k in enumerate(clean_data.keys())]; vals = list(clean_data.values())
            await conn.execute(f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = $1", int(pk_val), *vals)

        target_id = new_id if request.method == "POST" else int(pk_val)
        
        if table_name == 'phc_roles_t' and virtual_screens is not None:
            await conn.execute("DELETE FROM phc_role_screen_assignment_t WHERE prs_role_id = $1", target_id)
            if virtual_screens:
                for s_id in virtual_screens.split(','):
                    if s_id.strip():
                        await conn.execute("""
                            INSERT INTO phc_role_screen_assignment_t 
                            (prs_company_id, prs_role_id, prs_screen_id, prs_status, prs_created_by, prs_modified_by, prs_created, prs_modified) 
                            VALUES (1001, $1, $2, 'ACT', $3, $3, NOW(), NOW())
                        """, target_id, int(s_id), str(current_user_id))

        if table_name == 'phc_users_t' and virtual_roles is not None:
            await conn.execute("DELETE FROM phc_user_roles_assignment_t WHERE pua_user_id = $1", target_id)
            if virtual_roles:
                for r_id in virtual_roles.split(','):
                    if r_id.strip():
                        await conn.execute("""
                            INSERT INTO phc_user_roles_assignment_t 
                            (pua_company_id, pua_user_id, pua_role_id, pua_status, pua_created_by, pua_modified_by, pua_created, pua_modified) 
                            VALUES (1001, $1, $2, 'ACT', $3, $3, NOW(), NOW())
                        """, target_id, int(r_id), str(current_user_id))

        return response.json({"status": "success"})

if __name__ == "__main__":
    # Render assigns the port dynamically using the PORT env variable
    port = int(os.environ.get("PORT", 8000))
    is_debug = os.environ.get("ENVIRONMENT") == "development"
    app.run(host="0.0.0.0", port=port, debug=is_debug, single_process=True)
