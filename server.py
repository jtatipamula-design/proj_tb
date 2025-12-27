from sanic import Sanic, response
from sanic_ext import Extend, render
import asyncpg
import os
import bcrypt
from datetime import datetime
from functools import wraps

app = Sanic("ERP_System")
app.config.SECRET = "KEEP_THIS_SECRET_SAFE"

# Enable Jinja2 templating
app.config.TEMPLATING_PATH_TO_TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")
Extend(app)

# --- DATABASE CONFIGURATION ---
CLOUD_DB_URL = os.environ.get("DB_URL")

@app.before_server_start
async def setup_db(app, loop):
    try:
        if CLOUD_DB_URL:
            app.ctx.pool = await asyncpg.create_pool(dsn=CLOUD_DB_URL)
            print("✅ Connected to Cloud Database (Neon)")
        else:
            app.ctx.pool = await asyncpg.create_pool(
                user='postgres', password='Jay25092005', database='tablesproj', host='localhost'
            )
            print("✅ Connected to Local Database")
            
        # --- AUTO-CREATE DEFAULT DATA ---
        async with app.ctx.pool.acquire() as conn:
            # 1. Ensure System Company Exists (ID 1001)
            company_exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_companies_t WHERE pcp_company_id = 1001)")
            if not company_exists:
                print("⚠️ No System Company found. Creating 'System Admin Company'...")
                await conn.execute("""
                    INSERT INTO phc_companies_t 
                    (pcp_company_id, pcp_company_code, pcp_company_name, pcp_created, pcp_modified, pcp_created_by, pcp_modified_by, pcp_status)
                    OVERRIDING SYSTEM VALUE
                    VALUES (1001, 'SYS', 'System Admin Company', NOW(), NOW(), 'System', 'System', 'ACT')
                """)
                print("✅ Created Default Company (ID: 1001)")

            # 2. Ensure Admin User Exists
            user_exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_users_t WHERE pus_user_name = 'admin')")
            if not user_exists:
                print("⚠️ No Admin user found. Creating default 'admin' user...")
                hashed = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode('utf-8')
                await conn.execute("""
                    INSERT INTO phc_users_t 
                    (pus_company_id, pus_user_name, pus_full_name, pus_pwd, pus_status, pus_created, pus_modified, pus_created_by, pus_modified_by, pus_start_date)
                    VALUES (1001, 'admin', 'System Admin', $1, 'ACT', NOW(), NOW(), 'System', 'System', NOW())
                """, hashed)
                print("✅ Default user created: admin / admin123")

    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'pool'): await app.ctx.pool.close()

# --- LOGGING HELPER (NEW) ---
async def log_action(conn, user_id, action_desc):
    """Inserts a record into phc_user_log_t"""
    try:
        # We assume 'user_id' acts as the 'parent' link or just text description
        await conn.execute("""
            INSERT INTO phc_user_log_t 
            (pul_parent, pul_description, pul_created, pul_modified, pul_created_by, pul_modified_by)
            VALUES ($1, $2, NOW(), NOW(), 'System', 'System')
        """, int(user_id) if user_id else None, action_desc)
    except Exception as e:
        print(f"⚠️ Failed to log action: {e}")

# --- AUTH MIDDLEWARE ---
def login_required(wrapped):
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        token = request.cookies.get("auth_token")
        if not token:
            return response.redirect("/login")
        return await wrapped(request, *args, **kwargs)
    return decorator

# --- HELPERS ---
async def get_allowed_tables(conn):
    rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'phc_%'")
    return [r['table_name'] for r in rows]

def make_human_readable(text):
    text = text.replace("phc_", "").replace("_t", "")
    if len(text) > 4 and text[3] == '_': text = text[4:]
    return text.replace("_", " ").title()

async def get_dropdown_options(conn, fk_column_name):
    try:
        base = fk_column_name[:-3]
        entity = base[4:] if len(base) > 4 and base[3] == '_' else base
        manual_map = {
            "dept": "phc_dept_t", "org": "phc_orgs_t", "services": "phc_services_t",
            "cost_center": "phc_cost_center_t", "app": "phc_apps_t", "apps": "phc_apps_t"
        }
        target_table = manual_map.get(entity)
        if not target_table:
            possible_names = {f"phc_{entity}_t", f"phc_{entity}s_t", f"phc_{entity}es_t"}
            if entity.endswith('y'): possible_names.add(f"phc_{entity[:-1]}ies_t")
            all_tables = await get_allowed_tables(conn)
            for real_table in all_tables:
                if real_table in possible_names:
                    target_table = real_table
                    break
        if not target_table: return None
        cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1", target_table)
        col_names = [r['column_name'] for r in cols]
        pk_col = next((c for c in col_names if c.endswith('_id')), None)
        label_col = next((c for c in col_names if any(x in c for x in ['name', 'title', 'code', 'desc'])), pk_col)
        if not pk_col: return None
        rows = await conn.fetch(f"SELECT {pk_col}, {label_col} FROM {target_table} ORDER BY {label_col} LIMIT 100")
        return [{"id": r[pk_col], "name": str(r[label_col])} for r in rows]
    except: return None

# --- AUTH ROUTES ---

@app.route("/login", methods=["GET"])
async def show_login(request):
    return await render("login.html")

@app.route("/login", methods=["POST"])
async def process_login(request):
    data = request.json
    username = data.get("username")
    password = data.get("password")
    
    async with app.ctx.pool.acquire() as conn:
        # Find user
        user = await conn.fetchrow("SELECT pus_user_id, pus_pwd FROM phc_users_t WHERE pus_user_name = $1", username)
        
        if user:
            # Verify Password
            stored_hash = user['pus_pwd'].encode('utf-8')
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash):
                # SUCCESS
                resp = response.json({"status": "success"})
                
                # --- FIX START: Use add_cookie instead of dictionary assignment ---
                resp.add_cookie("auth_token", str(user['pus_user_id']), httponly=True)
                # --- FIX END ---
                
                # LOG LOGIN ACTION
                await log_action(conn, user['pus_user_id'], f"User '{username}' logged in.")
                return resp
    
    return response.json({"error": "Invalid Username or Password"}, status=401)

@app.route("/logout")
async def logout(request):
    resp = response.redirect("/login")
    if "auth_token" in request.cookies:
        del resp.cookies["auth_token"]
    return resp

# --- APP ROUTES ---

@app.route("/")
@login_required
async def index(request):
    async with app.ctx.pool.acquire() as conn:
        tables = await get_allowed_tables(conn)
    if not tables: return response.text("Connected to DB, but no 'phc_' tables found.")
    return response.redirect(f"/table/{tables[0]}")

@app.get("/table/<table_name>")
@login_required
async def show_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.text("Invalid Table", status=400)
        col_rows = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        columns = [{"raw": r['column_name'], "label": make_human_readable(r['column_name'])} for r in col_rows]
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None
        data_rows = await conn.fetch(f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT 100")
        return await render("table_view.html", context={"table_name": table_name, "table_title": make_human_readable(table_name), "columns": columns, "rows": data_rows, "all_tables": allowed, "pk_column": pk_column})

@app.get("/new/<table_name>")
@login_required
async def show_add_form(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.text("Invalid Table", status=400)
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None
        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        columns = []
        for r in col_rows:
            c_name = r['column_name']
            if c_name == pk_column or c_name.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            col_data = {"column_name": c_name, "data_type": r['data_type'], "label": make_human_readable(c_name), "required": (r['is_nullable'] == 'NO'), "options": None}
            if c_name.endswith('_id'): 
                opts = await get_dropdown_options(conn, c_name)
                if opts: col_data["options"] = opts
            columns.append(col_data)
        return await render("form_view.html", context={"table_name": table_name, "table_title": make_human_readable(table_name), "columns": columns, "all_tables": allowed})

@app.post("/api/<table_name>")
@login_required
async def create_row(request, table_name):
    data = request.json
    user_id = request.cookies.get("auth_token") # Get Current User ID

    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.json({"error": "Invalid table"}, status=400)
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None
        schema_map = {r['column_name']: r for r in await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)}
        clean_data = {}
        for k, v in data.items():
            if v == "" or v is None or k == pk_column or k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            target_type = schema_map.get(k, {}).get('data_type')
            if target_type == 'date' and isinstance(v, str):
                try: clean_data[k] = datetime.strptime(v, '%Y-%m-%d').date()
                except: return response.json({"error": f"Invalid date: {v}"}, status=400)
            elif target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                v = v.strip()
                clean_data[k] = int(v) if v.isdigit() or (v.startswith('-') and v[1:].isdigit()) else v
            else: clean_data[k] = v
        if pk_column:
            max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
            clean_data[pk_column] = (int(max_val) + 1) if max_val is not None else 1
        for col_name in schema_map:
            if col_name.endswith(('_created_by', '_modified_by')): clean_data[col_name] = 'System' 

        if not clean_data: return response.json({"error": "No valid data provided"}, status=400)
        try:
            cols, vals = list(clean_data.keys()), list(clean_data.values())
            await conn.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join([f'${i+1}' for i in range(len(vals))])})", *vals)
            
            # LOG CREATION ACTION
            await log_action(conn, user_id, f"Created new record in {table_name} (ID: {clean_data.get(pk_column, '?')})")
            
            return response.json({"status": "success"})
        except Exception as e: return response.json({"error": str(e)}, status=500)

@app.delete("/api/<table_name>/<pk_val>")
@login_required
async def delete_row(request, table_name, pk_val):
    user_id = request.cookies.get("auth_token") # Get Current User ID

    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.json({"error": "Invalid table"}, status=400)
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        if not pk_row: return response.json({"error": "Table has no Primary Key"}, status=400)
        pk_column = pk_row['column_name']
        col_type = await conn.fetchval("SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2", table_name, pk_column)
        try:
            if col_type in ('integer', 'bigint', 'smallint', 'numeric'): pk_val = int(pk_val)
            await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", pk_val)
            
            # LOG DELETE ACTION
            await log_action(conn, user_id, f"Deleted record {pk_val} from {table_name}")
            
            return response.json({"status": "deleted"})
        except Exception as e: return response.json({"error": str(e)}, status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, single_process=True)
