from sanic import Sanic, response
from sanic_ext import Extend, render
import asyncpg
import os
import bcrypt
import csv
import io
from datetime import datetime, date
from functools import wraps

app = Sanic("ERP_System")
app.config.SECRET = "KEEP_THIS_SECRET_SAFE"

app.config.TEMPLATING_PATH_TO_TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")
Extend(app)

CLOUD_DB_URL = os.environ.get("DB_URL")

@app.before_server_start
async def setup_db(app, loop):
    try:
        if CLOUD_DB_URL:
            app.ctx.pool = await asyncpg.create_pool(dsn=CLOUD_DB_URL)
        else:
            app.ctx.pool = await asyncpg.create_pool(
                user='postgres', password='Jay25092005', database='tablesproj', host='localhost'
            )
        
        # Auto-create Default Data (Admin/Company)
        async with app.ctx.pool.acquire() as conn:
            if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_companies_t WHERE pcp_company_id = 1001)"):
                await conn.execute("INSERT INTO phc_companies_t (pcp_company_id, pcp_company_code, pcp_company_name, pcp_created, pcp_modified, pcp_created_by, pcp_modified_by, pcp_status) OVERRIDING SYSTEM VALUE VALUES (1001, 'SYS', 'System Admin Company', NOW(), NOW(), 'System', 'System', 'ACT')")
            if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM phc_users_t WHERE pus_user_name = 'admin')"):
                hashed = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode('utf-8')
                await conn.execute("INSERT INTO phc_users_t (pus_company_id, pus_user_name, pus_full_name, pus_pwd, pus_status, pus_created, pus_modified, pus_created_by, pus_modified_by, pus_start_date) VALUES (1001, 'admin', 'System Admin', $1, 'ACT', NOW(), NOW(), 'System', 'System', NOW())", hashed)
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

def login_required(wrapped):
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        if not request.cookies.get("auth_token"): return response.redirect("/login")
        return await wrapped(request, *args, **kwargs)
    return decorator

async def get_allowed_tables(conn):
    rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'phc_%'")
    return [r['table_name'] for r in rows]

def make_human_readable(text):
    text = text.replace("phc_", "").replace("_t", "")
    if len(text) > 4 and text[3] == '_': text = text[4:]
    return text.replace("_", " ").title()

# --- AUTH ROUTES ---
@app.route("/login", methods=["GET", "POST"])
async def handle_login(request):
    if request.method == "GET": return await render("login.html")
    data = request.json
    async with app.ctx.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT pus_user_id, pus_pwd FROM phc_users_t WHERE pus_user_name = $1", data.get("username"))
        if user and bcrypt.checkpw(data.get("password").encode(), user['pus_pwd'].encode()):
            resp = response.json({"status": "success"})
            resp.add_cookie("auth_token", str(user['pus_user_id']), httponly=True)
            await log_action(conn, user['pus_user_id'], f"User logged in")
            return resp
    return response.json({"error": "Invalid Creds"}, status=401)

# --- DASHBOARD & MAIN ROUTES ---

@app.route("/")
@login_required
async def dashboard(request):
    async with app.ctx.pool.acquire() as conn:
        # Get Stats
        stats = {
            "emp_count": await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t"),
            "comp_count": await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status = 'ACT'"),
            "dept_count": await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t"),
            "app_count": await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t")
        }
        allowed = await get_allowed_tables(conn)
    return await render("dashboard.html", context={"stats": stats, "all_tables": allowed})

@app.get("/table/<table_name>")
@login_required
async def show_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.redirect("/")
        
        # Get Columns
        col_rows = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        columns = [{"raw": r['column_name'], "label": make_human_readable(r['column_name'])} for r in col_rows]
        
        # Get Primary Key
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None
        
        # Get Data
        rows = await conn.fetch(f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT 100")
        
        return await render("table_view.html", context={
            "table_name": table_name, "table_title": make_human_readable(table_name),
            "columns": columns, "rows": rows, "all_tables": allowed, "pk_column": pk_column
        })

# --- CSV EXPORT ROUTE ---
@app.route("/export/<table_name>")
@login_required
async def export_csv(request, table_name):
    pk_id = request.args.get("id") # Check if exporting single row
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.text("Error", status=400)
        
        # Build Query
        query = f"SELECT * FROM {table_name}"
        params = []
        if pk_id:
            # Find PK column name dynamically
            pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
            if pk_row:
                query += f" WHERE {pk_row['column_name']} = $1"
                params.append(int(pk_id))

        rows = await conn.fetch(query, *params)
        if not rows: return response.text("No data found")

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write Header
        writer.writerow(rows[0].keys())
        # Write Data
        for row in rows:
            writer.writerow(row.values())
            
        return response.text(
            output.getvalue(),
            headers={"Content-Disposition": f'attachment; filename="{table_name}_export.csv"', "Content-Type": "text/csv"}
        )

# --- EDIT & CREATE ROUTES ---

@app.get("/edit/<table_name>/<pk_val>")
@login_required
async def show_edit_form(request, table_name, pk_val):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        
        # Get PK Column
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name']
        
        # Fetch Existing Data
        record = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", int(pk_val))
        
        # Get Columns for Form
        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        columns = []
        for r in col_rows:
            c_name = r['column_name']
            if c_name == pk_column or c_name.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            
            # Helper to format date objects for HTML input
            val = record[c_name]
            if isinstance(val, (date, datetime)): val = val.strftime('%Y-%m-%d')
            
            columns.append({
                "column_name": c_name, "label": make_human_readable(c_name),
                "required": (r['is_nullable'] == 'NO'), "value": val, "data_type": r['data_type']
            })

        return await render("form_view.html", context={
            "table_name": table_name, "table_title": f"Edit {make_human_readable(table_name)}",
            "columns": columns, "all_tables": allowed, "pk_val": pk_val, "mode": "edit"
        })

@app.get("/new/<table_name>")
@login_required
async def show_add_form(request, table_name):
    # (Same add logic, just passing mode='create')
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        col_rows = await conn.fetch("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table_name)
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name'] if pk_row else None

        columns = []
        for r in col_rows:
            c_name = r['column_name']
            if c_name == pk_column or c_name.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            columns.append({
                "column_name": c_name, "label": make_human_readable(c_name),
                "required": (r['is_nullable'] == 'NO'), "value": "", "data_type": r['data_type']
            })
            
        return await render("form_view.html", context={
            "table_name": table_name, "table_title": f"New {make_human_readable(table_name)}",
            "columns": columns, "all_tables": allowed, "mode": "create"
        })

# --- REPLACE THE save_data FUNCTION WITH THIS VERSION ---

@app.post("/api/<table_name>", name="create_row")        # <--- Added name="create_row"
@app.put("/api/<table_name>/<pk_val>", name="update_row") # <--- Added name="update_row"
@login_required
async def save_data(request, table_name, pk_val=None):
    data = request.json
    async with app.ctx.pool.acquire() as conn:
        # Get PK
        pk_row = await conn.fetchrow("SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'", table_name)
        pk_column = pk_row['column_name']
        
        schema_map = {r['column_name']: r for r in await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)}
        clean_data = {}
        
        for k, v in data.items():
            if v == "" or v is None or k == pk_column or k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            target_type = schema_map.get(k, {}).get('data_type')
            if target_type == 'date' and isinstance(v, str):
                try: clean_data[k] = datetime.strptime(v, '%Y-%m-%d').date()
                except: pass
            elif target_type in ('integer', 'bigint', 'numeric') and isinstance(v, str):
                 if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v

        if request.method == "POST":
            # INSERT
            if pk_column:
                max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                clean_data[pk_column] = (int(max_val) + 1) if max_val else 1
            cols, vals = list(clean_data.keys()), list(clean_data.values())
            await conn.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join([f'${i+1}' for i in range(len(vals))])})", *vals)
        else:
            # UPDATE
            set_clauses = [f"{k} = ${i+2}" for i, k in enumerate(clean_data.keys())]
            vals = list(clean_data.values())
            await conn.execute(f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = $1", int(pk_val), *vals)

        return response.json({"status": "success"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, single_process=True)
