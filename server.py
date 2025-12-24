from sanic import Sanic, response
from sanic_ext import Extend, render
import asyncpg
import os
from datetime import datetime, date

app = Sanic("ERP_System")

# Enable Jinja2 templating
app.config.TEMPLATING_PATH_TO_TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")
Extend(app)

# --- DATABASE CONFIGURATION (CLOUD READY) ---
# This variable comes from Render's Environment Settings
CLOUD_DB_URL = os.environ.get("DB_URL")

@app.before_server_start
async def setup_db(app, loop):
    try:
        if CLOUD_DB_URL:
            # CASE 1: We are on the Cloud (Render) -> Use the secure URL
            app.ctx.pool = await asyncpg.create_pool(dsn=CLOUD_DB_URL)
            print("✅ Connected to Cloud Database (Neon)")
        else:
            # CASE 2: We are on the Laptop (Localhost) -> Use local settings
            app.ctx.pool = await asyncpg.create_pool(
                user='postgres',
                password='Jay25092005',   # Your local password
                database='tablesproj',   # Your local database
                host='localhost',
                port='5432'
            )
            print("✅ Connected to Local Database")
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'pool'):
        await app.ctx.pool.close()

# --- Helpers ---

async def get_allowed_tables(conn):
    rows = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'phc_%'
    """)
    return [r['table_name'] for r in rows]

def make_human_readable(text):
    text = text.replace("phc_", "").replace("_t", "")
    if len(text) > 4 and text[3] == '_':
        text = text[4:]
    return text.replace("_", " ").title()

async def get_dropdown_options(conn, fk_column_name):
    try:
        base = fk_column_name[:-3]
        if len(base) > 4 and base[3] == '_':
            entity = base[4:]
        else:
            entity = base
        
        manual_map = {
            "dept": "phc_dept_t",
            "org": "phc_orgs_t",
            "services": "phc_services_t",
            "cost_center": "phc_cost_center_t",
            "app": "phc_apps_t",
            "apps": "phc_apps_t"
        }
        
        target_table = None
        if entity in manual_map:
            t = manual_map[entity]
            exists = await conn.fetchval("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)", t)
            if exists: target_table = t

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
        if not pk_col: return None

        label_col = None
        for candidate in ['name', 'title', 'code', 'desc', 'description', 'detail']:
            label_col = next((c for c in col_names if candidate in c), None)
            if label_col: break
        
        if not label_col: label_col = pk_col 

        rows = await conn.fetch(f"SELECT {pk_col}, {label_col} FROM {target_table} ORDER BY {label_col} LIMIT 100")
        return [{"id": r[pk_col], "name": str(r[label_col])} for r in rows]

    except Exception as e:
        print(f"Lookup Error: {e}")
        return None

# --- Routes ---

@app.route("/")
async def index(request):
    async with app.ctx.pool.acquire() as conn:
        tables = await get_allowed_tables(conn)
    if not tables:
        return response.text("Connected to DB, but no 'phc_' tables found.")
    return response.redirect(f"/table/{tables[0]}") 

@app.get("/table/<table_name>")
async def show_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.text("Invalid Table", status=400)

        # 1. Get Columns
        col_rows = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
        """, table_name)
        
        columns = []
        for r in col_rows:
            columns.append({"raw": r['column_name'], "label": make_human_readable(r['column_name'])})

        # 2. Get Primary Key
        pk_row = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc 
              ON kcu.constraint_name = tc.constraint_name
            WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
        """, table_name)
        pk_column = pk_row['column_name'] if pk_row else None

        # 3. Get Data
        data_rows = await conn.fetch(f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT 100")
        
        return await render(
            "table_view.html", 
            context={
                "table_name": table_name,
                "table_title": make_human_readable(table_name),
                "columns": columns,
                "rows": data_rows, 
                "all_tables": allowed,
                "pk_column": pk_column 
            }
        )

@app.get("/new/<table_name>")
async def show_add_form(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.text("Invalid Table", status=400)

        pk_row = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc 
              ON kcu.constraint_name = tc.constraint_name
            WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
        """, table_name)
        pk_column = pk_row['column_name'] if pk_row else None

        col_rows = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
        """, table_name)
        
        columns = []
        for r in col_rows:
            c_name = r['column_name']
            if c_name == pk_column: continue
            if c_name.endswith('_created') or c_name.endswith('_modified'): continue
            if c_name.endswith('_created_by') or c_name.endswith('_modified_by'): continue
            
            col_data = {
                "column_name": c_name,
                "data_type": r['data_type'],
                "label": make_human_readable(c_name),
                "required": (r['is_nullable'] == 'NO'),
                "options": None
            }
            if c_name.endswith('_id'):
                options = await get_dropdown_options(conn, c_name)
                if options: col_data["options"] = options
            
            columns.append(col_data)
        
        return await render("form_view.html", context={
            "table_name": table_name, 
            "table_title": make_human_readable(table_name),
            "columns": columns, 
            "all_tables": allowed
        })

@app.post("/api/<table_name>")
async def create_row(request, table_name):
    data = request.json
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.json({"error": "Invalid table"}, status=400)

        pk_row = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc 
              ON kcu.constraint_name = tc.constraint_name
            WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
        """, table_name)
        pk_column = pk_row['column_name'] if pk_row else None

        schema_rows = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {r['column_name']: r for r in schema_rows}
        clean_data = {}

        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column: continue 
            if k.endswith('_created') or k.endswith('_modified'): continue

            target_type = schema_map.get(k, {}).get('data_type')
            if target_type == 'date' and isinstance(v, str):
                try: clean_data[k] = datetime.strptime(v, '%Y-%m-%d').date()
                except: return response.json({"error": f"Invalid date: {v}"}, status=400)
            elif target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                v = v.strip()
                if v.isdigit() or (v.startswith('-') and v[1:].isdigit()): clean_data[k] = int(v)
                else: clean_data[k] = v 
            else: clean_data[k] = v

        if pk_column:
            max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
            next_id = 1 if max_val is None else int(max_val) + 1
            clean_data[pk_column] = next_id

        for col_name in schema_map:
            if col_name.endswith('_created_by') or col_name.endswith('_modified_by'):
                clean_data[col_name] = 'System'

        if not clean_data: return response.json({"error": "No valid data provided"}, status=400)

        cols = clean_data.keys()
        vals = list(clean_data.values())
        col_str = ", ".join(cols)
        placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
        
        try:
            await conn.execute(f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})", *vals)
            return response.json({"status": "success"})
        except Exception as e:
            return response.json({"error": str(e)}, status=500)

@app.delete("/api/<table_name>/<pk_val>")
async def delete_row(request, table_name, pk_val):
    async with app.ctx.pool.acquire() as conn:
        allowed = await get_allowed_tables(conn)
        if table_name not in allowed: return response.json({"error": "Invalid table"}, status=400)

        pk_row = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc 
              ON kcu.constraint_name = tc.constraint_name
            WHERE kcu.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
        """, table_name)
        
        if not pk_row:
            return response.json({"error": "Table has no Primary Key"}, status=400)
            
        pk_column = pk_row['column_name']

        col_type = await conn.fetchval("""
            SELECT data_type FROM information_schema.columns 
            WHERE table_name = $1 AND column_name = $2
        """, table_name, pk_column)
        
        try:
            if col_type in ('integer', 'bigint', 'smallint', 'numeric'):
                pk_val = int(pk_val)
        except ValueError:
            return response.json({"error": "Invalid ID format"}, status=400)

        try:
            result = await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", pk_val)
            return response.json({"status": "deleted", "meta": result})
        except Exception as e:
            return response.json({"error": str(e)}, status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, single_process=True)
