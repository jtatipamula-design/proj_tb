from sanic import Sanic, response
from sanic.exceptions import NotFound
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import asyncpg
import os
import json
import bcrypt
from datetime import datetime
import jwt
import functools
import uuid

load_dotenv()
app = Sanic("ERP_System")

# ==========================================
# TEMPLATING & MIDDLEWARE
# ==========================================
env = Environment(loader=FileSystemLoader('templates'))

async def render_template(template_name, **kwargs):
    template = env.get_template(template_name)
    html_content = template.render(**kwargs)
    return response.html(html_content)

@app.middleware("response")
async def add_security_headers(request, res):
    res.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate, private"
    res.headers["Pragma"] = "no-cache"
    res.headers["Expires"] = "0"
    res.headers["X-Content-Type-Options"] = "nosniff"
    res.headers["X-Frame-Options"] = "DENY"
    res.headers["X-XSS-Protection"] = "1; mode=block"

# ==========================================
# DATABASE LIFECYCLE
# ==========================================
@app.before_server_start
async def setup_db(app, loop):
    app.ctx.db = await asyncpg.create_pool(
        dsn=os.getenv("DATABASE_URL"),
        min_size=2,
        max_size=20
    )

@app.after_server_stop
async def close_db(app, loop):
    await app.ctx.db.close()

# ==========================================
# AUTHENTICATION
# ==========================================
def check_auth(wrapped):
    @functools.wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        if not request.ctx.session.get("user_id"):
            return response.redirect("/login")
        return await wrapped(request, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET', 'POST'])
async def handle_login(request):
    if request.method == 'GET':
        return await render_template("login.html")
        
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.db.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM phc_users_t WHERE pus_user_name = $1", username)
        
        if user:
            stored_pwd = user['pus_pwd']
            is_valid = False
            
            try:
                if bcrypt.checkpw(password.encode('utf-8'), stored_pwd.encode('utf-8')):
                    is_valid = True
            except ValueError:
                if password == stored_pwd:
                    is_valid = True
                    
            if is_valid:
                request.ctx.session['user_id'] = user['pus_user_id']
                request.ctx.session['username'] = user['pus_user_name']
                request.ctx.session['role'] = user.get('pus_user_type', 'STD')
                request.ctx.session['company_id'] = user.get('pus_company_id', 1001)
                return response.json({"status": "success", "message": "Login successful"})
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def handle_logout(request):
    request.ctx.session.clear()
    return response.redirect("/login")

# ==========================================
# CACHING & HELPERS
# ==========================================
SCHEMA_CACHE = {"pks": {}, "dropdowns": {}, "tables": []}

def get_table_modules():
    return {
        'phc_companies_t': 'MasterData',
        'phc_cost_center_t': 'MasterData',
        'phc_dept_t': 'MasterData',
        'phc_locations_t': 'MasterData',
        'phc_orgs_t': 'MasterData',
        'phc_services_t': 'MasterData',
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
        'phc_prod_alt_names': 'MasterData',
        'phc_lookup_types': 'MasterData',
        'phc_lookup_values_t': 'MasterData',
        
        'cv_product_registration_t': 'Cleaning',
        'cv_product_equipment_map_t': 'Cleaning',
        'cv_product_apis_t': 'Cleaning',

        'phc_emp_t': 'Employee',
        
        'phc_users_t': 'AppSetup',
        'phc_apps_t': 'AppSetup',
        'phc_screens_t': 'AppSetup',
        'phc_roles_t': 'AppSetup',
        'phc_role_screen_assignment_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup',
        'phc_menu_folders_t': 'AppSetup',
        'phc_approval_types_t': 'AppSetup',
        'phc_approval_setup_t': 'AppSetup',
        'phc_notifications_setup_t': 'AppSetup',
        'phc_approval_events_t': 'AppSetup'
    }

async def get_allowed_tables(request, conn):
    role = request.ctx.session.get("role", "STD")
    user_id = request.ctx.session.get("user_id")

    if role == 'ADM':
        rows = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        return [r['tablename'] for r in rows]

    query = """
        SELECT s.psn_screen_code
        FROM phc_screens_t s
        JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
        JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
        WHERE ura.pua_user_id = $1 AND s.psn_status = 'ACT' AND rsa.prs_status = 'ACT'
    """
    rows = await conn.fetch(query, user_id)
    return [r['psn_screen_code'] for r in rows]

async def get_pk_column(conn, table_name):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu 
          ON kcu.constraint_name = tco.constraint_name
        WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        LIMIT 1;
    """
    pk = await conn.fetchval(query, table_name)
    SCHEMA_CACHE["pks"][table_name] = pk
    return pk

async def get_dropdown_options(conn, table_name, column_name):
    # Map foreign keys to their actual names for the native dropdowns
    fallback_map = {
        'company_id': ('phc_companies_t', 'pcp_company_id', 'pcp_company_name'),
        'pma_product_id': ('cv_product_registration_t', 'product_id', 'product_name'),
        'product_id': ('cv_product_registration_t', 'product_id', 'product_name'),
        'equipment_id': ('cv_equipment_registration_t', 'equipment_id', 'equipment_name'),
        'dept_id': ('phc_dept_t', 'pdp_dept_id', 'pdp_dept_name'),
        'org_id': ('phc_orgs_t', 'pos_org_id', 'pos_org_name'),
        'cost_center_id': ('phc_cost_center_t', 'pcc_cost_center_id', 'pcc_cost_center_name'),
        'services_id': ('phc_services_t', 'pse_services_id', 'pse_services_name'),
        'menu_id': ('phc_menu_folders_t', 'menu_id', 'menu_name'),
        'lookup_type': ('phc_lookup_types', 'plt_lookup_type_code', 'plt_lookup_type'),
        'approval_type': ('phc_approval_types_t', 'pat_approval_type', 'pat_approval_type_desc'),
        'created_by': ('phc_users_t', 'pus_user_id', 'pus_user_name'),
        'modified_by': ('phc_users_t', 'pus_user_id', 'pus_user_name'),
        'last_updated_by': ('phc_users_t', 'pus_user_id', 'pus_user_name')
    }
    
    # 1. Native Dropdown via Fallback Map
    for key in fallback_map:
        if key in column_name.lower():
            target_table, target_pk, target_name = fallback_map[key]
            try:
                # Try to fetch using status=ACT, fall back if no status column exists
                rows = await conn.fetch(f"""
                    SELECT {target_pk} as id, {target_name} as name 
                    FROM {target_table} 
                    WHERE status = 'ACT' OR status IS NULL OR status = 'Active' OR pat_status = 'ACT' OR plt_status = 'ACT'
                """)
                return [{"id": r["id"], "name": f"{r['name']} ({r['id']})"} for r in rows]
            except Exception as e:
                pass
                
    # 2. ERP Lookup Engine (phc_lookup_values_t)
    try:
        # Match standard LOVs
        if column_name.lower() in ('dosage_form', 'process_stage', 'solubility_water', 'cleanability_score', 'pel_zone_classification'):
            lookup_code = column_name.upper()
            rows = await conn.fetch("SELECT plv_lookup_value_code, plv_lookup_value_name FROM phc_lookup_values_t WHERE plv_lookup_type_code = $1 AND plv_status = 'ACT'", lookup_code)
            if rows:
                return [{"id": r["plv_lookup_value_code"], "name": r["plv_lookup_value_name"]} for r in rows]
                
        # Match the new Approval Lookups dynamically
        elif column_name.lower().endswith('_role_type'):
            rows = await conn.fetch("SELECT plv_lookup_value_code, plv_lookup_value_name FROM phc_lookup_values_t WHERE plv_lookup_type_code = 'APPR_ROLE_TYPE' AND plv_status = 'ACT'")
            if rows: return [{"id": r["plv_lookup_value_code"], "name": r["plv_lookup_value_name"]} for r in rows]
            
        elif column_name.lower().endswith('_freq_type'):
            rows = await conn.fetch("SELECT plv_lookup_value_code, plv_lookup_value_name FROM phc_lookup_values_t WHERE plv_lookup_type_code = 'EVENT_FREQ_TYPE' AND plv_status = 'ACT'")
            if rows: return [{"id": r["plv_lookup_value_code"], "name": r["plv_lookup_value_name"]} for r in rows]
            
    except Exception:
        pass

    return None

# ==========================================
# ROUTES
# ==========================================
@app.route('/')
@check_auth
async def dashboard(request):
    async with app.ctx.db.acquire() as conn:
        # Fetch tables FIRST to load the Cache securely
        allowed_tables = await get_allowed_tables(request, conn)
        SCHEMA_CACHE["tables"] = allowed_tables
        
        emp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t WHERE pem_status='ACT'") if 'phc_emp_t' in allowed_tables else 0
        comp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status='ACT'") if 'phc_companies_t' in allowed_tables else 0
        dept_count = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t WHERE pdp_status='ACT'") if 'phc_dept_t' in allowed_tables else 0
        app_count = await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t WHERE pap_status='ACT'") if 'phc_apps_t' in allowed_tables else 0

        # Fetch active menu folders
        folders = await conn.fetch("SELECT * FROM phc_menu_folders_t WHERE status = 'ACT' ORDER BY display_order")
        
        menus = []
        for f in folders:
            screens = await conn.fetch("SELECT psn_screen_code, psn_screen_name FROM phc_screens_t WHERE menu_id = $1 AND psn_status = 'ACT'", f['menu_id'])
            if screens:
                menus.append({
                    "id": f['menu_id'],
                    "name": f['menu_name'],
                    "icon": f['icon_name'],
                    "screens": [{"code": s['psn_screen_code'], "name": s['psn_screen_name']} for s in screens if s['psn_screen_code'] in allowed_tables]
                })

        return await render_template(
            "dashboard.html",
            username=request.ctx.session.get('username'),
            user_id=request.ctx.session.get('user_id'),
            stats={"emp_count": emp_count, "comp_count": comp_count, "dept_count": dept_count, "app_count": app_count},
            all_tables=allowed_tables,
            table_modules=get_table_modules(),
            menus=menus
        )

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    if table_name not in SCHEMA_CACHE.get("tables", []):
        return response.html("Table not found", status=404)
        
    async with app.ctx.db.acquire() as conn:
        allowed_tables = await get_allowed_tables(request, conn)
        if table_name not in allowed_tables:
            return response.html("Access Denied", status=403)

        is_htmx = request.headers.get("HX-Request") == "true"
        q = request.args.get("q", "")
        type_filter = request.args.get("type_filter", "")
        page = int(request.args.get("page", 1))
        per_page = 50
        offset = (page - 1) * per_page
        
        schema_rows = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = $1
            ORDER BY ordinal_position
        """, table_name)
        
        columns = []
        for r in schema_rows:
            col_name = r['column_name']
            
            # Hide WHO audit columns and Company ID from the table view
            if col_name.lower() in ['created_by', 'last_updated_by', 'creation_date', 'last_update_date', 'status', 'company_id']:
                continue
            if col_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by', '_company_id')):
                continue
                
            columns.append({
                "raw": col_name,
                "label": col_name.replace("_", " ").title().replace(" Id", "").replace(" Pcp", "").replace(" Pus", ""),
                "data_type": r['data_type']
            })

        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            pk_column = columns[0]['raw']

        table_title = table_name.replace("_t", "").replace("phc_", "").replace("cv_", "").replace("_", " ").title()

        where_clauses = []
        params = []
        
        # Security: Tenant Isolation
        has_company_id = any(c['column_name'].lower() in ('company_id', 'pcp_company_id', 'pat_company_id') for c in schema_rows)
        if has_company_id and request.ctx.session.get('role') != 'ADM':
            company_col_name = next(c['column_name'] for c in schema_rows if 'company_id' in c['column_name'].lower())
            where_clauses.append(f"{company_col_name} = ${len(params) + 1}")
            params.append(request.ctx.session.get('company_id', 1001))

        if q:
            search_conds = []
            for col in columns:
                if col['data_type'] in ('character varying', 'text', 'varchar'):
                    search_conds.append(f"{col['raw']} ILIKE ${len(params) + 1}")
                    params.append(f"%{q}%")
            if search_conds:
                where_clauses.append("(" + " OR ".join(search_conds) + ")")

        # FILTER: Lookup Values Type Filter (Dropdown next to search)
        if table_name == 'phc_lookup_values_t' and type_filter:
            where_clauses.append(f"plv_lookup_type_code = ${len(params) + 1}")
            params.append(type_filter)

        where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        total_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name} {where_str}", *params)
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        
        rows = await conn.fetch(f"SELECT * FROM {table_name} {where_str} ORDER BY {pk_column} DESC LIMIT {per_page} OFFSET {offset}", *params)

        # FETCH: Categories for Lookup Values Dropdown Filter
        lookup_categories = []
        if table_name == 'phc_lookup_values_t':
            lookup_categories = [dict(r) for r in await conn.fetch("SELECT plt_lookup_type_code as code, plt_lookup_type as name FROM phc_lookup_types ORDER BY plt_lookup_type")]

        return await render_template(
            "table_view.html",
            request=request,
            table_name=table_name,
            table_title=table_title,
            columns=columns,
            rows=[dict(r) for r in rows],
            pk_column=pk_column,
            search_query=q,
            type_filter=type_filter,
            lookup_categories=lookup_categories,
            page=page,
            total_pages=total_pages,
            total_count=total_count,
            start_row=offset + 1 if total_count > 0 else 0,
            end_row=min(offset + per_page, total_count),
            all_tables=SCHEMA_CACHE.get("tables", []),
            table_modules=get_table_modules(),
            username=request.ctx.session.get('username')
        )

@app.get('/new/<table_name>', name="new_form")
@app.get('/edit/<table_name>/<pk_val>', name="edit_form")
@check_auth
async def show_form(request, table_name, pk_val=None):
    async with app.ctx.db.acquire() as conn:
        allowed_tables = await get_allowed_tables(request, conn)
        if table_name not in allowed_tables:
            return response.html("Access Denied", status=403)

        schema_rows = await conn.fetch("""
            SELECT column_name, data_type, is_nullable, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = $1
            ORDER BY ordinal_position
        """, table_name)
        
        if not schema_rows:
            return response.html("Table not found", status=404)

        pk_column = await get_pk_column(conn, table_name)
        row_data = {}
        if pk_val:
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", int(pk_val) if pk_val.isdigit() else pk_val)
            if row: row_data = dict(row)

        columns = []
        for r in schema_rows:
            col_name = r['column_name']
            
            # Hide WHO audit columns and Company ID so they don't clutter the form UI
            if col_name.lower() in ('created_by', 'last_updated_by', 'creation_date', 'last_update_date', 'company_id'):
                continue
            if col_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by', '_company_id')):
                continue

            val = row_data.get(col_name)
            options = await get_dropdown_options(conn, table_name, col_name)
            
            json_options = None
            if r['data_type'] in ('json', 'jsonb'):
                if col_name == 'pr_allowed_tables':
                    json_options = [{"id": t, "name": t} for t in allowed_tables]
                elif col_name == 'pu_assigned_roles':
                    roles = await conn.fetch("SELECT prl_role_id, prl_role_name FROM phc_roles_t WHERE prl_status = 'ACT'")
                    json_options = [{"id": str(r['prl_role_id']), "name": r['prl_role_name']} for r in roles]

            columns.append({
                "column_name": col_name,
                "label": col_name.replace("_", " ").title(),
                "data_type": r['data_type'],
                "required": r['is_nullable'] == 'NO',
                "max_length": r['character_maximum_length'],
                "is_pk": col_name == pk_column,
                "value": val,
                "options": options,
                "json_options": json_options
            })

        table_title = table_name.replace("_t", "").replace("phc_", "").title()

        return await render_template(
            "form_view.html",
            table_name=table_name,
            table_title=table_title,
            columns=columns,
            is_update=bool(pk_val),
            pk_val=pk_val,
            pk_column=pk_column,
            all_tables=allowed_tables,
            table_modules=get_table_modules(),
            username=request.ctx.session.get('username')
        )

# ==========================================
# API ENDPOINTS (SAVE & DELETE)
# ==========================================

@app.post('/api/menu/assign')
@check_auth
async def assign_menu(request):
    data = request.json
    screen_code = data.get('screen_code')
    folder_id = data.get('folder_id')
    
    if not screen_code:
        return response.json({"error": "Missing screen code"}, status=400)
        
    async with app.ctx.db.acquire() as conn:
        if folder_id:
        await conn.execute("UPDATE phc_screens_t SET menu_id = $1 WHERE psn_screen_code = $2", int(folder_id), screen_code)
    else:
        await conn.execute("UPDATE phc_screens_t SET menu_id = NULL WHERE psn_screen_code = $1", screen_code)
        
    return response.json({"status": "success"})


@app.post('/api/<table_name>', name="post_save_data")
@app.put('/api/<table_name>/<pk_val>', name="put_save_data")
@check_auth
async def save_data(request, table_name, pk_val=None):
    data = request.json
    
    async with app.ctx.db.acquire() as conn:
        allowed_tables = await get_allowed_tables(request, conn)
        if table_name not in allowed_tables:
            return response.json({"error": "Access Denied"}, status=403)
            
        pk_column = await get_pk_column(conn, table_name)
        schema_rows = await conn.fetch("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {r['column_name']: r for r in schema_rows}
        
        current_username = request.ctx.session.get('username', 'System')
        current_user_id = request.ctx.session.get('user_id', 1)

        clean_data = {}
        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column and not pk_val: continue 
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

        # Inject Creation/Update Audit Trails
        for col_name, col_info in schema_map.items():
            target_type = col_info['data_type'].lower()
            
            if col_name == 'company_id' or col_name.endswith('company_id'):
                clean_data[col_name] = request.ctx.session.get('company_id', 1001)

            if not pk_val and (col_name.lower().endswith('_created') or col_name.lower() in ('creation_date', 'created')):
                clean_data[col_name] = datetime.now()
            if col_name.lower().endswith('_modified') or col_name.lower() in ('last_update_date', 'modified'):
                clean_data[col_name] = datetime.now()

            if not pk_val and (col_name.lower().endswith('_created_by') or col_name.lower() in ('created_by', 'createdby')):
                clean_data[col_name] = current_user_id if target_type in ('integer', 'bigint', 'numeric') else current_username
            if col_name.lower().endswith('_modified_by') or col_name.lower() in ('last_updated_by', 'modified_by'):
                clean_data[col_name] = current_user_id if target_type in ('integer', 'bigint', 'numeric') else current_username

        if pk_val:
            set_clauses = []
            params = []
            for k, v in clean_data.items():
                if k == pk_column: continue
                set_clauses.append(f"{k} = ${len(params) + 1}")
                params.append(v)
            
            params.append(int(pk_val) if pk_val.isdigit() else pk_val)
            query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = ${len(params)}"
            await conn.execute(query, *params)
            msg = "Record updated successfully"
            
        else:
            # New Record - Check if PK is String vs Int
            pk_col_info = schema_map.get(pk_column, {})
            is_string_pk = pk_col_info.get('data_type') in ('character varying', 'text', 'varchar')
            
            if is_string_pk:
                target_id = data.get(pk_column)
                if not target_id:
                    # Auto-Generate unique String ID (e.g. LOO-A39B2)
                    prefix = "".join(c for c in table_name.split('_')[1] if c.isalpha())[:3].upper()
                    if not prefix: prefix = "ID"
                    random_hex = uuid.uuid4().hex[:5].upper()
                    target_id = f"{prefix}-{random_hex}"
            else:
                max_val = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                target_id = (int(max_val) + 1) if max_val else 1

            clean_data[pk_column] = target_id
            
            columns = list(clean_data.keys())
            values = list(clean_data.values())
            placeholders = [f"${i+1}" for i in range(len(values))]
            
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            await conn.execute(query, *values)
            msg = "Record created successfully"

        return response.html(f"""
            <script>
                sessionStorage.setItem('pendingToast', JSON.stringify({{msg: "{msg}", type: "success"}}));
                window.location.href = "/table/{table_name}";
            </script>
        """)


@app.delete('/api/<table_name>/<pk_val>')
@check_auth
async def delete_data(request, table_name, pk_val):
    async with app.ctx.db.acquire() as conn:
        allowed_tables = await get_allowed_tables(request, conn)
        if table_name not in allowed_tables:
            return response.json({"error": "Access Denied"}, status=403)
            
        pk_column = await get_pk_column(conn, table_name)
        schema_rows = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1", table_name)
        status_col = next((r['column_name'] for r in schema_rows if 'status' in r['column_name'].lower()), None)
        
        parsed_pk = int(pk_val) if pk_val.isdigit() else pk_val
        
        if status_col:
            await conn.execute(f"UPDATE {table_name} SET {status_col} = 'INA' WHERE {pk_column} = $1", parsed_pk)
            msg = "Record deactivated successfully"
        else:
            await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", parsed_pk)
            msg = "Record deleted permanently"
            
        return response.html(f"""
            <script>
                window.showToast("{msg}");
                const row = document.querySelector('tr[hx-target]');
                if(row) row.remove();
            </script>
        """)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    is_development = os.environ.get("ENVIRONMENT") != "production"
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)
