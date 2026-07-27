import os
import uuid
import time
import json
from datetime import datetime, date
from functools import wraps
import bcrypt
import jwt
from sanic import Sanic, response
from sanic.exceptions import NotFound
import asyncpg
from jinja2 import Environment, FileSystemLoader

app = Sanic("ERP_System")

# Environment Variables
DATABASE_URL = os.environ.get("DATABASE_URL")
JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key-change-in-prod")
PORT = int(os.environ.get("PORT", 10000))

# Jinja2 Templating
env = Environment(loader=FileSystemLoader('templates'))

# --- ENTERPRISE PERFORMANCE CACHE ---
USER_AUTH_CACHE = {}
CACHE_TTL = 30  

SCHEMA_CACHE = {"pks": {}}

# ==========================================
# MODULE ROUTING LOGIC (11 Modules)
# ==========================================
def get_table_modules():
    return {
        # ERP Admin
        'phc_companies_t': 'ERPAdmin',
        'phc_operating_orgs_t': 'ERPAdmin',
        'phc_number_range_master_t': 'ERPAdmin',
        'phc_lookup_types': 'ERPAdmin',
        # Master Data
        'phc_dept_t': 'MasterData',
        'phc_services_t': 'MasterData',
        'phc_cost_center_t': 'MasterData',
        'phc_locations_t': 'MasterData',
        'phc_apps_t': 'MasterData',
        'phc_emp_apps_grant_t': 'MasterData',
        'phc_lookup_values_t': 'MasterData',
        'phc_plant_master_t': 'MasterData',
        'phc_plant_compliance_t': 'MasterData',
        'phc_certifications_t': 'MasterData',
        'phc_storage_location_master_t': 'MasterData',
        'phc_partners_t': 'MasterData',
        'phc_uom_master_t': 'MasterData',
        'phc_uom_conversion_t': 'MasterData',
        'phc_prod_master_t': 'MasterData',
        'phc_prod_lifecycle_history_t': 'MasterData',
        'phc_prod_alt_names_t': 'MasterData',
        # HR
        'phc_emp_t': 'HR',
        # People
        'phc_users_t': 'People',
        # User Mgmt
        'phc_screens_t': 'UserMgmt',
        'phc_roles_t': 'UserMgmt',
        'phc_role_screen_assignment_t': 'UserMgmt',
        'phc_user_roles_assignment_t': 'UserMgmt',
        'phc_user_group_t': 'UserMgmt',
        'phc_user_log_t': 'UserMgmt',
        # App Admin
        'phc_error_log_t': 'AppAdmin',
        # Purchasing
        'phc_plant_equipment_t': 'Purchasing',
        'phc_material_master_t': 'Purchasing',
        'phc_material_group_master_t': 'Purchasing',
        'phc_vendors_t': 'Purchasing',
        'phc_vend_sites_t': 'Purchasing',
        'phc_vend_contact_points_t': 'Purchasing',
        'phc_vend_site_locations_t': 'Purchasing',
        # Supply Chain
        'phc_equipment_locations_t': 'SupplyChain',
        'phc_prod_formulation': 'SupplyChain',
        'phc_prod_ingredients': 'SupplyChain',
        'phc_prod_pack_presentation': 'SupplyChain',
        'phc_prod_regulatory_status': 'SupplyChain',
        'phc_prod_regulatory_variations': 'SupplyChain',
        'phc_prod_ectd_documents': 'SupplyChain',
        'phc_product_indications': 'SupplyChain',
        'phc_prod_pharmacology': 'SupplyChain',
        'phc_prod_dosing_regimen': 'SupplyChain',
        'phc_prod_contraindications': 'SupplyChain',
        'phc_prod_warnings': 'SupplyChain',
        'phc_prod_drug_interactions': 'SupplyChain',
        'phc_prod_adverse_events': 'SupplyChain',
        'phc_prod_special_populations': 'SupplyChain',
        'phc_clinical_trials': 'SupplyChain',
        'phc_prod_immunogenicity_data': 'SupplyChain',
        'phc_prod_manufacturing_site': 'SupplyChain',
        'phc_prod_batch_specification': 'SupplyChain',
        'phc_prod_process_parameters': 'SupplyChain',
        'phc_prod_finished_specifications': 'SupplyChain',
        'phc_prod_reference_standards': 'SupplyChain',
        'phc_prod_stability_studies': 'SupplyChain',
        'phc_prod_container_closure': 'SupplyChain',
        'phc_prod_deviations_capa': 'SupplyChain',
        'phc_prod_gmp_certificates': 'SupplyChain',
        'phc_prod_site_inspections': 'SupplyChain',
        'phc_prod_packaging_spec': 'SupplyChain',
        'phc_prod_labeling': 'SupplyChain',
        'phc_prod_serialization': 'SupplyChain',
        'phc_prod_patents': 'SupplyChain',
        'phc_prod_exclusivity': 'SupplyChain',
        'phc_prod_loe': 'SupplyChain',
        'phc_prod_competitor_filings': 'SupplyChain',
        # Workflow Setup
        'phc_approval_types_t': 'WorkflowSetup',
        'phc_approval_setup_t': 'WorkflowSetup',
        'phc_notifications_setup_t': 'WorkflowSetup',
        # Workflow Opps
        'phc_approval_events_t': 'WorkflowOpps',
        # CRM
        'phc_customer_t': 'CRM',
        'phc_cust_site_t': 'CRM',
        'phc_cust_contact_points_t': 'CRM',
        'phc_cust_site_locations_t': 'CRM'
    }

# ==========================================
# DATABASE LIFECYCLE
# ==========================================
@app.before_server_start
async def setup_db(app, loop):
    app.ctx.pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=20)

@app.after_server_stop
async def close_db(app, loop):
    await app.ctx.pool.close()

# ==========================================
# SECURITY & AUTHENTICATION (TITANIUM SHIELD)
# ==========================================
def check_auth(f):
    @wraps(f) # THIS FIXES THE 503 CRASH
    async def decorated_function(request, *args, **kwargs):
        token = request.cookies.get("auth_token")
        if not token:
            return response.redirect("/login")
        
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("user_id")
            session_id = payload.get("session_id")
            
            # --- 30 SECOND RAM CACHE ---
            now = time.time()
            cache_entry = USER_AUTH_CACHE.get(user_id)
            
            if cache_entry and now < cache_entry['expires']:
                if cache_entry['session_id'] != session_id:
                    return response.redirect("/login")
                request.ctx.user_id = user_id
                request.ctx.username = payload.get("username")
                request.ctx.role = cache_entry['role']
                return await f(request, *args, **kwargs)
            
            # --- DATABASE FALLBACK ---
            async with app.ctx.pool.acquire() as conn:
                user = await conn.fetchrow("SELECT pus_session_id, pus_user_type FROM phc_users_t WHERE pus_user_id = $1", user_id)
                if not user or user['pus_session_id'] != session_id:
                    return response.redirect("/login")
                
                # Update RAM Cache
                USER_AUTH_CACHE[user_id] = {
                    "session_id": session_id,
                    "role": user['pus_user_type'],
                    "expires": now + CACHE_TTL
                }
                
                request.ctx.user_id = user_id
                request.ctx.username = payload.get("username")
                request.ctx.role = user['pus_user_type']
                
        except jwt.ExpiredSignatureError:
            return response.redirect("/login")
        except jwt.InvalidTokenError:
            return response.redirect("/login")
            
        return await f(request, *args, **kwargs)
    return decorated_function

async def get_authorized_tables(conn, user_id, role):
    # Titanium Shield: Only fetch actual phc_ screens
    if role == 'ADM':
        rows = await conn.fetch("SELECT psn_screen_code, psn_screen_name FROM phc_screens_t WHERE psn_screen_code LIKE 'phc_%'")
        return {r['psn_screen_code']: r['psn_screen_name'] for r in rows}
    
    query = """
        SELECT DISTINCT s.psn_screen_code, s.psn_screen_name 
        FROM phc_screens_t s
        JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
        JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
        WHERE ura.pua_user_id = $1 AND s.psn_screen_code LIKE 'phc_%'
    """
    rows = await conn.fetch(query, user_id)
    return {r['psn_screen_code']: r['psn_screen_name'] for r in rows}

# ==========================================
# AUTHENTICATION ROUTES
# ==========================================
@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        # Infinite Loop Fix
        token = request.cookies.get("auth_token")
        if token:
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                user_id = payload.get("user_id")
                session_id = payload.get("session_id")
                async with app.ctx.pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if user and user['pus_session_id'] == session_id:
                        return response.redirect("/")
            except:
                pass 
        
        # Render page
        template = env.get_template('login.html')
        res = response.html(template.render())
        res.delete_cookie("auth_token")
        return res

    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.pool.acquire() as conn:
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
                new_session_id = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", new_session_id, user['pus_user_id'])
                
                USER_AUTH_CACHE.pop(user['pus_user_id'], None) # Flush cache
                
                token = jwt.encode({
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "session_id": new_session_id,
                    "exp": datetime.utcnow().timestamp() + 86400
                }, JWT_SECRET, algorithm="HS256")
                
                res = response.json({"status": "success"})
                # Session Cookie
                res.add_cookie("auth_token", token, httponly=True, samesite="Lax")
                return res
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    res = response.redirect("/login")
    res.delete_cookie("auth_token")
    return res

# ==========================================
# DASHBOARD
# ==========================================
@app.route('/')
@check_auth
async def dashboard(request):
    async with app.ctx.pool.acquire() as conn:
        stats = {}
        try:
            stats['emp_count'] = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t WHERE pem_status = 'ACT'")
            stats['comp_count'] = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status = 'ACT'")
            stats['dept_count'] = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t WHERE pdp_status = 'ACT'")
            stats['app_count'] = await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t WHERE pap_status = 'ACT'")
        except:
            pass

        auth_tables = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
        
    template = env.get_template('dashboard.html')
    return response.html(template.render(
        username=request.ctx.username,
        user_id=request.ctx.user_id,
        user_role=request.ctx.role,
        stats=stats,
        all_tables=list(auth_tables.keys()),
        table_modules=get_table_modules()
    ))

# ==========================================
# HELPER FUNCTIONS
# ==========================================
async def get_pk_column(conn, table_name):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu 
          ON kcu.constraint_name = tco.constraint_name 
          AND kcu.constraint_schema = tco.constraint_schema 
        WHERE tco.constraint_type = 'PRIMARY KEY' 
          AND tco.table_name = $1
    """
    pk = await conn.fetchval(query, table_name)
    SCHEMA_CACHE["pks"][table_name] = pk
    return pk

async def get_table_schema(conn, table_name):
    query = """
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns 
        WHERE table_name = $1 
        ORDER BY ordinal_position
    """
    rows = await conn.fetch(query, table_name)
    return {r['column_name']: dict(r) for r in rows}

# ==========================================
# MAGIC DROPDOWNS & VIRTUAL CHECKBOXES
# ==========================================
async def get_dropdown_options(conn, column_name):
    options = []
    try:
        if 'company_id' in column_name:
            rows = await conn.fetch("SELECT pcp_company_id, pcp_company_name FROM phc_companies_t WHERE pcp_status = 'ACT'")
            options = [{'id': r['pcp_company_id'], 'name': r['pcp_company_name']} for r in rows]
        elif 'org_id' in column_name:
            rows = await conn.fetch("SELECT pos_org_id, pos_org_name FROM phc_operating_orgs_t WHERE pos_status = 'ACT'")
            options = [{'id': r['pos_org_id'], 'name': r['pos_org_name']} for r in rows]
        elif 'dept_id' in column_name:
            rows = await conn.fetch("SELECT pdp_dept_id, pdp_dept_name FROM phc_dept_t WHERE pdp_status = 'ACT'")
            options = [{'id': r['pdp_dept_id'], 'name': r['pdp_dept_name']} for r in rows]
        elif 'services_id' in column_name:
            rows = await conn.fetch("SELECT pse_services_id, pse_services_name FROM phc_services_t WHERE pse_status = 'ACT'")
            options = [{'id': r['pse_services_id'], 'name': r['pse_services_name']} for r in rows]
        elif 'cost_center_id' in column_name:
            rows = await conn.fetch("SELECT pcc_cost_center_id, pcc_cost_center_name FROM phc_cost_center_t WHERE pcc_status = 'ACT'")
            options = [{'id': r['pcc_cost_center_id'], 'name': r['pcc_cost_center_name']} for r in rows]
        elif 'location_id' in column_name:
            rows = await conn.fetch("SELECT pln_location_id, pln_location_name FROM phc_locations_t WHERE pln_status = 'ACT'")
            options = [{'id': r['pln_location_id'], 'name': r['pln_location_name']} for r in rows]
        elif column_name == 'pus_user_type':
            options = [{'id': 'ADM', 'name': 'Admin'}, {'id': 'STD', 'name': 'Standard User'}]
    except Exception as e:
        print(f"Dropdown error for {column_name}: {e}")
    return options

async def get_json_options(conn, column_name):
    options = []
    try:
        if column_name == 'pua_role_id':
            rows = await conn.fetch("SELECT prl_role_id, prl_role_name FROM phc_roles_t WHERE prl_status = 'ACT'")
            options = [{'id': str(r['prl_role_id']), 'name': r['prl_role_name']} for r in rows]
        elif column_name == 'prs_screen_id':
            rows = await conn.fetch("SELECT psn_screen_id, psn_screen_name FROM phc_screens_t WHERE psn_status = 'ACT'")
            options = [{'id': str(r['psn_screen_id']), 'name': r['psn_screen_name']} for r in rows]
    except Exception as e:
        print(f"JSON dropdown error for {column_name}: {e}")
    return options

async def resolve_magic_ids(conn, rows, schema):
    if not rows: return rows
    resolved_rows = []
    
    for row in rows:
        row_dict = dict(row)
        for col, val in row_dict.items():
            if val is None: continue
            
            # Skip primary keys
            if col == 'pus_user_id' or col == 'pem_emp_id': 
                continue
                
            if 'company_id' in col:
                name = await conn.fetchval("SELECT pcp_company_name FROM phc_companies_t WHERE pcp_company_id = $1", val)
                if name: row_dict[col] = name
            elif 'org_id' in col:
                name = await conn.fetchval("SELECT pos_org_name FROM phc_operating_orgs_t WHERE pos_org_id = $1", val)
                if name: row_dict[col] = name
            elif 'dept_id' in col:
                name = await conn.fetchval("SELECT pdp_dept_name FROM phc_dept_t WHERE pdp_dept_id = $1", val)
                if name: row_dict[col] = name
            elif 'role_id' in col and isinstance(val, str):
                try:
                    ids = json.loads(val)
                    if isinstance(ids, list) and ids:
                        names = []
                        for i in ids:
                            n = await conn.fetchval("SELECT prl_role_name FROM phc_roles_t WHERE prl_role_id = $1", int(i))
                            if n: names.append(n)
                        row_dict[col] = ", ".join(names)
                except: pass
            elif 'screen_id' in col and isinstance(val, str):
                try:
                    ids = json.loads(val)
                    if isinstance(ids, list) and ids:
                        names = []
                        for i in ids:
                            n = await conn.fetchval("SELECT psn_screen_name FROM phc_screens_t WHERE psn_screen_id = $1", int(i))
                            if n: names.append(n)
                        row_dict[col] = ", ".join(names)
                except: pass
        resolved_rows.append(row_dict)
    return resolved_rows

# ==========================================
# DYNAMIC VIEWS (TABLE & FORM)
# ==========================================
@app.route('/table/<table_name>')
@check_auth
async def view_table(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
        table_lower = table_name.lower()
        if table_lower not in [t.lower() for t in auth_tables.keys()]:
            return response.html("<h1>403 Forbidden</h1>", status=403)
            
        real_table_name = next(t for t in auth_tables.keys() if t.lower() == table_lower)
        pk_col = await get_pk_column(conn, real_table_name)
        schema = await get_table_schema(conn, real_table_name)
        
        page = int(request.args.get('page', 1))
        per_page = 50
        offset = (page - 1) * per_page
        search_query = request.args.get('q', '').strip()
        
        base_query = f"FROM {real_table_name}"
        where_clause = ""
        params = []
        
        if search_query:
            conditions = []
            for i, (col, details) in enumerate(schema.items()):
                if details['data_type'] in ('character varying', 'text', 'varchar'):
                    conditions.append(f"{col} ILIKE ${len(params)+1}")
                    params.append(f"%{search_query}%")
            if conditions:
                where_clause = " WHERE " + " OR ".join(conditions)

        count_query = f"SELECT COUNT(*) {base_query} {where_clause}"
        total_count = await conn.fetchval(count_query, *params)
        
        select_query = f"SELECT * {base_query} {where_clause} ORDER BY 1 DESC LIMIT {per_page} OFFSET {offset}"
        raw_rows = await conn.fetch(select_query, *params)
        
        rows = await resolve_magic_ids(conn, raw_rows, schema)
        
        columns = []
        for col in schema.keys():
            if 'password' in col.lower() or col == 'pus_pwd' or col == 'pus_session_id': continue
            columns.append({
                "raw": col,
                "label": col.replace('pcp_', '').replace('pos_', '').replace('pdp_', '').replace('pse_', '').replace('pcc_', '').replace('pln_', '').replace('pem_', '').replace('pap_', '').replace('pus_', '').replace('psn_', '').replace('prl_', '').replace('prs_', '').replace('pua_', '').replace('_', ' ').title()
            })
            
        is_htmx = request.headers.get("HX-Request") == "true"
        template = env.get_template('table_view.html')
        html_out = template.render(
            table_name=real_table_name,
            table_title=auth_tables.get(real_table_name, real_table_name),
            columns=columns,
            rows=rows,
            pk_column=pk_col,
            page=page,
            total_pages=(total_count // per_page) + (1 if total_count % per_page > 0 else 0),
            total_count=total_count,
            start_row=offset + 1 if total_count > 0 else 0,
            end_row=min(offset + per_page, total_count),
            search_query=search_query,
            username=request.ctx.username,
            user_id=request.ctx.user_id,
            user_role=request.ctx.role,
            all_tables=list(auth_tables.keys()),
            table_modules=get_table_modules()
        )
        return response.html(html_out)

@app.route('/new/<table_name>')
@check_auth
async def new_record(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
        if table_name not in auth_tables: return response.html("403", status=403)
        
        schema = await get_table_schema(conn, table_name)
        pk_col = await get_pk_column(conn, table_name)
        
        columns = []
        audit_cols = []
        
        for col_name, info in schema.items():
            if col_name == 'pus_session_id': continue
            
            is_audit = col_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by', 'creation_date', 'last_update_date', 'created_by', 'last_updated_by'))
            
            col_data = {
                "column_name": col_name,
                "label": col_name.replace('pcp_', '').replace('pos_', '').replace('pdp_', '').replace('pse_', '').replace('pcc_', '').replace('pln_', '').replace('pem_', '').replace('pap_', '').replace('pus_', '').replace('psn_', '').replace('prl_', '').replace('prs_', '').replace('pua_', '').replace('_', ' ').title(),
                "data_type": info['data_type'],
                "required": info['is_nullable'] == 'NO',
                "is_pk": col_name == pk_col,
                "options": await get_dropdown_options(conn, col_name),
                "json_options": await get_json_options(conn, col_name)
            }
            if is_audit: audit_cols.append(col_data)
            else: columns.append(col_data)
            
        columns.extend(audit_cols)

        template = env.get_template('form_view.html')
        return response.html(template.render(
            table_name=table_name,
            table_title=auth_tables.get(table_name, table_name),
            columns=columns,
            is_update=False,
            username=request.ctx.username,
            user_id=request.ctx.user_id,
            user_role=request.ctx.role,
            all_tables=list(auth_tables.keys()),
            table_modules=get_table_modules()
        ))

@app.route('/edit/<table_name>/<pk_val>')
@check_auth
async def edit_record(request, table_name, pk_val):
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
        if table_name not in auth_tables: return response.html("403", status=403)
        
        schema = await get_table_schema(conn, table_name)
        pk_col = await get_pk_column(conn, table_name)
        
        val_to_search = int(pk_val) if pk_val.isdigit() else pk_val
        record = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_col} = $1", val_to_search)
        if not record: return response.html("404 Not Found", status=404)
        
        columns = []
        audit_cols = []
        
        for col_name, info in schema.items():
            if col_name == 'pus_session_id': continue
            
            is_audit = col_name.lower().endswith(('_created', '_modified', '_created_by', '_modified_by', 'creation_date', 'last_update_date', 'created_by', 'last_updated_by'))
            val = record[col_name]
            if isinstance(val, dict) or isinstance(val, list): val = json.dumps(val)
            elif 'date' in info['data_type'] and val: val = str(val)[:10]
            
            col_data = {
                "column_name": col_name,
                "label": col_name.replace('pcp_', '').replace('pos_', '').replace('pdp_', '').replace('pse_', '').replace('pcc_', '').replace('pln_', '').replace('pem_', '').replace('pap_', '').replace('pus_', '').replace('psn_', '').replace('prl_', '').replace('prs_', '').replace('pua_', '').replace('_', ' ').title(),
                "data_type": info['data_type'],
                "required": info['is_nullable'] == 'NO',
                "is_pk": col_name == pk_col,
                "value": val,
                "options": await get_dropdown_options(conn, col_name),
                "json_options": await get_json_options(conn, col_name)
            }
            if is_audit: audit_cols.append(col_data)
            else: columns.append(col_data)
            
        columns.extend(audit_cols)

        template = env.get_template('form_view.html')
        return response.html(template.render(
            table_name=table_name,
            table_title=auth_tables.get(table_name, table_name),
            columns=columns,
            is_update=True,
            pk_val=pk_val,
            username=request.ctx.username,
            user_id=request.ctx.user_id,
            user_role=request.ctx.role,
            all_tables=list(auth_tables.keys()),
            table_modules=get_table_modules()
        ))

# ==========================================
# API CRUD LOGIC
# ==========================================
@app.route('/api/<table_name>', methods=['POST'])
@check_auth
async def save_new(request, table_name):
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
        if table_name not in auth_tables: return response.json({"error": "Forbidden"}, status=403)
        
        schema = await get_table_schema(conn, table_name)
        pk_col = await get_pk_column(conn, table_name)
        data = request.form if request.content_type and "multipart/form-data" in request.content_type else request.json
        
        clean_data = {}
        for k, v in data.items():
            if isinstance(v, list) and len(v) == 1: v = v[0]
            if v == "" or v is None: continue 
            if k == pk_col: continue 
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            
            if k == 'pus_pwd':
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')
                
            col_info = schema.get(k, {})
            target_type = col_info.get('data_type', '').lower()
            max_len = col_info.get('character_maximum_length')
            
            if 'date' in target_type or 'timestamp' in target_type:
                if isinstance(v, str) and v:
                    try: v = datetime.strptime(v, '%Y-%m-%d')
                    except ValueError: pass
            
            if target_type == 'jsonb' and isinstance(v, str):
                try: v = json.loads(v)
                except: pass

            if isinstance(v, str) and max_len is not None:
                if len(v) > max_len:
                    if "status" in k and v.lower() == "active": v = "ACT"
                    elif "status" in k and v.lower() == "inactive": v = "INA"
                    else: v = v[:max_len]
            
            if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                 if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v

        # Inject Audit Data
        creator_col = next((c for c in schema if c.endswith('_created_by') or c == 'created_by'), None)
        created_col = next((c for c in schema if c.endswith('_created') or c == 'creation_date'), None)
        modifier_col = next((c for c in schema if c.endswith('_modified_by') or c == 'last_updated_by'), None)
        modified_col = next((c for c in schema if c.endswith('_modified') or c == 'last_update_date'), None)
        comp_col = next((c for c in schema if 'company_id' in c), None)

        if creator_col: clean_data[creator_col] = str(request.ctx.user_id) if schema[creator_col]['data_type'] in ('character varying', 'text') else request.ctx.user_id
        if created_col: clean_data[created_col] = datetime.utcnow()
        if modifier_col: clean_data[modifier_col] = str(request.ctx.user_id) if schema[modifier_col]['data_type'] in ('character varying', 'text') else request.ctx.user_id
        if modified_col: clean_data[modified_col] = datetime.utcnow()
        if comp_col and comp_col not in clean_data: clean_data[comp_col] = 1001

        if not clean_data: return response.json({"error": "No valid data to save"}, status=400)
        
        # Auto-Gen PK
        pk_type = schema[pk_col]['data_type']
        if pk_type in ('integer', 'bigint', 'numeric'):
            max_id = await conn.fetchval(f"SELECT MAX({pk_col}) FROM {table_name}")
            clean_data[pk_col] = (max_id or 0) + 1
        
        cols = list(clean_data.keys())
        vals = list(clean_data.values())
        placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
        
        query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"
        try:
            await conn.execute(query, *vals)
            return response.json({"status": "success"}, headers={"HX-Redirect": f"/table/{table_name}"})
        except Exception as e:
            return response.json({"error": str(e)}, status=500)

@app.route('/api/<table_name>/<pk_val>', methods=['PUT', 'DELETE'])
@check_auth
async def modify_record(request, table_name, pk_val):
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
        if table_name not in auth_tables: return response.json({"error": "Forbidden"}, status=403)
        
        schema = await get_table_schema(conn, table_name)
        pk_col = await get_pk_column(conn, table_name)
        search_val = int(pk_val) if pk_val.isdigit() else pk_val
        
        if request.method == 'DELETE':
            status_col = next((c for c in schema if 'status' in c), None)
            try:
                if status_col:
                    await conn.execute(f"UPDATE {table_name} SET {status_col} = 'INA' WHERE {pk_col} = $1", search_val)
                else:
                    await conn.execute(f"DELETE FROM {table_name} WHERE {pk_col} = $1", search_val)
                return response.json({"status": "success"})
            except Exception as e:
                return response.json({"error": str(e)}, status=500)

        data = request.form if request.content_type and "multipart/form-data" in request.content_type else request.json
        clean_data = {}
        for k, v in data.items():
            if isinstance(v, list) and len(v) == 1: v = v[0]
            if k == pk_col: continue 
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            
            if k == 'pus_pwd':
                if not v or v.strip() == "": continue
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')
                
            col_info = schema.get(k, {})
            target_type = col_info.get('data_type', '').lower()
            
            if 'date' in target_type or 'timestamp' in target_type:
                if isinstance(v, str) and v:
                    try: v = datetime.strptime(v, '%Y-%m-%d')
                    except ValueError: pass
                    
            if target_type == 'jsonb' and isinstance(v, str):
                try: v = json.loads(v)
                except: pass

            if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                 if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v

        modifier_col = next((c for c in schema if c.endswith('_modified_by') or c == 'last_updated_by'), None)
        modified_col = next((c for c in schema if c.endswith('_modified') or c == 'last_update_date'), None)
        if modifier_col: clean_data[modifier_col] = str(request.ctx.user_id) if schema[modifier_col]['data_type'] in ('character varying', 'text') else request.ctx.user_id
        if modified_col: clean_data[modified_col] = datetime.utcnow()
            
        if not clean_data: return response.json({"status": "success"}, headers={"HX-Redirect": f"/table/{table_name}"})

        cols = list(clean_data.keys())
        vals = list(clean_data.values())
        set_clause = ", ".join([f"{col} = ${i+1}" for i, col in enumerate(cols)])
        vals.append(search_val)
        
        query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = ${len(vals)}"
        try:
            await conn.execute(query, *vals)
            return response.json({"status": "success"}, headers={"HX-Redirect": f"/table/{table_name}"})
        except Exception as e:
            return response.json({"error": str(e)}, status=500)

if __name__ == "__main__":
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not set in environment.")
        exit(1)
    app.run(host="0.0.0.0", port=PORT, single_process=True)
