import os
import json
import uuid
from datetime import datetime, timezone
import asyncpg
import bcrypt
import jwt
from sanic import Sanic, response
from jinja2 import Environment, FileSystemLoader

app = Sanic("ERP_System")
app.static('/static', './static')

# Environment configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_KRIaP@ep-my-db.us-east-2.aws.neon.tech/neondb?sslmode=require")
JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-key-change-in-prod")

# Setup Jinja2 for HTML Templates
template_env = Environment(loader=FileSystemLoader('templates'), autoescape=True)

# Central cache to avoid querying database for schema multiple times
SCHEMA_CACHE = {
    "pks": {},
    "fks": {},
    "dropdowns": {},
    "dropdown_expiry": {}
}

def sort_columns(col_names, pk_column):
    """Strict Logical Order: PK -> Status -> Normal Data -> Dates -> Audit"""
    audit_keywords = ['created', 'modified', 'creation_date', 'last_update_date', 'updated_by', 'created_by']
    
    sorted_cols = [pk_column] if pk_column in col_names else []
    status_cols = [c for c in col_names if 'status' in c.lower() and c != pk_column]
    
    start_date_cols = [c for c in col_names if 'start_date' in c.lower() and c != pk_column and c not in status_cols]
    end_date_cols = [c for c in col_names if 'end_date' in c.lower() and c != pk_column and c not in status_cols and c not in start_date_cols]
    
    audit_cols = [c for c in col_names if any(k in c.lower() for k in audit_keywords) and c != pk_column and c not in status_cols]
    
    pinned_so_far = sorted_cols + status_cols + start_date_cols + end_date_cols + audit_cols
    standard_cols = [c for c in col_names if c not in pinned_so_far]
            
    final_list = sorted_cols + status_cols + standard_cols + start_date_cols + end_date_cols + audit_cols
    return final_list

def get_table_modules(all_tables):
    """Maps the 79 verified tables to their spreadsheet module."""
    mapping = {
        'phc_companies_t': 'ERPAdmin',
        'phc_operating_orgs_t': 'ERPAdmin',
        'phc_dept_t': 'MasterData',
        'phc_services_t': 'MasterData',
        'phc_cost_center_t': 'MasterData',
        'phc_locations_t': 'MasterData',
        'phc_emp_t': 'HR',
        'phc_apps_t': 'MasterData',
        'phc_emp_apps_grant_t': 'MasterData',
        'phc_lookup_types': 'ERPAdmin',
        'phc_lookup_values_t': 'MasterData',
        'phc_users_t': 'People',
        'phc_screens_t': 'UserMgmt',
        'phc_roles_t': 'UserMgmt',
        'phc_role_screen_assignment_t': 'UserMgmt',
        'phc_user_roles_assignment_t': 'UserMgmt',
        'phc_user_group_t': 'UserMgmt',
        'phc_user_log_t': 'UserMgmt',
        'phc_error_log_t': 'AppAdmin',
        'phc_plant_master_t': 'MasterData',
        'phc_plant_compliance_t': 'MasterData',
        'phc_certifications_t': 'MasterData',
        'phc_plant_equipment_t': 'Purchasing',
        'phc_equipment_locations_t': 'SupplyChain',
        'phc_material_master_t': 'Purchasing',
        'phc_material_group_master_t': 'Purchasing',
        'phc_uom_master_t': 'MasterData',
        'phc_uom_conversion_t': 'MasterData',
        'phc_prod_master_t': 'MasterData',
        'phc_prod_lifecycle_history_t': 'MasterData',
        'phc_prod_alt_names_t': 'MasterData',
        'phc_approval_types_t': 'WorkflowSetup',
        'phc_approval_setup_t': 'WorkflowSetup',
        'phc_notifications_setup_t': 'WorkflowSetup',
        'phc_approval_events_t': 'WorkflowOpps',
        'phc_number_range_master_t': 'ERPAdmin',
        'phc_storage_location_master_t': 'MasterData',
        'phc_partners_t': 'MasterData',
        'phc_customer_t': 'CRM',
        'phc_cust_site_t': 'CRM',
        'phc_cust_contact_points_t': 'CRM',
        'phc_cust_site_locations_t': 'CRM',
        'phc_vendors_t': 'Purchasing',
        'phc_vend_sites_t': 'Purchasing',
        'phc_vend_contact_points_t': 'Purchasing',
        'phc_vend_site_locations_t': 'Purchasing',
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
        'phc_prod_competitor_filings': 'SupplyChain'
    }

    final_mapping = {}
    for tbl in all_tables:
        tbl_lower = tbl.lower()
        if tbl_lower in mapping:
            final_mapping[tbl] = mapping[tbl_lower]
        else:
            final_mapping[tbl] = 'Unknown' 
    return final_mapping

@app.listener('before_server_start')
async def setup_db(app, loop):
    app.ctx.pool = await asyncpg.create_pool(dsn=DB_URL, min_size=2, max_size=10)

@app.listener('after_server_stop')
async def close_db(app, loop):
    await app.ctx.pool.close()

async def get_authorized_tables(conn, user_id, user_role):
    # If Admin, fetch all active screens
    if user_role == 'ADM':
        query = "SELECT psn_screen_code FROM phc_screens_t WHERE psn_status = 'ACT'"
        records = await conn.fetch(query)
    # If Standard User, fetch only assigned screens
    else:
        query = """
            SELECT s.psn_screen_code 
            FROM phc_user_roles_assignment_t ura
            JOIN phc_role_screen_assignment_t rsa ON ura.pua_role_id = rsa.prs_role_id
            JOIN phc_screens_t s ON rsa.prs_screen_id = s.psn_screen_id
            WHERE ura.pua_user_id = $1 
              AND ura.pua_status = 'ACT' 
              AND rsa.prs_status = 'ACT' 
              AND s.psn_status = 'ACT'
        """
        records = await conn.fetch(query, user_id)

    raw_tables = [r['psn_screen_code'] for r in records]
    
    # TITANIUM SHIELD: Automatically reject anything that isn't phc_ or playing_with_neon
    clean_tables = [
        t for t in raw_tables 
        if t.lower().startswith('phc_') or t.lower() == 'playing_with_neon'
    ]
    return list(set(clean_tables))

def check_auth(wrapped):
    async def decorator(request, *args, **kwargs):
        token = request.cookies.get("auth_token")
        if not token:
            return response.redirect("/login")
        
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("user_id")
            session_uuid = payload.get("session_uuid")
            
            # One Active Session Per User Enforcement
            async with app.ctx.pool.acquire() as conn:
                db_session = await conn.fetchval("SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
                if db_session and str(db_session) != str(session_uuid):
                    # Kicked out because they logged in elsewhere
                    res = response.redirect("/login")
                    res.delete_cookie("auth_token")
                    return res
            
            request.ctx.session = payload
            return await wrapped(request, *args, **kwargs)
        except jwt.ExpiredSignatureError:
            res = response.redirect("/login")
            res.delete_cookie("auth_token")
            return res
        except jwt.InvalidTokenError:
            res = response.redirect("/login")
            res.delete_cookie("auth_token")
            return res
            
    return decorator

@app.route('/login', methods=['GET', 'POST'])
async def handle_login(request):
    if request.method == 'GET':
        # Infinite Loop Fix: Actually verify the token before blindly redirecting to dashboard
        token = request.cookies.get("auth_token")
        if token:
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                user_id = payload.get("user_id")
                session_uuid = payload.get("session_uuid")
                
                async with app.ctx.pool.acquire() as conn:
                    db_session = await conn.fetchval("SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    
                    if db_session and str(db_session) == str(session_uuid):
                        return response.redirect("/")
            except:
                pass
                
        # If we reach here, render the login card and wipe any stale cookies
        template = template_env.get_template("login.html")
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
                # Generate a single session token to lock out older logins
                new_session_id = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", new_session_id, user['pus_user_id'])
                
                payload = {
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "role": user.get('pus_user_type', 'STD'),
                    "session_uuid": new_session_id,
                    "exp": datetime.now(timezone.utc).timestamp() + 86400
                }
                token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
                
                res = response.json({"status": "success", "message": "Login successful"})
                res.add_cookie(
                    "auth_token", 
                    token, 
                    httponly=True, 
                    samesite="Lax", 
                    max_age=86400
                )
                return res
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def handle_logout(request):
    res = response.redirect("/login")
    res.delete_cookie("auth_token")
    return res

async def get_pk_column(conn, table_name):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    q = """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1::regclass AND i.indisprimary;
    """
    try:
        pk = await conn.fetchval(q, table_name)
        SCHEMA_CACHE["pks"][table_name] = pk
        return pk
    except asyncpg.exceptions.UndefinedTableError:
        return None

async def get_foreign_keys(conn, table_name):
    if table_name in SCHEMA_CACHE["fks"]:
        return SCHEMA_CACHE["fks"][table_name]
    q = """
        SELECT
            kcu.column_name as fk_column,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;
    """
    fks = await conn.fetch(q, table_name)
    result = {fk['fk_column']: fk for fk in fks}
    SCHEMA_CACHE["fks"][table_name] = result
    return result

@app.route('/')
@check_auth
async def dashboard(request):
    user_role = request.ctx.session.get('role')
    user_id = request.ctx.session.get('user_id')
    
    async with app.ctx.pool.acquire() as conn:
        all_tables = await get_authorized_tables(conn, user_id, user_role)
        all_tables.sort()
        table_modules = get_table_modules(all_tables)
        
        # Dashboard stats
        try:
            emp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t")
            comp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t")
            dept_count = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t")
            app_count = await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t")
        except:
            emp_count, comp_count, dept_count, app_count = 0, 0, 0, 0

    template = template_env.get_template("dashboard.html")
    return response.html(template.render(
        all_tables=all_tables,
        table_modules=table_modules,
        username=request.ctx.session.get('username'),
        user_id=user_id,
        user_role=user_role,
        stats={"emp_count": emp_count, "comp_count": comp_count, "dept_count": dept_count, "app_count": app_count}
    ))

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    user_role = request.ctx.session.get('role')
    user_id = request.ctx.session.get('user_id')
    
    page = int(request.args.get("page", 1))
    limit = 50
    offset = (page - 1) * limit
    search_query = request.args.get("q", "").strip()

    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, user_id, user_role)
        if table_name not in auth_tables:
            return response.text("Access Denied.", status=403)
            
        table_modules = get_table_modules(auth_tables)
        pk_column = await get_pk_column(conn, table_name)
        
        # Introspect columns
        col_query = "SELECT column_name FROM information_schema.columns WHERE table_name = $1;"
        col_records = await conn.fetch(col_query, table_name)
        raw_cols = [r['column_name'] for r in col_records]
        
        ordered_cols = sort_columns(raw_cols, pk_column)
        
        # Build live search WHERE clause
        where_clause = ""
        args = []
        if search_query:
            conditions = []
            for i, col in enumerate(ordered_cols):
                conditions.append(f"CAST({col} AS TEXT) ILIKE ${i+1}")
                args.append(f"%{search_query}%")
            where_clause = "WHERE " + " OR ".join(conditions)
        
        data_query = f"SELECT * FROM {table_name} {where_clause} ORDER BY {pk_column} DESC LIMIT {limit} OFFSET {offset}"
        count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
        
        rows = await conn.fetch(data_query, *args)
        total_count = await conn.fetchval(count_query, *args)
        
        # Magic ID Resolver (Skipping the PK column!)
        processed_rows = []
        for r in rows:
            row_dict = dict(r)
            for col in ordered_cols:
                if col.endswith('_id') and col != pk_column and row_dict.get(col) is not None:
                    lookup_val = row_dict[col]
                    if 'company' in col.lower():
                        val = await conn.fetchval("SELECT pcp_company_name FROM phc_companies_t WHERE pcp_company_id = $1", lookup_val)
                    elif 'role' in col.lower():
                        val = await conn.fetchval("SELECT prl_role_name FROM phc_roles_t WHERE prl_role_id = $1", lookup_val)
                    elif 'screen' in col.lower():
                        val = await conn.fetchval("SELECT psn_screen_name FROM phc_screens_t WHERE psn_screen_id = $1", lookup_val)
                    elif 'user' in col.lower():
                        val = await conn.fetchval("SELECT pus_user_name FROM phc_users_t WHERE pus_user_id = $1", lookup_val)
                    else:
                        val = None
                    if val:
                        row_dict[col] = f"{val} (ID: {lookup_val})"
            processed_rows.append(row_dict)

        columns = [{"raw": c, "label": c.replace('_', ' ').title().replace(' Phc', '')} for c in ordered_cols]
        total_pages = max(1, (total_count + limit - 1) // limit)
        
        template = template_env.get_template("table_view.html")
        return response.html(template.render(
            table_name=table_name,
            table_title=table_name.replace('_t', '').replace('_', ' ').title(),
            columns=columns,
            rows=processed_rows,
            pk_column=pk_column,
            all_tables=sorted(auth_tables),
            table_modules=table_modules,
            username=request.ctx.session.get('username'),
            user_id=user_id,
            user_role=user_role,
            page=page,
            total_pages=total_pages,
            total_count=total_count,
            start_row=offset + 1 if total_count > 0 else 0,
            end_row=min(offset + limit, total_count),
            search_query=search_query
        ))

@app.route('/new/<table_name>')
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>')
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

async def render_form(request, table_name, is_update=False, pk_val=None):
    user_role = request.ctx.session.get('role')
    user_id = request.ctx.session.get('user_id')
    
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(conn, user_id, user_role)
        if table_name not in auth_tables:
            return response.text("Access Denied.", status=403)
            
        pk_column = await get_pk_column(conn, table_name)
        fks = await get_foreign_keys(conn, table_name)
        
        q = """
            SELECT column_name, data_type, is_nullable, character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = $1
        """
        cols_info = await conn.fetch(q, table_name)
        
        existing_data = {}
        if is_update:
            try:
                pk_type = next((c['data_type'] for c in cols_info if c['column_name'] == pk_column), 'integer')
                search_val = int(pk_val) if pk_type in ('integer', 'bigint') else pk_val
                existing_data = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", search_val)
                if not existing_data:
                    return response.text("Record not found.", status=404)
                existing_data = dict(existing_data)
            except ValueError:
                return response.text("Invalid ID format.", status=400)
                
        raw_col_names = [c['column_name'] for c in cols_info]
        ordered_cols = sort_columns(raw_col_names, pk_column)
        
        form_fields = []
        for col_name in ordered_cols:
            c = next(x for x in cols_info if x['column_name'] == col_name)
            field = {
                "column_name": col_name,
                "label": col_name.replace('_', ' ').title(),
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO',
                "is_pk": col_name == pk_column,
                "value": existing_data.get(col_name) if is_update else "",
                "options": None,
                "json_options": None
            }
            
            # Populate dropdowns for foreign keys
            if col_name in fks or col_name.endswith('_id'):
                if 'company' in col_name.lower():
                    opts = await conn.fetch("SELECT pcp_company_id as id, pcp_company_name as name FROM phc_companies_t")
                    field["options"] = [{"id": o["id"], "name": o["name"]} for o in opts]
                elif 'role' in col_name.lower():
                    opts = await conn.fetch("SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t WHERE prl_status='ACT'")
                    field["options"] = [{"id": o["id"], "name": o["name"]} for o in opts]
                elif 'screen' in col_name.lower():
                    opts = await conn.fetch("SELECT psn_screen_id as id, psn_screen_name as name FROM phc_screens_t WHERE psn_status='ACT'")
                    field["options"] = [{"id": o["id"], "name": o["name"]} for o in opts]
                    
            form_fields.append(field)
            
    template = template_env.get_template("form_view.html")
    return response.html(template.render(
        table_name=table_name,
        table_title=table_name.replace('_t', '').replace('_', ' ').title(),
        columns=form_fields,
        is_update=is_update,
        pk_val=pk_val,
        pk_column=pk_column,
        all_tables=sorted(auth_tables),
        table_modules=get_table_modules(auth_tables),
        username=request.ctx.session.get('username'),
        user_id=user_id,
        user_role=user_role
    ))

@app.route('/api/<table_name>', methods=['POST'])
@app.route('/api/<table_name>/<pk_val>', methods=['PUT'])
@check_auth
async def save_data(request, table_name, pk_val=None):
    is_update = request.method == 'PUT'
    user_id = request.ctx.session.get('user_id')
    
    # Process multipart form data
    data = {}
    for key, val in request.form.items():
        data[key] = val[0]
        
    for key, file_list in request.files.items():
        if file_list:
            data[key] = file_list[0].body
            
    async with app.ctx.pool.acquire() as conn:
        pk_column = await get_pk_column(conn, table_name)
        schema = await conn.fetch("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {r['column_name']: r for r in schema}
        
        clean_data = {}
        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column: continue 
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue

            if k == 'pus_pwd' and isinstance(v, str):
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')

            col_info = schema_map.get(k, {})
            target_type = col_info.get('data_type', '').lower()
            max_len = col_info.get('character_maximum_length')
            
            # Robust Date Conversion
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

        if not clean_data:
            return response.json({"error": "No data provided"}, status=400)
            
        keys = list(clean_data.keys())
        values = list(clean_data.values())
        
        if is_update:
            set_clause = ", ".join([f"{k} = ${i+1}" for i, k in enumerate(keys)])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ${len(keys)+1}"
            await conn.execute(query, *values, int(pk_val) if str(pk_val).isdigit() else pk_val)
        else:
            # Inject WHO columns if missing
            who_cols = [c for c in schema_map.keys() if c.endswith(('_created', '_modified', '_created_by', '_modified_by', 'creation_date', 'last_update_date'))]
            for w in who_cols:
                if 'created' in w.lower() or 'modified' in w.lower():
                    keys.append(w)
                    values.append(user_id if '_by' in w else datetime.now(timezone.utc))
                    
            # Auto-generate ID (Max + 1)
            pk_type = schema_map[pk_column]['data_type']
            if pk_type in ('integer', 'bigint'):
                max_id = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}") or 0
                keys.append(pk_column)
                values.append(max_id + 1)

            val_placeholders = ", ".join([f"${i+1}" for i in range(len(values))])
            key_names = ", ".join(keys)
            query = f"INSERT INTO {table_name} ({key_names}) VALUES ({val_placeholders})"
            await conn.execute(query, *values)

        return response.json({"status": "success"})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000, single_process=True)
