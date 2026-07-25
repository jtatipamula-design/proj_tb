import os
import json
import uuid
import bcrypt
import jwt
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sanic import Sanic, response
from sanic.exceptions import NotFound
from sanic_ext import render
import asyncpg

# MUST BE AT THE VERY TOP to ensure DATABASE_URL is found
load_dotenv()

app = Sanic("ERP_System")
app.static('/static', './static')

app.config.SECRET_KEY = os.getenv("SECRET_KEY", "fallback_secret_for_dev_only_change_in_prod")

# In-memory fast cache to prevent database hammering
SCHEMA_CACHE = {
    "pks": {},
    "tables": [],
    "lookups": {}
}

@app.before_server_start
async def setup_db(app, loop):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("\n==================================================")
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print("❌ ERROR: DATABASE_URL is MISSING or EMPTY!")
        print("❌ Render is completely blind to your database variable.")
        print("❌ Action: Check Render Dashboard -> Environment Variables.")
        print("==================================================\n")
    else:
        print("\n==================================================")
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print(f"✅ DATABASE_URL Found.")
        print("✅ Environment Variables are loaded correctly.")
        print("==================================================\n")
        
    app.ctx.db = await asyncpg.create_pool(
        dsn=db_url,
        min_size=2,
        max_size=20,
        command_timeout=60
    )

@app.after_server_stop
async def close_db(app, loop):
    await app.ctx.db.close()

@app.middleware('request')
async def add_session(request):
    request.ctx.session = {}
    token = request.cookies.get('auth_token')
    if token:
        try:
            data = jwt.decode(token, app.config.SECRET_KEY, algorithms=["HS256"])
            request.ctx.session['user_id'] = data.get('user_id')
            request.ctx.session['username'] = data.get('username')
            request.ctx.session['role'] = data.get('role')
        except jwt.ExpiredSignatureError:
            pass
        except jwt.InvalidTokenError:
            pass

@app.middleware('response')
async def add_security_headers(request, response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'

def check_auth(wrapped):
    async def decorator(request, *args, **kwargs):
        if not request.ctx.session.get("user_id"):
            if request.method == 'GET':
                return response.redirect('/login')
            return response.json({"error": "Unauthorized"}, status=401)
        return await wrapped(request, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET'])
async def show_login(request):
    if request.ctx.session.get("user_id"):
        return response.redirect('/')
    return await render("login.html")

@app.route('/login', methods=['POST'])
async def handle_login(request):
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.db.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM phc_users_t WHERE pus_user_name = $1 AND pus_status = 'ACT'", username)
        
        if user:
            stored_pwd = user['pus_pwd']
            is_valid = False
            
            try:
                if bcrypt.checkpw(password.encode('utf-8'), stored_pwd.encode('utf-8')):
                    is_valid = True
            except ValueError:
                # Fallback for plain-text passwords stored before bcrypt was implemented
                if password == stored_pwd:
                    is_valid = True
                    
            if is_valid:
                token_data = {
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "role": user.get('pus_user_type', 'STD'),
                    "exp": datetime.utcnow() + timedelta(hours=12)
                }
                token = jwt.encode(token_data, app.config.SECRET_KEY, algorithm="HS256")
                
                resp = response.json({"status": "success", "message": "Login successful"})
                resp.cookies['auth_token'] = token
                resp.cookies['auth_token']['httponly'] = True
                resp.cookies['auth_token']['samesite'] = 'Lax'
                resp.cookies['auth_token']['max-age'] = 43200
                return resp
        
        return response.json({"error": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    resp = response.redirect('/login')
    del resp.cookies['auth_token']
    return resp

async def get_pk_column(table_name, conn):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    
    query = """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                             AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1::regclass
        AND    i.indisprimary;
    """
    try:
        pk = await conn.fetchval(query, table_name)
        if pk:
            SCHEMA_CACHE["pks"][table_name] = pk
            return pk
    except asyncpg.exceptions.UndefinedTableError:
        pass
        
    fallback_query = "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position LIMIT 1;"
    pk = await conn.fetchval(fallback_query, table_name)
    SCHEMA_CACHE["pks"][table_name] = pk
    return pk

async def get_allowed_tables(request, conn):
    user_id = request.ctx.session.get('user_id')
    role = request.ctx.session.get('role')
    
    if role == 'ADM':
        # Admins see everything
        query = "SELECT psn_screen_code FROM phc_screens_t WHERE psn_status = 'ACT'"
        rows = await conn.fetch(query)
    else:
        # Standard users see mapped screens
        query = """
            SELECT s.psn_screen_code 
            FROM phc_screens_t s
            JOIN phc_role_screen_assignment_t rs ON s.psn_screen_id = rs.prs_screen_id
            JOIN phc_user_roles_assignment_t ur ON rs.prs_role_id = ur.pua_role_id
            WHERE ur.pua_user_id = $1 AND ur.pua_status = 'ACT' AND rs.prs_status = 'ACT' AND s.psn_status = 'ACT'
        """
        rows = await conn.fetch(query, user_id)
        
    return [row['psn_screen_code'] for row in rows]

async def get_dropdown_options(table_name, col_name, conn):
    cache_key = f"{table_name}_{col_name}"
    if cache_key in SCHEMA_CACHE["lookups"]:
        return SCHEMA_CACHE["lookups"][cache_key]

    # Explicit handling for Enterprise Lookups and Approvals
    if "lookup_type" in col_name.lower() or col_name.lower() == "plt_lookup_type_code":
        try:
            rows = await conn.fetch("SELECT plt_lookup_type_code as id, plt_lookup_type || ' (' || plt_lookup_type_code || ')' as name FROM phc_lookup_types WHERE plt_status = 'ACT'")
            return [{"id": r["id"], "name": r["name"]} for r in rows]
        except Exception: pass
        
    if "approval_type" in col_name.lower():
        try:
            rows = await conn.fetch("SELECT pat_approval_type as id, pat_approval_type_desc as name FROM phc_approval_types_t WHERE pat_status = 'ACT'")
            return [{"id": r["id"], "name": r["name"]} for r in rows]
        except Exception: pass

    # Generic Table Mappings
    lookup_map = {
        'company_id': ('phc_companies_t', 'pcp_company_id', 'pcp_company_name'),
        'org_id': ('phc_orgs_t', 'pos_org_id', 'pos_org_name'),
        'cost_center_id': ('phc_cost_center_t', 'pcc_cost_center_id', 'pcc_cost_center_name'),
        'services_id': ('phc_services_t', 'pse_services_id', 'pse_services_name'),
        'dept_id': ('phc_dept_t', 'pdp_dept_id', 'pdp_dept_name'),
        'product_id': ('cv_product_registration_t', 'product_id', 'product_name'),
        'equipment_id': ('cv_equipment_registration_t', 'equipment_id', 'equipment_name'),
        'pma_parent_product_id': ('phc_prod_master', 'pma_product_id', 'pma_product_name'),
        'pmat_material_group_id': ('phc_material_group_master', 'pmgm_material_group_id', 'pmgm_group_name'),
        'puc_from_uom_id': ('phc_uom_master', 'pum_uom_id', 'pum_uom_name'),
        'puc_to_uom_id': ('phc_uom_master', 'pum_uom_id', 'pum_uom_name'),
        'pmat_base_uom_id': ('phc_uom_master', 'pum_uom_id', 'pum_uom_name'),
        'pmat_alt_uom_id': ('phc_uom_master', 'pum_uom_id', 'pum_uom_name'),
    }

    # Match by exact column name or suffix
    mapping = lookup_map.get(col_name)
    if not mapping:
        for suffix, target in lookup_map.items():
            if col_name.endswith(suffix):
                mapping = target
                break

    if mapping:
        target_table, target_id, target_name = mapping
        try:
            query = f"SELECT {target_id}, {target_name} FROM {target_table}"
            if target_table == 'phc_companies_t': query += " WHERE pcp_status = 'ACT'"
            rows = await conn.fetch(query)
            options = [{"id": r[target_id], "name": r[target_name]} for r in rows]
            SCHEMA_CACHE["lookups"][cache_key] = options
            return options
        except asyncpg.exceptions.UndefinedTableError:
            pass

    return []

def get_table_modules():
    return {
        # General Ledger
        'pgl_batches_t': 'Ledger', 'pgl_headers_t': 'Ledger', 'pgl_lines_t': 'Ledger',
        'pgl_sources_t': 'Ledger', 'pgl_daily_rates_t': 'Ledger', 'pgl_balances_t': 'Ledger',
        
        # Master Data
        'pmd_parties_t': 'MasterData', 'pmd_accounts_t': 'CustomerSetup', 'pmd_acct_sites_t': 'CustomerSetup',
        'pmd_locations_t': 'MasterData', 'pmd_person_profiles_t': 'MasterData', 'phc_orgs_t': 'MasterData',
        'phc_lookup_types': 'MasterData', 'phc_lookup_values_t': 'MasterData',
        
        # Plant & Equipment Master Data
        'phc_plant_master': 'MasterData', 'phc_plant_compliance': 'MasterData', 'phc_certifications': 'MasterData',
        'phc_plant_equipment': 'MasterData', 'phc_equipment_locations': 'MasterData',
        
        # Inventory & Material Master
        'mtl_system_items_t': 'Product', 'mtl_item_locations_t': 'Product',
        'phc_material_group_master': 'Product', 'phc_material_master': 'Product',
        'phc_uom_master': 'Product', 'phc_uom_conversion': 'Product',
        'phc_prod_master': 'Product', 'phc_prod_lifecycle_history': 'Product', 'phc_prod_alt_names': 'Product',
        
        # Accounts Receivable
        'pra_customer_trx_t': 'Receivables', 'pra_customer_trx_lines_t': 'Receivables',
        'pra_cust_trx_line_dist_t': 'Receivables', 'pra_cust_trx_types_t': 'Receivables',
        'par_payment_schedules_t': 'Receivables', 'par_batch_sources_t': 'Receivables',
        
        # Order Management
        'poe_order_headers_t': 'OrderMgmt', 'poe_order_lines_t': 'OrderMgmt', 
        'poe_order_sources_t': 'OrderMgmt', 'poe_transaction_types_t': 'OrderMgmt',
        
        # Procurement
        'po_requisition_headers_t': 'Procurement', 'po_requisition_lines_t': 'Procurement',
        'po_req_distributions_t': 'Procurement', 'po_headers_t': 'Procurement',
        'po_lines_t': 'Procurement', 'po_distributions_t': 'Procurement',
        
        # Accounts Payable
        'ap_invoices_t': 'Payables', 'ap_invoice_distributions_t': 'Payables', 'ap_payments_schedules_t': 'Payables',
        
        # Project Accounting
        'pa_projects_t': 'Project', 'pa_tasks_t': 'Project', 'pa_expenditure_items_t': 'Project',
        'pa_expenditures_t': 'Project', 'pa_resource_assignments_t': 'Project',
        
        # Cleaning Validation
        'cv_product_registration_t': 'Cleaning', 'cv_equipment_registration_t': 'Cleaning',
        'cv_product_equipment_map_t': 'Cleaning', 'cv_product_apis_t': 'Cleaning',
        
        # App Setup & Security
        'phc_emp_t': 'Employee', 'phc_dept_t': 'Employee',
        'phc_users_t': 'AppSetup', 'phc_roles_t': 'AppSetup', 'phc_screens_t': 'AppSetup',
        'phc_companies_t': 'AppSetup', 'phc_role_screen_assignment_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup', 'phc_apps_t': 'AppSetup',
        'phc_approval_types_t': 'AppSetup', 'phc_approval_setup_t': 'AppSetup',
        'phc_notifications_setup_t': 'AppSetup', 'phc_approval_events_t': 'AppSetup'
    }

@app.route('/')
@check_auth
async def dashboard(request):
    async with app.ctx.db.acquire() as conn:
        allowed_tables = await get_allowed_tables(request, conn)
        SCHEMA_CACHE["tables"] = allowed_tables
        
        emp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t WHERE pem_status='ACT'") if 'phc_emp_t' in allowed_tables else 0
        comp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t WHERE pcp_status='ACT'") if 'phc_companies_t' in allowed_tables else 0
        dept_count = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t WHERE pdp_status='ACT'") if 'phc_dept_t' in allowed_tables else 0
        app_count = await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t WHERE pap_status='ACT'") if 'phc_apps_t' in allowed_tables else 0

        return await render(
            "dashboard.html",
            context={
                "username": request.ctx.session.get('username'),
                "user_id": request.ctx.session.get('user_id'),
                "stats": {"emp_count": emp_count, "comp_count": comp_count, "dept_count": dept_count, "app_count": app_count},
                "all_tables": allowed_tables,
                "table_modules": get_table_modules()
            }
        )

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    if table_name not in SCHEMA_CACHE.get("tables", []):
        raise NotFound("Table not found or unauthorized")

    page = int(request.args.get("page", 1))
    search_query = request.args.get("q", "").strip()
    type_filter = request.args.get("type_filter", "").strip()
    
    limit = 50
    offset = (page - 1) * limit

    async with app.ctx.db.acquire() as conn:
        pk_column = await get_pk_column(table_name, conn)
        
        col_query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position;"
        cols = await conn.fetch(col_query, table_name)
        
        if not cols:
            raise NotFound("Table schema not found")
            
        columns = [{"raw": c["column_name"], "label": c["column_name"].replace('_', ' ').title()} for c in cols if 'password' not in c["column_name"].lower() and 'pwd' not in c["column_name"].lower()]

        # Master Detail Lookups Logic
        if table_name == 'phc_lookup_types':
            types = await conn.fetch("SELECT * FROM phc_lookup_types ORDER BY plt_lookup_type_code")
            return await render("lookups_view.html", context={
                "table_name": table_name,
                "rows": types,
                "all_tables": SCHEMA_CACHE["tables"],
                "table_modules": get_table_modules()
            })

        query_conditions = []
        params = []
        
        if search_query:
            text_cols = [c["column_name"] for c in cols if c["data_type"] in ('character varying', 'text')]
            if text_cols:
                search_conds = [f"{c} ILIKE ${len(params)+1}" for c in text_cols]
                params.append(f"%{search_query}%")
                query_conditions.append(f"({' OR '.join(search_conds)})")
                
        if type_filter and table_name == 'phc_lookup_values_t':
            query_conditions.append(f"plv_lookup_type_code = ${len(params)+1}")
            params.append(type_filter)

        where_clause = f" WHERE {' AND '.join(query_conditions)}" if query_conditions else ""
        
        count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
        total_count = await conn.fetchval(count_query, *params)
        total_pages = max(1, (total_count + limit - 1) // limit)
        
        data_query = f"SELECT * FROM {table_name} {where_clause} ORDER BY {pk_column} DESC LIMIT {limit} OFFSET {offset}"
        rows = await conn.fetch(data_query, *params)
        
        # Inject Lookup Categories Dropdown for the master values UI
        lookup_categories = []
        if table_name == 'phc_lookup_values_t':
            cat_rows = await conn.fetch("SELECT plt_lookup_type_code, plt_lookup_type FROM phc_lookup_types ORDER BY plt_lookup_type_code")
            lookup_categories = [{"code": r['plt_lookup_type_code'], "name": r['plt_lookup_type']} for r in cat_rows]

        clean_name = table_name.replace('phc_', '').replace('_t', '').replace('_', ' ').title()

        return await render(
            "table_view.html",
            context={
                "table_title": clean_name,
                "table_name": table_name,
                "columns": columns,
                "rows": rows,
                "pk_column": pk_column,
                "search_query": search_query,
                "type_filter": type_filter,
                "lookup_categories": lookup_categories,
                "page": page,
                "total_pages": total_pages,
                "total_count": total_count,
                "start_row": offset + 1 if total_count > 0 else 0,
                "end_row": min(offset + limit, total_count),
                "all_tables": SCHEMA_CACHE["tables"],
                "table_modules": get_table_modules()
            }
        )

@app.route('/new/<table_name>')
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>')
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

async def render_form(request, table_name, is_update=False, pk_val=None):
    if table_name not in SCHEMA_CACHE.get("tables", []):
        raise NotFound("Table not found or unauthorized")

    async with app.ctx.db.acquire() as conn:
        pk_column = await get_pk_column(table_name, conn)
        
        col_query = """
            SELECT column_name, data_type, is_nullable, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = $1 ORDER BY ordinal_position;
        """
        cols = await conn.fetch(col_query, table_name)
        
        if not cols:
            raise NotFound("Table schema not found")
            
        SCHEMA_CACHE[f"{table_name}_schema"] = {c['column_name']: dict(c) for c in cols}

        row_data = {}
        if is_update:
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", pk_val if not pk_val.isdigit() else int(pk_val))
            if row: row_data = dict(row)

        form_columns = []
        for c in cols:
            cname = c['column_name']
            
            # Hide audit columns from forms permanently
            if cname.lower() in ('creation_date', 'last_update_date', 'created_by', 'last_updated_by', 'psn_created', 'psn_modified'):
                continue
            if cname.endswith(('_created', '_modified', '_created_by', '_modified_by')):
                continue
                
            col_info = {
                "column_name": cname,
                "label": cname.replace('_', ' ').title(),
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO',
                "is_pk": cname == pk_column,
                "value": row_data.get(cname, ''),
                "options": await get_dropdown_options(table_name, cname, conn)
            }
            
            # Arrays logic for JSON fields
            if cname in ('pr_allowed_tables', 'pu_assigned_roles'):
                col_info["json_options"] = []
                if cname == 'pr_allowed_tables':
                    all_screens = await conn.fetch("SELECT psn_screen_code, psn_screen_name FROM phc_screens_t")
                    col_info["json_options"] = [{"id": s['psn_screen_code'], "name": s['psn_screen_name']} for s in all_screens]
                elif cname == 'pu_assigned_roles':
                    all_roles = await conn.fetch("SELECT prl_role_code, prl_role_name FROM phc_roles_t")
                    col_info["json_options"] = [{"id": r['prl_role_code'], "name": r['prl_role_name']} for r in all_roles]
                
                try:
                    if col_info["value"] and isinstance(col_info["value"], str):
                        col_info["value"] = json.loads(col_info["value"])
                except:
                    col_info["value"] = []
                    
            form_columns.append(col_info)

        clean_name = table_name.replace('phc_', '').replace('_t', '').replace('_', ' ').title()

        return await render(
            "form_view.html",
            context={
                "table_title": clean_name,
                "table_name": table_name,
                "columns": form_columns,
                "is_update": is_update,
                "pk_val": pk_val,
                "pk_column": pk_column,
                "all_tables": SCHEMA_CACHE["tables"],
                "table_modules": get_table_modules()
            }
        )

# Explicit route names prevent Sanic duplicate route ServerError crashes
@app.route('/api/<table_name>', methods=['POST'], name="post_save_data")
@app.route('/api/<table_name>/<pk_val>', methods=['PUT'], name="put_save_data")
@check_auth
async def save_data(request, table_name, pk_val=None):
    if request.ctx.session.get('role') == 'STD' and table_name in ('phc_users_t', 'phc_roles_t', 'phc_screens_t'):
        return response.json({"error": "Unauthorized"}, status=403)

    data = request.json
    is_update = request.method == 'PUT'
    
    async with app.ctx.db.acquire() as conn:
        pk_column = await get_pk_column(table_name, conn)
        schema_map = SCHEMA_CACHE.get(f"{table_name}_schema", {})
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer').lower()

        clean_data = {}
        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column: continue 
            
            # Ensure audit columns aren't overridden maliciously
            if k.lower() in ('creation_date', 'last_update_date', 'created_by', 'last_updated_by', 'psn_created', 'psn_modified'):
                continue
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')):
                continue

            # Auto-Hash Passwords
            if k == 'pus_pwd':
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
        
        # --- AUTO-ID GENERATOR (Replaces Custom Code Box) ---
        if not is_update:
            if 'integer' in pk_type or 'bigint' in pk_type or 'numeric' in pk_type:
                # Numeric Fallback
                try:
                    next_id = await conn.fetchval(f"SELECT COALESCE(MAX({pk_column}), 0) + 1 FROM {table_name}")
                    clean_data[pk_column] = next_id
                except Exception:
                    clean_data[pk_column] = int(datetime.now().timestamp())
            else:
                # Generate unique string ID for varchar PKs (e.g. REC-A492B)
                clean_data[pk_column] = f"REC-{str(uuid.uuid4())[:8].upper()}"
        
        # Prevent privilege escalation
        if table_name == 'phc_users_t' and request.ctx.session.get('role') != 'ADM':
            if 'pus_user_type' in clean_data: del clean_data['pus_user_type']

        try:
            if is_update:
                set_clauses = [f"{k} = ${i+1}" for i, k in enumerate(clean_data.keys())]
                values = list(clean_data.values())
                values.append(pk_val if not pk_val.isdigit() else int(pk_val))
                query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = ${len(values)}"
                await conn.execute(query, *values)
            else:
                cols = list(clean_data.keys())
                placeholders = [f"${i+1}" for i in range(len(cols))]
                query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
                await conn.execute(query, *clean_data.values())
            
            return response.json({"status": "success"}, headers={
                "HX-Trigger": json.dumps({"showMessage": {"msg": "Record saved successfully!", "type": "success"}})
            })
        except Exception as e:
            return response.json({"error": str(e)}, status=400)

@app.route('/api/<table_name>/<pk_val>', methods=['DELETE'])
@check_auth
async def delete_data(request, table_name, pk_val):
    if request.ctx.session.get('role') == 'STD' and table_name in ('phc_users_t', 'phc_roles_t', 'phc_screens_t'):
        return response.json({"error": "Unauthorized"}, status=403)

    async with app.ctx.db.acquire() as conn:
        pk_column = await get_pk_column(table_name, conn)
        
        try:
            cols = await conn.fetch(f"SELECT column_name FROM information_schema.columns WHERE table_name = $1", table_name)
            col_names = [c['column_name'].lower() for c in cols]
            
            status_col = None
            for c in col_names:
                if 'status' in c:
                    status_col = c
                    break
                    
            if status_col:
                query = f"UPDATE {table_name} SET {status_col} = 'INA' WHERE {pk_column} = $1"
            else:
                query = f"DELETE FROM {table_name} WHERE {pk_column} = $1"
                
            await conn.execute(query, pk_val if not pk_val.isdigit() else int(pk_val))
            
            return response.json({"status": "success"}, headers={
                "HX-Redirect": f"/table/{table_name}"
            })
        except Exception as e:
            return response.json({"error": str(e)}, status=400)

@app.route('/api/lookup_values/<type_code>')
@check_auth
async def get_lookup_values(request, type_code):
    async with app.ctx.db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM phc_lookup_values_t WHERE plv_lookup_type_code = $1 ORDER BY plv_lookup_value_code", type_code)
        return await render("lookups_partial.html", context={"rows": rows, "type_code": type_code})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    is_development = os.environ.get("ENVIRONMENT") != "production"
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)
