import os
import math
import json
import uuid
import logging
from datetime import datetime
from functools import wraps

from sanic import Sanic, response
from sanic.exceptions import ServerError
import asyncpg
import bcrypt
import jwt
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Sanic("ERP_System")
app.static('/static', './static')

env = Environment(loader=FileSystemLoader('./templates'))

# Global Cache
SCHEMA_CACHE = {}

def clean_label(col_name):
    """Removes short prefixes (like pus_) and converts to Title Case"""
    parts = col_name.split('_')
    if len(parts) > 1 and len(parts[0]) <= 4:
        parts = parts[1:]
    return ' '.join(parts).title().replace(' Id', ' ID').replace(' Uom', ' UOM')

def clean_table_name(t_name):
    """Removes short prefixes (like phc_) and table suffixes (_t)"""
    clean = t_name.replace('_t', '')
    parts = clean.split('_')
    if len(parts) > 1 and len(parts[0]) <= 4:
        parts = parts[1:]
    return ' '.join(parts).title()

def sort_columns(col_names, pk_column):
    """Sorts columns: PK first, normal columns, audit columns last"""
    audit_keywords = ['created', 'modified', 'creation_date', 'last_update_date', 'updated_by', 'created_by', 'status']
    
    def weight(c):
        cl = c.lower()
        if c == pk_column: return 0
        if any(k in cl for k in audit_keywords): return 2
        return 1
        
    return sorted(col_names, key=lambda c: (weight(c), c))

def get_table_modules():
    return {
        # General Ledger
        'pgl_batches_t': 'Ledger', 'pgl_headers_t': 'Ledger', 'pgl_lines_t': 'Ledger',
        'pgl_sources_t': 'Ledger', 'pgl_daily_rates_t': 'Ledger', 'pgl_balances_t': 'Ledger',
        'pgl_acc_periods_t': 'Ledger', 'pgl_period_sets_t': 'Ledger', 'pgl_code_combinations_t': 'Ledger',
        
        # Receivables
        'pra_customer_trx_t': 'Receivables', 'pra_customer_trx_lines_t': 'Receivables',
        'pra_cust_trx_line_dist_t': 'Receivables', 'pra_cust_trx_types_t': 'Receivables',
        'par_payment_schedules_t': 'Receivables', 'par_batch_sources_t': 'Receivables',
        'par_vat_tax_t': 'Receivables', 'par_terms_t': 'Receivables', 'par_periods_t': 'Receivables',
        'par_period_types_t': 'Receivables',
        
        # Payables & Purchasing
        'po_requisition_headers_t': 'Payables', 'po_requisition_lines_t': 'Payables',
        'po_req_distributions_t': 'Payables', 'po_headers_t': 'Payables',
        'po_lines_t': 'Payables', 'po_distributions_t': 'Payables',
        'ap_invoices_t': 'Payables', 'ap_invoice_distributions_t': 'Payables',
        'ap_payments_schedules_t': 'Payables',
        
        # Order Management
        'poe_order_headers_t': 'OrderMgmt', 'poe_order_lines_t': 'OrderMgmt',
        'poe_order_sources_t': 'OrderMgmt', 'poe_transaction_types_t': 'OrderMgmt',
        
        # Projects
        'pa_projects_t': 'Project', 'pa_tasks_t': 'Project', 'pa_expenditures_t': 'Project',
        'pa_expenditure_items_t': 'Project', 'pa_resource_assignments_t': 'Project',
        
        # Inventory
        'mtl_system_items_t': 'Product', 'mtl_item_locations_t': 'Product',
        'phc_material_group_master': 'Product', 'phc_material_master': 'Product',
        'phc_prod_master': 'Product', 'phc_prod_lifecycle_history': 'Product', 'phc_prod_alt_names': 'Product',
        
        # Master Data
        'pmd_parties_t': 'MasterData', 'pmd_accounts_t': 'MasterData', 'pmd_acct_sites_t': 'MasterData',
        'pmd_locations_t': 'MasterData', 'pmd_person_profiles_t': 'MasterData',
        'phc_plant_master': 'MasterData', 'phc_plant_compliance': 'MasterData', 'phc_certifications': 'MasterData',
        'phc_plant_equipment': 'MasterData', 'phc_equipment_locations': 'MasterData',
        'phc_uom_master': 'MasterData', 'phc_uom_conversion': 'MasterData',
        'phc_lookup_types': 'MasterData', 'phc_lookup_values_t': 'MasterData',
        
        # Employees
        'phc_emp_t': 'Employee', 'phc_dept_t': 'Employee', 'phc_cost_center_t': 'Employee',
        'phc_orgs_t': 'Employee',
        
        # Cleaning Validation
        'cv_product_registration_t': 'Cleaning', 'cv_equipment_registration_t': 'Cleaning',
        'cv_product_equipment_map_t': 'Cleaning', 'cv_product_apis_t': 'Cleaning',
        
        # App Setup
        'phc_users_t': 'AppSetup', 'phc_roles_t': 'AppSetup', 'phc_apps_t': 'AppSetup',
        'phc_screens_t': 'AppSetup', 'phc_companies_t': 'AppSetup', 'phc_role_screen_assignment_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup', 'phc_menu_folders_t': 'AppSetup',
        'phc_approval_types_t': 'AppSetup', 'phc_approval_setup_t': 'AppSetup',
        'phc_notifications_setup_t': 'AppSetup', 'phc_approval_events_t': 'AppSetup',
        'phc_lov_types_t': 'AppSetup', 'phc_lov_values_t': 'AppSetup', 'phc_column_lov_map_t': 'AppSetup'
    }

@app.before_server_start
async def setup_db(app, loop):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("\n" + "="*50)
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print("❌ ERROR: DATABASE_URL is MISSING or EMPTY!")
        print("❌ Render is completely blind to your database variable.")
        print("❌ Action: Check Render Dashboard -> Environment Variables.")
        print("="*50 + "\n")
        return
        
    print("\n" + "="*50)
    print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
    print(f"✅ DATABASE_URL Found: {db_url[:35]}...")
    print("✅ Environment Variables are loaded correctly.")
    print("="*50 + "\n")
    
    app.ctx.db = await asyncpg.create_pool(dsn=db_url, min_size=2, max_size=20)

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'db'):
        await app.ctx.db.close()

@app.middleware("request")
async def add_session(request):
    request.ctx.session = {}
    token = request.cookies.get("auth_token")
    if token:
        try:
            payload = jwt.decode(token, os.getenv("SECRET_KEY", "fallback_secret"), algorithms=["HS256"])
            request.ctx.session['user_id'] = payload.get("user_id")
            request.ctx.session['username'] = payload.get("username")
            request.ctx.session['role'] = payload.get("role")
        except jwt.ExpiredSignatureError:
            pass
        except jwt.InvalidTokenError:
            pass

@app.middleware("response")
async def add_security_headers(request, response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"

def check_auth(wrapped):
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        if not request.ctx.session.get("user_id"):
            return response.redirect("/login")
        return await wrapped(request, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET'])
async def login_view(request):
    if request.ctx.session.get("user_id"):
        return response.redirect("/")
    template = env.get_template('login.html')
    return response.html(template.render())

@app.route('/login', methods=['POST'])
async def handle_login(request):
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
                token = jwt.encode({
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "role": user.get('pus_user_type', 'STD'),
                    "exp": datetime.utcnow().timestamp() + 86400
                }, os.getenv("SECRET_KEY", "fallback_secret"), algorithm="HS256")
                
                res = response.json({"status": "success", "message": "Login successful"})
                res.cookies["auth_token"] = token
                res.cookies["auth_token"]["httponly"] = True
                res.cookies["auth_token"]["samesite"] = "Lax"
                return res
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    res = response.redirect("/login")
    del res.cookies["auth_token"]
    return res

@app.route('/')
@check_auth
async def dashboard(request):
    template = env.get_template('dashboard.html')
    
    async with app.ctx.db.acquire() as conn:
        all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
        
        stats = {
            'emp_count': await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t") if 'phc_emp_t' in all_tables else 0,
            'comp_count': await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t") if 'phc_companies_t' in all_tables else 0,
            'dept_count': await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t") if 'phc_dept_t' in all_tables else 0,
            'app_count': await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t") if 'phc_apps_t' in all_tables else 0
        }
        
    return response.html(template.render(
        username=request.ctx.session.get('username'),
        user_id=request.ctx.session.get('user_id'),
        stats=stats,
        all_tables=all_tables,
        table_modules=get_table_modules(),
        table_name=None
    ))

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    template = env.get_template('table_view.html')
    page = int(request.args.get('page', 1))
    search_query = request.args.get('q', '').strip()
    type_filter = request.args.get('type_filter', '').strip()
    per_page = 50
    offset = (page - 1) * per_page
    
    async with app.ctx.db.acquire() as conn:
        try:
            cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
            if not cols:
                return response.html(f"<h3>Table {table_name} not found.</h3>")
                
            raw_col_names = [c['column_name'] for c in cols]
            
            pk_record = await conn.fetchrow("""
                SELECT kcu.column_name 
                FROM information_schema.table_constraints tco
                JOIN information_schema.key_column_usage kcu 
                  ON kcu.constraint_name = tco.constraint_name 
                 AND kcu.constraint_schema = tco.constraint_schema
                WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
            """, table_name)
            pk_column = pk_record['column_name'] if pk_record else raw_col_names[0]
            
            col_names = sort_columns(raw_col_names, pk_column)
            
            where_clause = ""
            params = []
            
            if search_query:
                conditions = []
                for c in cols:
                    if c['data_type'] in ['character varying', 'text', 'character']:
                        conditions.append(f"{c['column_name']} ILIKE ${len(params) + 1}")
                    elif c['data_type'] in ['integer', 'bigint', 'numeric'] and search_query.isdigit():
                        conditions.append(f"{c['column_name']}::text = ${len(params) + 1}")
                if conditions:
                    params.append(f"%{search_query}%")
                    where_clause = "WHERE (" + " OR ".join(conditions) + ")"

            if table_name == 'phc_lookup_values_t' and type_filter:
                filter_cond = f"plv_lookup_type_code = ${len(params) + 1}"
                params.append(type_filter)
                where_clause = f"WHERE {filter_cond}" if not where_clause else f"{where_clause} AND {filter_cond}"

            count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
            total_count = await conn.fetchval(count_query, *params)
            total_pages = math.ceil(total_count / per_page) if total_count > 0 else 1
            
            data_query = f"SELECT * FROM {table_name} {where_clause} ORDER BY {pk_column} DESC LIMIT {per_page} OFFSET {offset}"
            rows = await conn.fetch(data_query, *params)
            
            columns = [{"raw": c, "label": clean_label(c)} for c in col_names if c != 'company_id']
            
            lookup_categories = []
            if table_name == 'phc_lookup_values_t':
                try:
                    cats = await conn.fetch("SELECT plt_lookup_type_code as code, plt_lookup_type as name FROM phc_lookup_types WHERE plt_status = 'ACT'")
                    lookup_categories = [dict(c) for c in cats]
                except Exception:
                    pass

            all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
            
            return response.html(template.render(
                table_name=table_name,
                table_title=clean_table_name(table_name),
                columns=columns,
                rows=rows,
                pk_column=pk_column,
                page=page,
                total_pages=total_pages,
                total_count=total_count,
                start_row=offset + 1 if total_count > 0 else 0,
                end_row=min(offset + per_page, total_count),
                search_query=search_query,
                lookup_categories=lookup_categories,
                type_filter=type_filter,
                all_tables=all_tables,
                table_modules=get_table_modules(),
                username=request.ctx.session.get('username'),
                user_id=request.ctx.session.get('user_id')
            ))
        except Exception as e:
            return response.html(f"<h3>Error loading table: {str(e)}</h3>")

async def render_form(request, table_name, is_update=False, pk_val=None):
    template = env.get_template('form_view.html')
    async with app.ctx.db.acquire() as conn:
        cols = await conn.fetch("SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_name = $1", table_name)
        
        pk_record = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name 
            WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        """, table_name)
        pk_column = pk_record['column_name'] if pk_record else cols[0]['column_name']
        
        raw_col_names = [c['column_name'] for c in cols]
        sorted_col_names = sort_columns(raw_col_names, pk_column)
        col_info = {c['column_name']: c for c in cols}
        
        row = None
        if is_update:
            try:
                cast_val = int(pk_val) if col_info[pk_column]['data_type'] in ('integer', 'bigint', 'smallint') else pk_val
                row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", cast_val)
            except Exception as e:
                return response.html(f"Error fetching record: {str(e)}")
            
        columns_data = []
        for cname in sorted_col_names:
            c = col_info[cname]
            is_pk = (cname == pk_column)
            val = row[cname] if row else request.args.get(cname, '')
            
            # Pre-fill date fields on new records
            if not is_update and not val:
                if 'date' in c['data_type'].lower() or 'timestamp' in c['data_type'].lower():
                    if 'start' in cname.lower() or 'creation' in cname.lower() or 'date' in cname.lower():
                        val = datetime.now().strftime('%Y-%m-%d')
            
            options = []
            if cname == 'status' or cname.endswith('_status'):
                options = [{'id': 'ACT', 'name': 'Active'}, {'id': 'INA', 'name': 'Inactive'}]
            elif 'lookup_type_code' in cname.lower():
                try:
                    types = await conn.fetch("SELECT plt_lookup_type_code, plt_lookup_type FROM phc_lookup_types")
                    options = [{'id': t['plt_lookup_type_code'], 'name': f"{t['plt_lookup_type']} ({t['plt_lookup_type_code']})"} for t in types]
                except Exception:
                    pass
                 
            json_options = []
            if c['data_type'] in ('json', 'jsonb'):
                if cname == 'pr_allowed_tables':
                    try:
                        tabs = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'phc_%'")
                        json_options = [{'id': t['tablename'], 'name': clean_table_name(t['tablename'])} for t in tabs]
                    except Exception: pass
                elif cname == 'pu_assigned_roles':
                    try:
                        roles = await conn.fetch("SELECT prl_role_id, prl_role_name FROM phc_roles_t WHERE prl_status = 'ACT'")
                        json_options = [{'id': str(r['prl_role_id']), 'name': r['prl_role_name']} for r in roles]
                    except Exception: pass

            columns_data.append({
                "column_name": cname,
                "label": clean_label(cname),
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO',
                "is_pk": is_pk,
                "value": val,
                "options": options,
                "json_options": json_options
            })
            
        all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
        
        return response.html(template.render(
            table_name=table_name,
            table_title=clean_table_name(table_name),
            columns=columns_data,
            pk_column=pk_column,
            is_update=is_update,
            pk_val=pk_val,
            all_tables=all_tables,
            table_modules=get_table_modules(),
            username=request.ctx.session.get('username'),
            user_id=request.ctx.session.get('user_id')
        ))

@app.route('/new/<table_name>')
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>')
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

@app.route('/api/<table_name>', methods=['POST'])
@app.route('/api/<table_name>/<pk_val>', methods=['PUT'], name="put_save_data")
@check_auth
async def save_data(request, table_name, pk_val=None):
    data = request.json
    is_update = request.method == 'PUT'
    
    async with app.ctx.db.acquire() as conn:
        cols = await conn.fetch("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {c['column_name']: c for c in cols}
        col_names = list(schema_map.keys())
        
        pk_record = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name
            WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        """, table_name)
        pk_column = pk_record['column_name'] if pk_record else col_names[0]
        
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
            
            if target_type in ('json', 'jsonb') and isinstance(v, str):
                try: v = json.loads(v)
                except: pass
            
            if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                 if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v

        audit_cols = [c for c in col_names if c.endswith(('_created', '_modified', '_created_by', '_modified_by', 'creation_date', 'last_update_date', 'created_by', 'last_updated_by'))]
        for ac in audit_cols:
            if 'date' in ac or 'created' in ac or 'modified' in ac:
                clean_data[ac] = datetime.now()
            if 'by' in ac:
                clean_data[ac] = request.ctx.session.get('user_id', 1)

        company_cols = [c for c in col_names if c == 'company_id' or c.endswith('_company_id')]
        if company_cols and not is_update:
            clean_data[company_cols[0]] = 1001

        try:
            if not is_update:
                pk_type = schema_map[pk_column]['data_type'].lower()
                new_id = None
                if pk_type in ('integer', 'bigint', 'smallint'):
                    max_id = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                    new_id = (max_id or 0) + 1
                else:
                    prefix = table_name.split('_')[1][:3].upper() if len(table_name.split('_')) > 1 else 'REC'
                    new_id = f"{prefix}-{str(uuid.uuid4())[:8].upper()}"
                
                clean_data[pk_column] = new_id
                
                keys = list(clean_data.keys())
                vals = list(clean_data.values())
                placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
                query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({placeholders})"
                
                await conn.execute(query, *vals)
                msg = f"Record {new_id} Created Successfully!"
                
            else:
                pk_type = schema_map[pk_column]['data_type'].lower()
                cast_pk = int(pk_val) if pk_type in ('integer', 'bigint', 'smallint') else pk_val
                
                set_clauses = []
                vals = []
                for i, (k, v) in enumerate(clean_data.items()):
                    set_clauses.append(f"{k} = ${i+1}")
                    vals.append(v)
                
                vals.append(cast_pk)
                query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {pk_column} = ${len(vals)}"
                
                await conn.execute(query, *vals)
                msg = f"Record Updated Successfully!"

            return response.html(
                f"<script>sessionStorage.setItem('pendingToast', JSON.stringify({{msg: '{msg}', type: 'success'}})); window.location.href='/table/{table_name}';</script>"
            )

        except Exception as e:
            return response.json({"error": str(e)}, status=500)

@app.route('/api/<table_name>/<pk_val>', methods=['DELETE'])
@check_auth
async def delete_data(request, table_name, pk_val):
    async with app.ctx.db.acquire() as conn:
        try:
            cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
            col_names = [c['column_name'] for c in cols]
            
            pk_record = await conn.fetchrow("""
                SELECT kcu.column_name 
                FROM information_schema.table_constraints tco
                JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name
                WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
            """, table_name)
            pk_column = pk_record['column_name'] if pk_record else col_names[0]
            pk_type = next((c['data_type'] for c in cols if c['column_name'] == pk_column), 'text').lower()
            cast_pk = int(pk_val) if pk_type in ('integer', 'bigint', 'smallint') else pk_val

            status_col = next((c for c in col_names if 'status' in c.lower()), None)
            
            if status_col:
                await conn.execute(f"UPDATE {table_name} SET {status_col} = 'INA' WHERE {pk_column} = $1", cast_pk)
                msg = "Record successfully deactivated (Soft Delete)."
            else:
                await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", cast_pk)
                msg = "Record deleted permanently."

            return response.html(
                f"<script>sessionStorage.setItem('pendingToast', JSON.stringify({{msg: '{msg}', type: 'success'}})); window.location.href='/table/{table_name}';</script>"
            )
        except Exception as e:
            return response.json({"error": str(e)}, status=500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    is_development = os.environ.get("RENDER") is None
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)
