import os
import math
import uuid
import bcrypt
import jwt
from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv

from sanic import Sanic, response
from jinja2 import Environment, FileSystemLoader
import asyncpg

# Load Environment Variables FIRST before anything else
load_dotenv()

app = Sanic("ERP_System")
env = Environment(loader=FileSystemLoader("templates"))
SECRET_KEY = os.environ.get("SECRET_KEY", "super-secret-erp-key-2026")

# Global Cache to speed up database queries
SCHEMA_CACHE = {}

def get_table_modules():
    """Static dictionary to map tables to sidebar folders without database queries"""
    return {
        # General Ledger
        'pgl_batches_t': 'Ledger',
        'pgl_headers_t': 'Ledger',
        'pgl_lines_t': 'Ledger',
        'pgl_sources_t': 'Ledger',
        'pgl_daily_rates_t': 'Ledger',
        'pgl_balances_t': 'Ledger',
        'pgl_acc_periods_t': 'Ledger',
        'pgl_period_sets_t': 'Ledger',
        'pgl_code_combinations_t': 'Ledger',
        
        # Receivables
        'pra_customer_trx_t': 'Receivables',
        'pra_customer_trx_lines_t': 'Receivables',
        'pra_cust_trx_line_dist_t': 'Receivables',
        'pra_cust_trx_types_t': 'Receivables',
        'par_payment_schedules_t': 'Receivables',
        'par_batch_sources_t': 'Receivables',
        'par_vat_tax_t': 'Receivables',
        'par_terms_t': 'Receivables',
        'par_periods_t': 'Receivables',
        'par_period_types_t': 'Receivables',
        
        # Payables
        'ap_invoices_t': 'Payables',
        'ap_invoice_distributions_t': 'Payables',
        'ap_payments_schedules_t': 'Payables',
        
        # Procurement
        'po_requisition_headers_t': 'Procurement',
        'po_requisition_lines_t': 'Procurement',
        'po_req_distributions_t': 'Procurement',
        'po_headers_t': 'Procurement',
        'po_lines_t': 'Procurement',
        'po_distributions_t': 'Procurement',
        
        # Order Management
        'poe_order_headers_t': 'OrderMgmt',
        'poe_order_lines_t': 'OrderMgmt',
        'poe_order_sources_t': 'OrderMgmt',
        'poe_transaction_types_t': 'OrderMgmt',
        
        # Projects
        'pa_projects_t': 'Project',
        'pa_tasks_t': 'Project',
        'pa_expenditure_items_t': 'Project',
        'pa_expenditures_t': 'Project',
        'pa_resource_assignments_t': 'Project',
        
        # Inventory / Product
        'mtl_system_items_t': 'Product',
        'mtl_item_locations_t': 'Product',
        'phc_prod_master': 'Product',
        'phc_prod_lifecycle_history': 'Product',
        'phc_prod_alt_names': 'Product',
        
        # Master Data
        'pmd_parties_t': 'MasterData',
        'pmd_accounts_t': 'MasterData',
        'pmd_acct_sites_t': 'MasterData',
        'pmd_locations_t': 'MasterData',
        'pmd_person_profiles_t': 'MasterData',
        'phc_lookup_types': 'MasterData',
        'phc_lookup_values_t': 'MasterData',
        'phc_material_group_master': 'MasterData',
        'phc_material_master': 'MasterData',
        'phc_uom_master': 'MasterData',
        'phc_uom_conversion': 'MasterData',
        'phc_plant_master': 'MasterData',
        'phc_plant_compliance': 'MasterData',
        'phc_certifications': 'MasterData',
        'phc_plant_equipment': 'MasterData',
        'phc_equipment_locations': 'MasterData',
        
        # Employees & Org
        'phc_emp_t': 'Employee',
        'phc_dept_t': 'Employee',
        'phc_orgs_t': 'Employee',
        'phc_cost_center_t': 'Employee',
        
        # Cleaning Validation
        'cv_product_registration_t': 'Cleaning',
        'cv_equipment_registration_t': 'Cleaning',
        'cv_product_apis_t': 'Cleaning',
        'cv_product_equipment_map_t': 'Cleaning',
        
        # App Setup
        'phc_users_t': 'AppSetup',
        'phc_roles_t': 'AppSetup',
        'phc_screens_t': 'AppSetup',
        'phc_companies_t': 'AppSetup',
        'phc_apps_t': 'AppSetup',
        'phc_role_screen_assignment_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup',
        'phc_approval_types_t': 'AppSetup',
        'phc_approval_setup_t': 'AppSetup',
        'phc_notifications_setup_t': 'AppSetup',
        'phc_approval_events_t': 'AppSetup',
    }

@app.before_server_start
async def setup_db(app, loop):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("="*50)
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print("❌ ERROR: DATABASE_URL is MISSING or EMPTY!")
        print("❌ Render is completely blind to your database variable.")
        print("="*50)
    else:
        print("="*50)
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print(f"✅ DATABASE_URL Found: {db_url[:30]}...")
        print("✅ Environment Variables are loaded correctly.")
        print("="*50)
        
    app.ctx.db = await asyncpg.create_pool(dsn=db_url, min_size=2, max_size=20)

@app.after_server_stop
async def close_db(app, loop):
    await app.ctx.db.close()

@app.middleware("request")
async def setup_session(request):
    """Initializes the session dictionary and reads the JWT cookie"""
    request.ctx.session = {}
    token = request.cookies.get("session_token")
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            request.ctx.session["user_id"] = payload.get("user_id")
            request.ctx.session["username"] = payload.get("username")
            request.ctx.session["role"] = payload.get("role")
        except jwt.ExpiredSignatureError:
            pass
        except jwt.InvalidTokenError:
            pass

@app.middleware("response")
async def add_security_headers(request, response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"

def check_auth(wrapped):
    """Security decorator to protect routes (includes wraps to prevent duplicate route errors)"""
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        session = getattr(request.ctx, 'session', {})
        if not session.get("user_id"):
            if request.method == 'GET':
                return response.redirect('/login')
            return response.json({"error": "Unauthorized"}, status=401)
        return await wrapped(request, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        template = env.get_template('login.html')
        return response.html(template.render())
    
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.db.acquire() as conn:
        try:
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
                        "exp": datetime.utcnow() + timedelta(hours=12)
                    }, SECRET_KEY, algorithm="HS256")
                    
                    res = response.json({"status": "success", "message": "Login successful"})
                    res.cookies["session_token"] = token
                    res.cookies["session_token"]["httponly"] = True
                    return res
        except Exception as e:
            print(f"Login error: {e}")
            
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    res = response.redirect('/login')
    del res.cookies["session_token"]
    return res

@app.route('/')
@check_auth
async def dashboard(request):
    template = env.get_template('dashboard.html')
    session = getattr(request.ctx, 'session', {})
    username = session.get('username', 'User')
    user_id = session.get('user_id')
    
    async with app.ctx.db.acquire() as conn:
        try:
            emp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t")
        except Exception:
            emp_count = 0
        try:
            dept_count = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t")
        except Exception:
            dept_count = 0
        try:
            comp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t")
        except Exception:
            comp_count = 0
            
        stats = {
            "emp_count": emp_count,
            "dept_count": dept_count,
            "comp_count": comp_count,
            "app_count": 12
        }
        
        all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
        
    return response.html(template.render(
        username=username,
        user_id=user_id,
        stats=stats,
        all_tables=all_tables,
        table_modules=get_table_modules()
    ))

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    template = env.get_template('table_view.html')
    page = int(request.args.get('page', 1))
    search_query = request.args.get('q', '').strip()
    per_page = 50
    offset = (page - 1) * per_page
    
    async with app.ctx.db.acquire() as conn:
        try:
            cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
            col_names = [c['column_name'] for c in cols]
            
            pk_record = await conn.fetchrow("""
                SELECT kcu.column_name 
                FROM information_schema.table_constraints tco
                JOIN information_schema.key_column_usage kcu 
                  ON kcu.constraint_name = tco.constraint_name 
                 AND kcu.constraint_schema = tco.constraint_schema
                WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
            """, table_name)
            pk_column = pk_record['column_name'] if pk_record else col_names[0]
            
            where_clause = ""
            params = []
            if search_query:
                search_exprs = []
                for i, c in enumerate(col_names):
                    search_exprs.append(f"CAST({c} AS TEXT) ILIKE ${i+1}")
                    params.append(f"%{search_query}%")
                where_clause = "WHERE " + " OR ".join(search_exprs)
                
            count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
            total_count = await conn.fetchval(count_query, *params)
            total_pages = math.ceil(total_count / per_page) if total_count > 0 else 1
            
            data_query = f"SELECT * FROM {table_name} {where_clause} ORDER BY {pk_column} DESC LIMIT {per_page} OFFSET {offset}"
            rows = await conn.fetch(data_query, *params)
            
            columns = [{"raw": c, "label": c.replace('_', ' ').title()} for c in col_names if c != 'company_id']
            
            all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
            
            session = getattr(request.ctx, 'session', {})
            
            return response.html(template.render(
                table_name=table_name,
                table_title=table_name.replace('_t', '').replace('_', ' ').title(),
                columns=columns,
                rows=rows,
                pk_column=pk_column,
                page=page,
                total_pages=total_pages,
                total_count=total_count,
                start_row=offset + 1 if total_count > 0 else 0,
                end_row=min(offset + per_page, total_count),
                search_query=search_query,
                all_tables=all_tables,
                table_modules=get_table_modules(),
                username=session.get('username'),
                user_id=session.get('user_id')
            ))
        except Exception as e:
            return response.html(f"<h3>Error loading table: {str(e)}</h3>")

@app.route('/new/<table_name>')
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>')
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

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
        
        row = None
        if is_update:
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
            
        columns_data = []
        for c in cols:
            cname = c['column_name']
            is_pk = (cname == pk_column)
            val = row[cname] if row else request.args.get(cname, '')
            
            options = []
            if cname == 'status' or cname.endswith('_status'):
                options = [{'id': 'ACT', 'name': 'Active'}, {'id': 'INA', 'name': 'Inactive'}]
                 
            columns_data.append({
                "column_name": cname,
                "label": cname.replace('_', ' ').title(),
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO',
                "is_pk": is_pk,
                "value": val,
                "options": options
            })
            
        all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
        
        session = getattr(request.ctx, 'session', {})
        
        return response.html(template.render(
            table_name=table_name,
            table_title=table_name.replace('_t', '').replace('_', ' ').title(),
            columns=columns_data,
            pk_column=pk_column,
            pk_val=pk_val,
            is_update=is_update,
            all_tables=all_tables,
            table_modules=get_table_modules(),
            username=session.get('username'),
            user_id=session.get('user_id')
        ))

def _type_cast(val, cols, col_name):
    dt = next((c['data_type'] for c in cols if c['column_name'] == col_name), 'text')
    if dt in ('integer', 'bigint', 'smallint'):
        try: return int(val)
        except: return val
    return val

@app.route('/api/<table_name>', methods=['POST'], name='post_save_data')
@app.route('/api/<table_name>/<pk_val>', methods=['PUT'], name='put_save_data')
@check_auth
async def save_data(request, table_name, pk_val=None):
    data = request.json
    is_update = request.method == 'PUT'
    
    async with app.ctx.db.acquire() as conn:
        pk_record = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name 
            WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        """, table_name)
        pk_column = pk_record['column_name']
        
        cols = await conn.fetch("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {c['column_name']: c for c in cols}
        
        clean_data = {}
        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column and not is_update: continue 
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            if k in ('creation_date', 'last_update_date', 'created_by', 'last_updated_by'): continue

            if k == 'pus_pwd':
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')

            col_info = schema_map.get(k, {})
            target_type = col_info.get('data_type', '').lower()
            max_len = col_info.get('character_maximum_length')
            
            if 'date' in target_type or 'timestamp' in target_type or (isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-'):
                if isinstance(v, str) and v:
                    try: v = datetime.strptime(v, '%Y-%m-%d')
                    except ValueError: pass 

            if isinstance(v, str) and max_len is not None:
                if len(v) > max_len:
                    if "status" in k and v.lower() == "active": v = "ACT"
                    elif "status" in k and v.lower() == "inactive": v = "INA"
                    else: v = v[:max_len]

            if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v
            
        if not is_update:
            # AUTO ID GENERATOR LOGIC
            if pk_column not in clean_data:
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                if pk_type in ('integer', 'bigint', 'smallint'):
                    max_id = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                    clean_data[pk_column] = (max_id or 0) + 1
                else:
                    clean_data[pk_column] = f"REC-{str(uuid.uuid4())[:8].upper()}"

            keys = list(clean_data.keys())
            values = list(clean_data.values())
            placeholders = ", ".join([f"${i+1}" for i in range(len(values))])
            query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({placeholders})"
            await conn.execute(query, *values)
        else:
            keys = list(clean_data.keys())
            values = list(clean_data.values())
            set_clause = ", ".join([f"{k} = ${i+1}" for i, k in enumerate(keys)])
            values.append(_type_cast(pk_val, cols, pk_column))
            query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ${len(values)}"
            await conn.execute(query, *values)
            
        return response.json({"status": "success", "msg": "Record saved successfully!"})

@app.route('/api/<table_name>/<pk_val>', methods=['DELETE'])
@check_auth
async def delete_data(request, table_name, pk_val):
    async with app.ctx.db.acquire() as conn:
        pk_record = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name 
            WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        """, table_name)
        pk_column = pk_record['column_name']
        
        cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
        col_names = [c['column_name'] for c in cols]
        
        if 'status' in col_names:
            await conn.execute(f"UPDATE {table_name} SET status = 'INA' WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
        elif 'psn_status' in col_names:
            await conn.execute(f"UPDATE {table_name} SET psn_status = 'INA' WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
        else:
            await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
        
        return response.json({"status": "success", "msg": "Record deleted successfully!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    is_development = os.environ.get("ENVIRONMENT") != "production"
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)import os
import math
import uuid
import bcrypt
import jwt
from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv

from sanic import Sanic, response
from jinja2 import Environment, FileSystemLoader
import asyncpg

# Load Environment Variables FIRST before anything else
load_dotenv()

app = Sanic("ERP_System")
env = Environment(loader=FileSystemLoader("templates"))
SECRET_KEY = os.environ.get("SECRET_KEY", "super-secret-erp-key-2026")

# Global Cache to speed up database queries
SCHEMA_CACHE = {}

def get_table_modules():
    """Static dictionary to map tables to sidebar folders without database queries"""
    return {
        # General Ledger
        'pgl_batches_t': 'Ledger',
        'pgl_headers_t': 'Ledger',
        'pgl_lines_t': 'Ledger',
        'pgl_sources_t': 'Ledger',
        'pgl_daily_rates_t': 'Ledger',
        'pgl_balances_t': 'Ledger',
        'pgl_acc_periods_t': 'Ledger',
        'pgl_period_sets_t': 'Ledger',
        'pgl_code_combinations_t': 'Ledger',
        
        # Receivables
        'pra_customer_trx_t': 'Receivables',
        'pra_customer_trx_lines_t': 'Receivables',
        'pra_cust_trx_line_dist_t': 'Receivables',
        'pra_cust_trx_types_t': 'Receivables',
        'par_payment_schedules_t': 'Receivables',
        'par_batch_sources_t': 'Receivables',
        'par_vat_tax_t': 'Receivables',
        'par_terms_t': 'Receivables',
        'par_periods_t': 'Receivables',
        'par_period_types_t': 'Receivables',
        
        # Payables
        'ap_invoices_t': 'Payables',
        'ap_invoice_distributions_t': 'Payables',
        'ap_payments_schedules_t': 'Payables',
        
        # Procurement
        'po_requisition_headers_t': 'Procurement',
        'po_requisition_lines_t': 'Procurement',
        'po_req_distributions_t': 'Procurement',
        'po_headers_t': 'Procurement',
        'po_lines_t': 'Procurement',
        'po_distributions_t': 'Procurement',
        
        # Order Management
        'poe_order_headers_t': 'OrderMgmt',
        'poe_order_lines_t': 'OrderMgmt',
        'poe_order_sources_t': 'OrderMgmt',
        'poe_transaction_types_t': 'OrderMgmt',
        
        # Projects
        'pa_projects_t': 'Project',
        'pa_tasks_t': 'Project',
        'pa_expenditure_items_t': 'Project',
        'pa_expenditures_t': 'Project',
        'pa_resource_assignments_t': 'Project',
        
        # Inventory / Product
        'mtl_system_items_t': 'Product',
        'mtl_item_locations_t': 'Product',
        'phc_prod_master': 'Product',
        'phc_prod_lifecycle_history': 'Product',
        'phc_prod_alt_names': 'Product',
        
        # Master Data
        'pmd_parties_t': 'MasterData',
        'pmd_accounts_t': 'MasterData',
        'pmd_acct_sites_t': 'MasterData',
        'pmd_locations_t': 'MasterData',
        'pmd_person_profiles_t': 'MasterData',
        'phc_lookup_types': 'MasterData',
        'phc_lookup_values_t': 'MasterData',
        'phc_material_group_master': 'MasterData',
        'phc_material_master': 'MasterData',
        'phc_uom_master': 'MasterData',
        'phc_uom_conversion': 'MasterData',
        'phc_plant_master': 'MasterData',
        'phc_plant_compliance': 'MasterData',
        'phc_certifications': 'MasterData',
        'phc_plant_equipment': 'MasterData',
        'phc_equipment_locations': 'MasterData',
        
        # Employees & Org
        'phc_emp_t': 'Employee',
        'phc_dept_t': 'Employee',
        'phc_orgs_t': 'Employee',
        'phc_cost_center_t': 'Employee',
        
        # Cleaning Validation
        'cv_product_registration_t': 'Cleaning',
        'cv_equipment_registration_t': 'Cleaning',
        'cv_product_apis_t': 'Cleaning',
        'cv_product_equipment_map_t': 'Cleaning',
        
        # App Setup
        'phc_users_t': 'AppSetup',
        'phc_roles_t': 'AppSetup',
        'phc_screens_t': 'AppSetup',
        'phc_companies_t': 'AppSetup',
        'phc_apps_t': 'AppSetup',
        'phc_role_screen_assignment_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup',
        'phc_approval_types_t': 'AppSetup',
        'phc_approval_setup_t': 'AppSetup',
        'phc_notifications_setup_t': 'AppSetup',
        'phc_approval_events_t': 'AppSetup',
    }

@app.before_server_start
async def setup_db(app, loop):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("="*50)
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print("❌ ERROR: DATABASE_URL is MISSING or EMPTY!")
        print("❌ Render is completely blind to your database variable.")
        print("="*50)
    else:
        print("="*50)
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print(f"✅ DATABASE_URL Found: {db_url[:30]}...")
        print("✅ Environment Variables are loaded correctly.")
        print("="*50)
        
    app.ctx.db = await asyncpg.create_pool(dsn=db_url, min_size=2, max_size=20)

@app.after_server_stop
async def close_db(app, loop):
    await app.ctx.db.close()

@app.middleware("request")
async def setup_session(request):
    """Initializes the session dictionary and reads the JWT cookie"""
    request.ctx.session = {}
    token = request.cookies.get("session_token")
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            request.ctx.session["user_id"] = payload.get("user_id")
            request.ctx.session["username"] = payload.get("username")
            request.ctx.session["role"] = payload.get("role")
        except jwt.ExpiredSignatureError:
            pass
        except jwt.InvalidTokenError:
            pass

@app.middleware("response")
async def add_security_headers(request, response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"

def check_auth(wrapped):
    """Security decorator to protect routes (includes wraps to prevent duplicate route errors)"""
    @wraps(wrapped)
    async def decorator(request, *args, **kwargs):
        session = getattr(request.ctx, 'session', {})
        if not session.get("user_id"):
            if request.method == 'GET':
                return response.redirect('/login')
            return response.json({"error": "Unauthorized"}, status=401)
        return await wrapped(request, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        template = env.get_template('login.html')
        return response.html(template.render())
    
    data = request.json
    username = data.get("username", "")
    password = data.get("password", "")
    
    async with app.ctx.db.acquire() as conn:
        try:
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
                        "exp": datetime.utcnow() + timedelta(hours=12)
                    }, SECRET_KEY, algorithm="HS256")
                    
                    res = response.json({"status": "success", "message": "Login successful"})
                    res.cookies["session_token"] = token
                    res.cookies["session_token"]["httponly"] = True
                    return res
        except Exception as e:
            print(f"Login error: {e}")
            
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    res = response.redirect('/login')
    del res.cookies["session_token"]
    return res

@app.route('/')
@check_auth
async def dashboard(request):
    template = env.get_template('dashboard.html')
    session = getattr(request.ctx, 'session', {})
    username = session.get('username', 'User')
    user_id = session.get('user_id')
    
    async with app.ctx.db.acquire() as conn:
        try:
            emp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t")
        except Exception:
            emp_count = 0
        try:
            dept_count = await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t")
        except Exception:
            dept_count = 0
        try:
            comp_count = await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t")
        except Exception:
            comp_count = 0
            
        stats = {
            "emp_count": emp_count,
            "dept_count": dept_count,
            "comp_count": comp_count,
            "app_count": 12
        }
        
        all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
        
    return response.html(template.render(
        username=username,
        user_id=user_id,
        stats=stats,
        all_tables=all_tables,
        table_modules=get_table_modules()
    ))

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    template = env.get_template('table_view.html')
    page = int(request.args.get('page', 1))
    search_query = request.args.get('q', '').strip()
    per_page = 50
    offset = (page - 1) * per_page
    
    async with app.ctx.db.acquire() as conn:
        try:
            cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
            col_names = [c['column_name'] for c in cols]
            
            pk_record = await conn.fetchrow("""
                SELECT kcu.column_name 
                FROM information_schema.table_constraints tco
                JOIN information_schema.key_column_usage kcu 
                  ON kcu.constraint_name = tco.constraint_name 
                 AND kcu.constraint_schema = tco.constraint_schema
                WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
            """, table_name)
            pk_column = pk_record['column_name'] if pk_record else col_names[0]
            
            where_clause = ""
            params = []
            if search_query:
                search_exprs = []
                for i, c in enumerate(col_names):
                    search_exprs.append(f"CAST({c} AS TEXT) ILIKE ${i+1}")
                    params.append(f"%{search_query}%")
                where_clause = "WHERE " + " OR ".join(search_exprs)
                
            count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
            total_count = await conn.fetchval(count_query, *params)
            total_pages = math.ceil(total_count / per_page) if total_count > 0 else 1
            
            data_query = f"SELECT * FROM {table_name} {where_clause} ORDER BY {pk_column} DESC LIMIT {per_page} OFFSET {offset}"
            rows = await conn.fetch(data_query, *params)
            
            columns = [{"raw": c, "label": c.replace('_', ' ').title()} for c in col_names if c != 'company_id']
            
            all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
            
            session = getattr(request.ctx, 'session', {})
            
            return response.html(template.render(
                table_name=table_name,
                table_title=table_name.replace('_t', '').replace('_', ' ').title(),
                columns=columns,
                rows=rows,
                pk_column=pk_column,
                page=page,
                total_pages=total_pages,
                total_count=total_count,
                start_row=offset + 1 if total_count > 0 else 0,
                end_row=min(offset + per_page, total_count),
                search_query=search_query,
                all_tables=all_tables,
                table_modules=get_table_modules(),
                username=session.get('username'),
                user_id=session.get('user_id')
            ))
        except Exception as e:
            return response.html(f"<h3>Error loading table: {str(e)}</h3>")

@app.route('/new/<table_name>')
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>')
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

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
        
        row = None
        if is_update:
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
            
        columns_data = []
        for c in cols:
            cname = c['column_name']
            is_pk = (cname == pk_column)
            val = row[cname] if row else request.args.get(cname, '')
            
            options = []
            if cname == 'status' or cname.endswith('_status'):
                options = [{'id': 'ACT', 'name': 'Active'}, {'id': 'INA', 'name': 'Inactive'}]
                 
            columns_data.append({
                "column_name": cname,
                "label": cname.replace('_', ' ').title(),
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO',
                "is_pk": is_pk,
                "value": val,
                "options": options
            })
            
        all_tables_records = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        all_tables = [r['tablename'] for r in all_tables_records if not r['tablename'].startswith('pg_')]
        
        session = getattr(request.ctx, 'session', {})
        
        return response.html(template.render(
            table_name=table_name,
            table_title=table_name.replace('_t', '').replace('_', ' ').title(),
            columns=columns_data,
            pk_column=pk_column,
            pk_val=pk_val,
            is_update=is_update,
            all_tables=all_tables,
            table_modules=get_table_modules(),
            username=session.get('username'),
            user_id=session.get('user_id')
        ))

def _type_cast(val, cols, col_name):
    dt = next((c['data_type'] for c in cols if c['column_name'] == col_name), 'text')
    if dt in ('integer', 'bigint', 'smallint'):
        try: return int(val)
        except: return val
    return val

@app.route('/api/<table_name>', methods=['POST'], name='post_save_data')
@app.route('/api/<table_name>/<pk_val>', methods=['PUT'], name='put_save_data')
@check_auth
async def save_data(request, table_name, pk_val=None):
    data = request.json
    is_update = request.method == 'PUT'
    
    async with app.ctx.db.acquire() as conn:
        pk_record = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name 
            WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        """, table_name)
        pk_column = pk_record['column_name']
        
        cols = await conn.fetch("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = $1", table_name)
        schema_map = {c['column_name']: c for c in cols}
        
        clean_data = {}
        for k, v in data.items():
            if v == "" or v is None: continue 
            if k == pk_column and not is_update: continue 
            if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue
            if k in ('creation_date', 'last_update_date', 'created_by', 'last_updated_by'): continue

            if k == 'pus_pwd':
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')

            col_info = schema_map.get(k, {})
            target_type = col_info.get('data_type', '').lower()
            max_len = col_info.get('character_maximum_length')
            
            if 'date' in target_type or 'timestamp' in target_type or (isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-'):
                if isinstance(v, str) and v:
                    try: v = datetime.strptime(v, '%Y-%m-%d')
                    except ValueError: pass 

            if isinstance(v, str) and max_len is not None:
                if len(v) > max_len:
                    if "status" in k and v.lower() == "active": v = "ACT"
                    elif "status" in k and v.lower() == "inactive": v = "INA"
                    else: v = v[:max_len]

            if target_type in ('integer', 'bigint', 'numeric', 'smallint') and isinstance(v, str):
                if v.strip().isdigit(): clean_data[k] = int(v)
            else: clean_data[k] = v
            
        if not is_update:
            # AUTO ID GENERATOR LOGIC
            if pk_column not in clean_data:
                pk_type = schema_map.get(pk_column, {}).get('data_type', '')
                if pk_type in ('integer', 'bigint', 'smallint'):
                    max_id = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                    clean_data[pk_column] = (max_id or 0) + 1
                else:
                    clean_data[pk_column] = f"REC-{str(uuid.uuid4())[:8].upper()}"

            keys = list(clean_data.keys())
            values = list(clean_data.values())
            placeholders = ", ".join([f"${i+1}" for i in range(len(values))])
            query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({placeholders})"
            await conn.execute(query, *values)
        else:
            keys = list(clean_data.keys())
            values = list(clean_data.values())
            set_clause = ", ".join([f"{k} = ${i+1}" for i, k in enumerate(keys)])
            values.append(_type_cast(pk_val, cols, pk_column))
            query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ${len(values)}"
            await conn.execute(query, *values)
            
        return response.json({"status": "success", "msg": "Record saved successfully!"})

@app.route('/api/<table_name>/<pk_val>', methods=['DELETE'])
@check_auth
async def delete_data(request, table_name, pk_val):
    async with app.ctx.db.acquire() as conn:
        pk_record = await conn.fetchrow("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name 
            WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = $1
        """, table_name)
        pk_column = pk_record['column_name']
        
        cols = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", table_name)
        col_names = [c['column_name'] for c in cols]
        
        if 'status' in col_names:
            await conn.execute(f"UPDATE {table_name} SET status = 'INA' WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
        elif 'psn_status' in col_names:
            await conn.execute(f"UPDATE {table_name} SET psn_status = 'INA' WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
        else:
            await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", _type_cast(pk_val, cols, pk_column))
        
        return response.json({"status": "success", "msg": "Record deleted successfully!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    is_development = os.environ.get("ENVIRONMENT") != "production"
    app.run(host="0.0.0.0", port=port, debug=is_development, single_process=True)
