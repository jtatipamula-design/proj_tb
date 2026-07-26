import os
import math
import json
import uuid
import logging
import zipfile
import io
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
    """Aggressively removes short prefixes and converts to Title Case"""
    parts = col_name.split('_')
    if len(parts) > 1 and len(parts[0]) <= 4:
        parts = parts[1:]
    
    label = ' '.join(parts).title()
    return label.replace(' Id', ' ID').replace(' Uom', ' UOM').replace(' Nos', ' NOS')

def clean_table_name(t_name):
    """Strips the _t suffix and any short table prefixes"""
    clean = t_name.replace('_t', '')
    parts = clean.split('_')
    if len(parts) > 1 and len(parts[0]) <= 4:
        parts = parts[1:]
    return ' '.join(parts).title()

def sort_columns(col_names, pk_column):
    """Strict Logical Order: PK -> Status -> Normal Data -> Dates -> Audit"""
    audit_keywords = ['created', 'modified', 'creation_date', 'last_update_date', 'updated_by', 'created_by']
    
    # 1. Primary Key
    sorted_cols = [pk_column] if pk_column in col_names else []
        
    # 2. Status
    status_cols = [c for c in col_names if 'status' in c.lower() and c != pk_column]
    
    # 3. Start & End Dates (Pulled out to be moved to the bottom)
    start_date_cols = [c for c in col_names if 'start_date' in c.lower() and c != pk_column and c not in status_cols]
    end_date_cols = [c for c in col_names if 'end_date' in c.lower() and c != pk_column and c not in status_cols and c not in start_date_cols]
    
    # 4. Audit Fields
    audit_cols = [c for c in col_names if any(k in c.lower() for k in audit_keywords) and c != pk_column and c not in status_cols]
    
    # Track everything we've pinned so far
    pinned_so_far = sorted_cols + status_cols + start_date_cols + end_date_cols + audit_cols

    # 5. Standard Columns (Names, Emails, Normal Data)
    standard_cols = [c for c in col_names if c not in pinned_so_far]
            
    # Assemble the final list in the requested order (Dates and Audit at the bottom!)
    final_list = sorted_cols + status_cols + standard_cols + start_date_cols + end_date_cols + audit_cols
    return final_list

def get_table_modules(all_tables):
    """Maps purely PHC tables to UI sidebar modules."""
    mapping = {
        # Enterprise Structure
        'phc_companies_t': 'Enterprise', 'phc_cost_center_t': 'Enterprise', 
        'phc_dept_t': 'Enterprise', 'phc_emp_t': 'Enterprise', 
        'phc_operating_orgs_t': 'Enterprise', 'phc_services_t': 'Enterprise',
        'phc_partners_t': 'Enterprise', 'phc_locations_t': 'Enterprise',
        
        # Master Data
        'phc_plant_master_t': 'MasterData', 'phc_plant_compliance_t': 'MasterData', 
        'phc_certifications_t': 'MasterData', 'phc_plant_equipment_t': 'MasterData', 
        'phc_equipment_locations_t': 'MasterData', 'phc_material_group_master_t': 'MasterData', 
        'phc_material_master_t': 'MasterData', 'phc_uom_master_t': 'MasterData', 
        'phc_uom_conversion_t': 'MasterData', 'phc_storage_location_master_t': 'MasterData',
        'phc_lookup_types': 'MasterData', 'phc_lookup_values_t': 'MasterData',
        
        # Product Master
        'phc_prod_master_t': 'Product', 'phc_prod_lifecycle_history_t': 'Product', 'phc_prod_alt_names_t': 'Product',
        
        # Approvals
        'phc_approval_types_t': 'Approvals', 'phc_approval_setup_t': 'Approvals', 
        'phc_notifications_setup_t': 'Approvals', 'phc_approval_events_t': 'Approvals',
        
        # App Setup
        'phc_users_t': 'AppSetup', 'phc_roles_t': 'AppSetup', 'phc_apps_t': 'AppSetup',
        'phc_screens_t': 'AppSetup', 'phc_role_screen_assignment_t': 'AppSetup',
        'phc_user_roles_assignment_t': 'AppSetup', 'phc_user_app_roles_assignment_t': 'AppSetup', 
        'phc_user_group_t': 'AppSetup', 'phc_emp_apps_grant_t': 'AppSetup', 
        'phc_error_log_t': 'AppSetup', 'phc_user_log_t': 'AppSetup'
    }

    # DYNAMIC RULE: Fallbacks in case new PHC tables are created later
    for tbl in all_tables:
        if tbl not in mapping:
            if 'master' in tbl.lower():
                mapping[tbl] = 'MasterData'
            elif 'approval' in tbl.lower() or 'notif' in tbl.lower():
                mapping[tbl] = 'Approvals'
            elif 'prod' in tbl.lower():
                mapping[tbl] = 'Product'
            elif 'emp' in tbl.lower() or 'dept' in tbl.lower():
                mapping[tbl] = 'Enterprise'
            else:
                mapping[tbl] = 'Other'
            
    return mapping

async def get_authorized_tables(conn, user_id, user_role):
    """
    DYNAMIC RBAC: Only fetches tables the logged-in user has explicit rights to see.
    """
    if user_role == 'ADM':
        # Admins see all active registered screens
        records = await conn.fetch("SELECT psn_screen_code as tablename FROM phc_screens_t WHERE psn_status = 'ACT'")
    else:
        # Standard users only see screens explicitly mapped to their active roles
        records = await conn.fetch("""
            SELECT DISTINCT s.psn_screen_code as tablename 
            FROM phc_user_roles_assignment_t ura
            JOIN phc_role_screen_assignment_t rsa ON ura.pua_role_id = rsa.prs_role_id
            JOIN phc_screens_t s ON rsa.prs_screen_id = s.psn_screen_id
            WHERE ura.pua_user_id = $1 
              AND ura.pua_status = 'ACT'
              AND rsa.prs_status = 'ACT'
              AND s.psn_status = 'ACT'
        """, user_id)
        
    return [r['tablename'] for r in records]

async def get_global_fk_map(conn):
    """
    DYNAMIC FOREIGN KEY RESOLVER:
    Fetches common IDs and their human-readable Names so we can swap them out
    in Table Views and render Dropdowns in Form Views.
    """
    fk_map = {}
    queries = {
        'role_id': "SELECT prl_role_id as id, prl_role_name as name FROM phc_roles_t",
        'user_id': "SELECT pus_user_id as id, pus_user_name as name FROM phc_users_t",
        'screen_id': "SELECT psn_screen_id as id, psn_screen_name as name FROM phc_screens_t",
        'dept_id': "SELECT pdp_dept_id as id, pdp_dept_name as name FROM phc_dept_t",
        'company_id': "SELECT pcp_company_id as id, pcp_company_name as name FROM phc_companies_t",
        'menu_id': "SELECT menu_id as id, menu_name as name FROM phc_menu_folders_t",
    }
    
    for f_key, q in queries.items():
        try:
            res = await conn.fetch(q)
            fk_map[f_key] = {r['id']: r['name'] for r in res}
        except Exception:
            pass
            
    return fk_map

@app.before_server_start
async def setup_db(app, loop):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("\n" + "="*50)
        print("🚀 SERVER PRE-FLIGHT CHECK 🚀")
        print("❌ ERROR: DATABASE_URL is MISSING or EMPTY!")
        print("="*50 + "\n")
        return
        
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
            request.ctx.session['session_id'] = payload.get("session_id")
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
        user_id = request.ctx.session.get("user_id")
        user_role = request.ctx.session.get("role")
        session_id = request.ctx.session.get("session_id")
        
        if not user_id or not session_id:
            return response.redirect("/login")
            
        async with app.ctx.db.acquire() as conn:
            # ========================================================
            # 1. STRICT SINGLE-SESSION ENFORCEMENT
            # ========================================================
            current_db_session = await conn.fetchval("SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
            
            # If the DB session ID doesn't match the cookie, they logged in elsewhere!
            if str(current_db_session) != str(session_id):
                res = response.redirect("/login")
                res.delete_cookie("auth_token")
                return res

            # ========================================================
            # 2. BULLETPROOF RBAC ENGINE (Blocks URL Hacks)
            # ========================================================
            table_name = kwargs.get("table_name")
            if table_name and user_role != 'ADM':
                has_access = await conn.fetchval("""
                    SELECT 1 FROM phc_user_roles_assignment_t ura
                    JOIN phc_role_screen_assignment_t rsa ON ura.pua_role_id = rsa.prs_role_id
                    JOIN phc_screens_t s ON rsa.prs_screen_id = s.psn_screen_id
                    WHERE ura.pua_user_id = $1 
                      AND s.psn_screen_code = $2
                      AND ura.pua_status = 'ACT'
                      AND rsa.prs_status = 'ACT'
                      AND s.psn_status = 'ACT'
                """, user_id, table_name)
                
                if not has_access:
                    # They hacked the URL! Block the request.
                    if request.method in ['POST', 'PUT', 'DELETE']:
                        return response.json({"error": "RBAC Violation: You do not have permission to modify this table."}, status=403)
                    else:
                        return response.html(
                            "<div style='padding:50px; background:#101010; height:100vh; color:white; font-family:sans-serif; text-align:center;'>"
                            "<h1 style='color:#ee6018; margin-bottom:10px;'>Access Denied</h1>"
                            f"<p style='color:#b8b3b0;'>Your assigned role does not have authorization to view or edit <b>{table_name}</b>.</p>"
                            "<a href='/' style='color:#80ACFF; text-decoration:none; display:inline-block; margin-top:20px;'>Return to Dashboard</a></div>", 
                            status=403
                        )
        
        return await wrapped(request, *args, **kwargs)
    return decorator

@app.route('/login', methods=['GET'])
async def login_view(request):
    user_id = request.ctx.session.get("user_id")
    session_id = request.ctx.session.get("session_id")
    
    if user_id and session_id:
        async with app.ctx.db.acquire() as conn:
            current_db_session = await conn.fetchval("SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
            # Only redirect to the dashboard if the session is TRULY valid!
            if str(current_db_session) == str(session_id):
                return response.redirect("/")
                
    # If we reach here, they aren't logged in, OR their session was killed.
    template = env.get_template('login.html')
    res = response.html(template.render())
    
    # Forcefully clear any stale/dead cookies
    res.delete_cookie("auth_token")
    return res

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
                # --- SECURE SESSION GENERATION ---
                new_session_id = str(uuid.uuid4())
                
                # Update DB so this device is now the ONLY valid session
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", new_session_id, user['pus_user_id'])
                
                from datetime import timezone, datetime
                token = jwt.encode({
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "role": user.get('pus_user_type', 'STD'),
                    "session_id": new_session_id,
                    "exp": datetime.now(timezone.utc).timestamp() + 86400
                }, os.getenv("SECRET_KEY", "fallback_secret"), algorithm="HS256")
                
                res = response.json({"status": "success", "message": "Login successful"})
                # Fix Sanic Deprecation Warning: Use the modern add_cookie method
                res.cookies.add_cookie("auth_token", token, httponly=True, samesite="Lax")
                return res
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    res = response.redirect("/login")
    res.delete_cookie("auth_token")
    return res

@app.route('/')
@check_auth
async def dashboard(request):
    template = env.get_template('dashboard.html')
    
    async with app.ctx.db.acquire() as conn:
        user_id = request.ctx.session.get('user_id')
        user_role = request.ctx.session.get('role')
        
        # Only fetch tables allowed by RBAC
        all_tables = await get_authorized_tables(conn, user_id, user_role)
        
        # Gracefully handle dashboard stats. If they lack access, it shows 0 without crashing.
        stats = {
            'emp_count': await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t") if 'phc_emp_t' in all_tables else 0,
            'comp_count': await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t") if 'phc_companies_t' in all_tables else 0,
            'dept_count': await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t") if 'phc_dept_t' in all_tables else 0,
            'app_count': await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t") if 'phc_apps_t' in all_tables else 0
        }
        
    return response.html(template.render(
        username=request.ctx.session.get('username'),
        user_id=user_id,
        stats=stats,
        all_tables=all_tables,
        table_modules=get_table_modules(all_tables),
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
            
            # Apply the smart column sorting logic
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
            raw_rows = await conn.fetch(data_query, *params)
            
            # --- MAGIC ID RESOLVER ---
            fk_map = await get_global_fk_map(conn)
            rows = []
            for row in raw_rows:
                row_dict = dict(row)
                for col in col_names:
                    val = row_dict[col]
                    # FIX: Explicitly exclude the pk_column so we don't break the Edit button links!
                    if val is not None and str(col).endswith('_id') and col != pk_column:
                        for f_key, f_dict in fk_map.items():
                            if str(col).endswith(f_key) and val in f_dict:
                                row_dict[col] = f_dict[val]
                                break 
                rows.append(row_dict)
            
            columns = [{"raw": c, "label": clean_label(c)} for c in col_names if c != 'company_id']
            
            lookup_categories = []
            if table_name == 'phc_lookup_values_t':
                try:
                    cats = await conn.fetch("SELECT plt_lookup_type_code as code, plt_lookup_type as name FROM phc_lookup_types")
                    lookup_categories = [dict(c) for c in cats]
                except Exception:
                    pass

            # Fetch Authorized Sidebar Tables
            user_id = request.ctx.session.get('user_id')
            user_role = request.ctx.session.get('role')
            all_tables = await get_authorized_tables(conn, user_id, user_role)
            
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
                table_modules=get_table_modules(all_tables),
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
        
        # Apply strict logical sorting (Dates & Audits at the bottom!)
        sorted_col_names = sort_columns(raw_col_names, pk_column)
        col_info = {c['column_name']: c for c in cols}
        
        row = None
        if is_update:
            try:
                cast_val = int(pk_val) if col_info[pk_column]['data_type'] in ('integer', 'bigint', 'smallint') else pk_val
                row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", cast_val)
            except Exception as e:
                return response.html(f"Error fetching record: {str(e)}")
        
        fk_map = await get_global_fk_map(conn)
            
        columns_data = []
        for cname in sorted_col_names:
            c = col_info[cname]
            is_pk = (cname == pk_column)
            val = row[cname] if row else request.args.get(cname, '')
            
            # PERFECT DATE PRE-FILL LOGIC: Fills ONLY start dates/creation dates with today's date
            if not is_update and not val:
                if 'start_date' in cname.lower() or 'creation_date' in cname.lower():
                    val = datetime.now().strftime('%Y-%m-%d')
            
            options = []
            if cname == 'status' or cname.endswith('_status'):
                options = [{'id': 'ACT', 'name': 'Active'}, {'id': 'INA', 'name': 'Inactive'}]
            elif 'lookup_type_code' in cname.lower():
                # DYNAMIC LOOKUP: Displays Name but saves Code
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
                        roles = await conn.fetch("SELECT prl_role_id, prl_role_name FROM phc_roles_t")
                        json_options = [{'id': str(r['prl_role_id']), 'name': r['prl_role_name']} for r in roles]
                    except Exception: pass
                    
            # DYNAMIC FK DROPDOWNS
            elif cname.endswith('_id') and not is_pk:
                for f_key, f_dict in fk_map.items():
                    if cname.endswith(f_key):
                        options = [{'id': k, 'name': v} for k, v in f_dict.items()]
                        break

            columns_data.append({
                "column_name": cname,
                "label": clean_label(cname), # Applies aggressive prefix stripping
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO',
                "is_pk": is_pk,
                "value": val,
                "options": options,
                "json_options": json_options
            })
            
        user_id = request.ctx.session.get('user_id')
        user_role = request.ctx.session.get('role')
        all_tables = await get_authorized_tables(conn, user_id, user_role)
        
        return response.html(template.render(
            table_name=table_name,
            table_title=clean_table_name(table_name), # Strips the table prefix
            columns=columns_data,
            pk_column=pk_column,
            is_update=is_update,
            pk_val=pk_val,
            all_tables=all_tables,
            table_modules=get_table_modules(all_tables),
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

@app.route('/api/<table_name>', methods=['POST'], name="post_save_data")
@app.route('/api/<table_name>/<pk_val>', methods=['PUT'], name="put_save_data")
@check_auth
async def save_data(request, table_name, pk_val=None):
    is_update = request.method == 'PUT'
    
    # Form data mapping
    data = {}
    if request.form:
        for k in request.form.keys():
            data[k] = request.form.get(k)
    elif request.json:
        data = request.json

    # Secure File Upload & ZIP Engine
    ALLOWED_EXTENSIONS = {'.docx', '.ppt', '.pptx', '.jpg', '.jpeg', '.png', '.txt'}
    if request.files:
        for field_name, files in request.files.items():
            if files:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
                    for file in files:
                        ext = os.path.splitext(file.name)[1].lower()
                        if ext not in ALLOWED_EXTENSIONS:
                            return response.json({"error": f"Security Block: Unsupported file type ({ext})"}, status=400)
                        # Add safe file to zip
                        zip_file.writestr(file.name, file.body)
                
                # Save raw ZIP binary to database (for bytea columns)
                data[field_name] = zip_buffer.getvalue()
    
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
