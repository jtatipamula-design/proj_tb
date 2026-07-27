import os
import json
import uuid
import time
from datetime import datetime, date
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
# Stores {user_id: {"session_id": str, "role": str, "expires": float}}
USER_AUTH_CACHE = {}
CACHE_TTL = 30  # Number of seconds before it pings the DB again

SCHEMA_CACHE = {"pks": {}}

# ==========================================
# MODULE ROUTING LOGIC
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
        # HR & People
        'phc_emp_t': 'HR',
        'phc_users_t': 'People',
        # User Mgmt & App Admin
        'phc_screens_t': 'UserMgmt',
        'phc_roles_t': 'UserMgmt',
        'phc_role_screen_assignment_t': 'UserMgmt',
        'phc_user_roles_assignment_t': 'UserMgmt',
        'phc_user_group_t': 'UserMgmt',
        'phc_user_log_t': 'UserMgmt',
        'phc_error_log_t': 'AppAdmin',
        # Purchasing & Supply Chain
        'phc_plant_equipment_t': 'Purchasing',
        'phc_material_master_t': 'Purchasing',
        'phc_material_group_master_t': 'Purchasing',
        'phc_vendors_t': 'Purchasing',
        'phc_vend_sites_t': 'Purchasing',
        'phc_vend_contact_points_t': 'Purchasing',
        'phc_vend_site_locations_t': 'Purchasing',
        'phc_equipment_locations_t': 'SupplyChain',
        # Workflow
        'phc_approval_types_t': 'WorkflowSetup',
        'phc_approval_setup_t': 'WorkflowSetup',
        'phc_notifications_setup_t': 'WorkflowSetup',
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
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL is missing!")
        return
    try:
        app.ctx.pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=2,
            max_size=20,
            command_timeout=60
        )
        print("✅ Database Connected")
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'pool'):
        await app.ctx.pool.close()

# ==========================================
# SECURITY ENGINES
# ==========================================
@app.middleware("response")
async def add_security_headers(request, response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"

def check_auth(wrapped):
    async def decorator(request, *args, **kwargs):
        token = request.cookies.get("auth_token")
        if not token:
            return response.redirect("/login")
        
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("user_id")
            session_uuid = payload.get("session_uuid")
            
            now = time.time()
            cached_data = USER_AUTH_CACHE.get(user_id)
            
            # 1. Check Fast RAM Cache
            if cached_data and cached_data["expires"] > now:
                db_session_id = cached_data["session_id"]
                db_role = cached_data["role"]
            else:
                # 2. Ping DB once every 30s
                async with app.ctx.pool.acquire() as conn:
                    user_data = await conn.fetchrow("SELECT pus_session_id, pus_user_type FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if not user_data:
                        res = response.redirect("/login")
                        res.delete_cookie("auth_token")
                        return res
                    
                    db_session_id = str(user_data['pus_session_id'])
                    db_role = user_data.get('pus_user_type', 'STD')
                    
                    USER_AUTH_CACHE[user_id] = {
                        "session_id": db_session_id,
                        "role": db_role,
                        "expires": now + CACHE_TTL
                    }

            # 3. Enforce 1 Active Session
            if db_session_id != str(session_uuid):
                # Another login happened, invalidate this one
                res = response.redirect("/login")
                res.delete_cookie("auth_token")
                return res
            
            payload['role'] = db_role
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

async def get_authorized_tables(user_id, user_role):
    async with app.ctx.pool.acquire() as conn:
        all_screens = await conn.fetch("""
            SELECT psn_screen_code 
            FROM phc_screens_t 
            WHERE psn_status = 'ACT' 
            AND lower(psn_screen_code) LIKE 'phc_%'
        """)
        valid_tables = [s['psn_screen_code'].lower() for s in all_screens]

        if user_role == 'ADM':
            return valid_tables
        
        allowed = await conn.fetch("""
            SELECT s.psn_screen_code
            FROM phc_user_roles_assignment_t ur
            JOIN phc_role_screen_assignment_t rs ON ur.pua_role_id = rs.prs_role_id
            JOIN phc_screens_t s ON rs.prs_screen_id = s.psn_screen_id
            WHERE ur.pua_user_id = $1 
              AND ur.pua_status = 'ACT' 
              AND rs.prs_status = 'ACT' 
              AND s.psn_status = 'ACT'
              AND lower(s.psn_screen_code) LIKE 'phc_%'
        """, user_id)
        
        return list(set(row['psn_screen_code'].lower() for row in allowed))

# ==========================================
# AUTHENTICATION ROUTES
# ==========================================
@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        # Smart Login Page: Cleans up old dead cookies before rendering
        token = request.cookies.get("auth_token")
        if token:
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                user_id = payload.get("user_id")
                session_uuid = payload.get("session_uuid")
                
                async with app.ctx.pool.acquire() as conn:
                    db_session = await conn.fetchval("SELECT pus_session_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if str(db_session) == str(session_uuid):
                        return response.redirect("/")
            except:
                pass
                
        template = env.get_template('login.html')
        res = response.html(template.render())
        res.delete_cookie("auth_token")
        return res

    # POST (Process Login)
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
                # Catch plaintext fallback
                if password == stored_pwd:
                    is_valid = True
                    
            if is_valid:
                # Generate new UUID for single-session enforcement
                new_session = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", new_session, user['pus_user_id'])
                
                token = jwt.encode({
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "session_uuid": new_session
                }, JWT_SECRET, algorithm="HS256")
                
                # Clear RAM cache to force immediate DB sync
                if user['pus_user_id'] in USER_AUTH_CACHE:
                    del USER_AUTH_CACHE[user['pus_user_id']]
                
                res = response.json({"status": "success", "message": "Login successful"})
                # NO max_age parameter -> This creates a Session Cookie that dies when the browser closes.
                res.add_cookie("auth_token", token, httponly=True, samesite="Strict")
                return res
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout')
async def logout(request):
    token = request.cookies.get("auth_token")
    if token:
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("user_id")
            # Kill session in DB
            async with app.ctx.pool.acquire() as conn:
                await conn.execute("UPDATE phc_users_t SET pus_session_id = NULL WHERE pus_user_id = $1", user_id)
            if user_id in USER_AUTH_CACHE:
                del USER_AUTH_CACHE[user_id]
        except:
            pass
            
    res = response.redirect('/login')
    res.delete_cookie("auth_token")
    return res

# ==========================================
# HELPERS
# ==========================================
async def get_pk_column(conn, table_name):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass AND i.indisprimary;
    """
    try:
        pk = await conn.fetchval(query, table_name)
        SCHEMA_CACHE["pks"][table_name] = pk
        return pk
    except Exception:
        return None

def sort_columns(columns, pk_col):
    def get_weight(col):
        name = col['column_name'].lower()
        # 1. Primary Key
        if pk_col and name == pk_col.lower(): return 0
        # 2. Status
        if 'status' in name: return 1
        # 5. Audit Columns (Bottom)
        if name.endswith(('_created', '_modified', '_created_by', '_modified_by')) or name in ('creation_date', 'last_update_date', 'created_by', 'last_updated_by'): return 4
        # 4. Dates (Above Audit)
        if 'date' in name or 'timestamp' in col.get('data_type', '').lower(): return 3
        # 3. Everything Else (Middle)
        return 2
    
    return sorted(columns, key=get_weight)

async def resolve_foreign_key_display(conn, table_name, column_name, raw_val):
    if not raw_val:
        return str(raw_val) if raw_val is not None else ""
        
    col_lower = column_name.lower()
    
    try:
        if col_lower == 'company_id' or col_lower.endswith('_company_id'):
            res = await conn.fetchval("SELECT pcp_company_name FROM phc_companies_t WHERE pcp_company_id = $1", int(raw_val))
            return res if res else str(raw_val)
        if col_lower.endswith('_role_id'):
            res = await conn.fetchval("SELECT prl_role_name FROM phc_roles_t WHERE prl_role_id = $1", int(raw_val))
            return res if res else str(raw_val)
        if col_lower.endswith('_screen_id'):
            res = await conn.fetchval("SELECT psn_screen_name FROM phc_screens_t WHERE psn_screen_id = $1", int(raw_val))
            return res if res else str(raw_val)
        if col_lower.endswith('_user_id') or col_lower.endswith('_by'):
            res = await conn.fetchval("SELECT pus_user_name FROM phc_users_t WHERE pus_user_id = $1 OR pus_user_name = $2", int(raw_val) if str(raw_val).isdigit() else 0, str(raw_val))
            return res if res else str(raw_val)
        if col_lower.endswith('_dept_id'):
            res = await conn.fetchval("SELECT pdp_dept_name FROM phc_dept_t WHERE pdp_dept_id = $1", int(raw_val))
            return res if res else str(raw_val)
    except Exception:
        pass
        
    return str(raw_val)

# ==========================================
# MAIN ROUTES
# ==========================================
@app.route('/')
@check_auth
async def dashboard(request):
    user_id = request.ctx.session['user_id']
    username = request.ctx.session['username']
    user_role = request.ctx.session.get('role', 'STD')
    
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(user_id, user_role)
        table_modules = get_table_modules()
        
        stats = {
            'emp_count': await conn.fetchval("SELECT COUNT(*) FROM phc_emp_t") if 'phc_emp_t' in auth_tables else 0,
            'comp_count': await conn.fetchval("SELECT COUNT(*) FROM phc_companies_t") if 'phc_companies_t' in auth_tables else 0,
            'dept_count': await conn.fetchval("SELECT COUNT(*) FROM phc_dept_t") if 'phc_dept_t' in auth_tables else 0,
            'app_count': await conn.fetchval("SELECT COUNT(*) FROM phc_apps_t") if 'phc_apps_t' in auth_tables else 0,
        }
        
    template = env.get_template('dashboard.html')
    return response.html(template.render(
        all_tables=auth_tables, 
        table_modules=table_modules, 
        stats=stats,
        username=username,
        user_id=user_id,
        user_role=user_role
    ))

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    user_id = request.ctx.session['user_id']
    user_role = request.ctx.session.get('role', 'STD')
    table_name_lower = table_name.lower()
    
    async with app.ctx.pool.acquire() as conn:
        auth_tables = await get_authorized_tables(user_id, user_role)
        if table_name_lower not in auth_tables:
            return response.html("<h3>403 Forbidden: You do not have access to this module.</h3>", status=403)

        pk_column = await get_pk_column(conn, table_name_lower)
        if not pk_column:
            return response.html(f"<h3>Error: Could not determine primary key for {table_name}</h3>")

        search_query = request.args.get('q', '').strip()
        page = int(request.args.get('page', 1))
        per_page = 50
        offset = (page - 1) * per_page

        col_data = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = $1
        """, table_name_lower)
        
        all_columns = [{'column_name': r['column_name'], 'data_type': r['data_type']} for r in col_data]
        sorted_cols = sort_columns(all_columns, pk_column)

        where_clause = ""
        params = []
        
        if search_query:
            conditions = []
            for i, c in enumerate(all_columns):
                conditions.append(f"{c['column_name']}::text ILIKE ${i+1}")
                params.append(f"%{search_query}%")
            where_clause = "WHERE " + " OR ".join(conditions)
        
        count_query = f"SELECT COUNT(*) FROM {table_name_lower} {where_clause}"
        total_count = await conn.fetchval(count_query, *params)
        
        data_query = f"SELECT * FROM {table_name_lower} {where_clause} ORDER BY {pk_column} DESC LIMIT {per_page} OFFSET {offset}"
        raw_rows = await conn.fetch(data_query, *params)
        
        # Process rows for display (Magic Resolver)
        display_rows = []
        for r in raw_rows:
            row_dict = dict(r)
            for c in sorted_cols:
                col_name = c['column_name']
                raw_val = row_dict.get(col_name)
                
                # Format Dates
                if isinstance(raw_val, (date, datetime)):
                    row_dict[col_name] = raw_val.strftime('%Y-%m-%d')
                # Resolve Foreign Keys (Except the primary key itself!)
                elif col_name.lower() != pk_column.lower() and col_name.endswith('_id'):
                    row_dict[col_name] = await resolve_foreign_key_display(conn, table_name_lower, col_name, raw_val)
                    
            display_rows.append(row_dict)

        clean_title = table_name.split('_', 1)[-1].replace('_t', '').replace('_', ' ').title()
        
    is_htmx = request.headers.get("HX-Request") == "true"
    template = env.get_template('table_view.html')
    
    rendered = template.render(
        table_name=table_name_lower, 
        table_title=clean_title, 
        columns=[{'raw': c['column_name'], 'label': c['column_name'].replace('_', ' ').title()} for c in sorted_cols], 
        rows=display_rows,
        pk_column=pk_column,
        all_tables=auth_tables,
        table_modules=get_table_modules(),
        username=request.ctx.session['username'],
        user_role=user_role,
        user_id=user_id,
        search_query=search_query,
        page=page,
        total_pages=(total_count // per_page) + (1 if total_count % per_page > 0 else 0),
        total_count=total_count,
        start_row=offset + 1 if total_count > 0 else 0,
        end_row=min(offset + per_page, total_count)
    )
    
    if is_htmx:
        html_start = rendered.find('<div id="tableContent">')
        html_end = rendered.rfind('</div>') + 6
        if html_start != -1 and html_end != -1:
            return response.html(rendered[html_start:html_end])
            
    return response.html(rendered)

# ==========================================
# STARTUP
# ==========================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, access_log=False)
