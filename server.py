import os
import uuid
import time
from datetime import datetime
from functools import wraps
import bcrypt
import jwt
from sanic import Sanic, response
from sanic.exceptions import NotFound
import asyncpg
from jinja2 import Environment, FileSystemLoader

app = Sanic("ERP_System")

DATABASE_URL = os.environ.get("DATABASE_URL")
JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key-change-in-prod")
PORT = int(os.environ.get("PORT", 10000))

env = Environment(loader=FileSystemLoader('templates'))

# --- ENTERPRISE PERFORMANCE CACHE ---
USER_AUTH_CACHE = {}
CACHE_TTL = 30  # Live checking every 30 seconds
SCHEMA_CACHE = {"pks": {}}

def build_modules_tree(all_tables, table_modules):
    modules_tree = {}
    for tbl_code, tbl_name in all_tables.items():
        mod_name = table_modules.get(tbl_code, 'Uncategorized')
        if mod_name not in modules_tree:
            modules_tree[mod_name] = []
        modules_tree[mod_name].append({"code": tbl_code, "name": tbl_name})
    return dict(sorted(modules_tree.items()))

def render_template(template_name, request=None, **context):
    default_context = {
        "username": "",
        "user_id": None,
        "user_role": "STD",
        "modules_tree": {},
        "all_tables": [],
        "table_modules": {}
    }
    if request and hasattr(request, 'ctx'):
        all_tables = getattr(request.ctx, "all_tables", {})
        if isinstance(all_tables, dict):
            all_tables_list = list(all_tables.keys())
        elif isinstance(all_tables, list):
            all_tables_list = all_tables
        else:
            all_tables_list = []

        default_context.update({
            "username": getattr(request.ctx, "username", ""),
            "user_id": getattr(request.ctx, "user_id", None),
            "user_role": getattr(request.ctx, "role", "STD"),
            "modules_tree": getattr(request.ctx, "modules_tree", {}),
            "all_tables": all_tables_list,
            "table_modules": getattr(request.ctx, "table_modules", {})
        })
    default_context.update(context)
    template = env.get_template(template_name)
    html = template.render(**default_context)
    return add_security_headers(response.html(html))

async def get_authorized_tables(conn, user_id, role):
    # ILIKE ensures case-insensitive matching so PHC_OPERATING_ORGS_T works perfectly
    if role == 'ADM':
        # Admin gets everything, joined dynamically with module metadata
        query = """
            SELECT s.psn_screen_code, s.psn_screen_name, COALESCE(m.pmd_module_name, 'System Config') as module_name
            FROM phc_screens_t s
            LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
            WHERE s.psn_screen_code ILIKE 'phc_%'
              -- AND m.pmd_status = 'ACT' -- (Uncomment if pmd_status exists)
        """
        rows = await conn.fetch(query)
    else:
        # Strict RBAC: Join assignment tables, ensuring users only fetch their authorized modules
        # Note: We check if the module is admin-only. If you haven't created this column yet, 
        # you need to run: ALTER TABLE phc_module_t ADD COLUMN pmd_admin_only BOOLEAN DEFAULT FALSE;
        query = """
            SELECT DISTINCT s.psn_screen_code, s.psn_screen_name, COALESCE(m.pmd_module_name, 'Uncategorized') as module_name 
            FROM phc_screens_t s
            JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
            JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
            LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
            WHERE ura.pua_user_id = $1 
              AND s.psn_screen_code ILIKE 'phc_%'
        """
        rows = await conn.fetch(query, user_id)
        
        # In case the column exists in the database, we can filter it out in Python to avoid crashing 
        # if the column is named slightly differently or doesn't exist yet.
        # But wait, if they have an 'erpadmin' module, we can just hardcode hide it for non-admins to be safe:
        valid_rows = []
        for r in rows:
            if r['module_name'].lower() == 'erpadmin':
                continue # Skip admin-only module for non-admins
            valid_rows.append(r)
        rows = valid_rows
        
    # Return two structures: Fast-lookup auth dictionary, and module mappings
    auth_tables = {}
    table_modules = {}
    for r in rows:
        code = r['psn_screen_code'].lower()
        auth_tables[code] = r['psn_screen_name']
        table_modules[code] = r['module_name']
        
    return auth_tables, table_modules

@app.before_server_start
async def setup_db(app, loop):
    if DATABASE_URL:
        app.ctx.pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=20)
    else:
        print("WARNING: DATABASE_URL not set. Running without db pool.")
        app.ctx.pool = None

@app.after_server_stop
async def close_db(app, loop):
    if hasattr(app.ctx, 'pool') and app.ctx.pool:
        await app.ctx.pool.close()

def add_security_headers(res):
    res.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    res.headers["Pragma"] = "no-cache"
    return res

@app.middleware('request')
async def setup_request_context(request):
    if not hasattr(request.ctx, 'user_id'):
        request.ctx.user_id = None
    if not hasattr(request.ctx, 'username'):
        request.ctx.username = ''
    if not hasattr(request.ctx, 'role'):
        request.ctx.role = 'STD'
    if not hasattr(request.ctx, 'all_tables'):
        request.ctx.all_tables = {}
    if not hasattr(request.ctx, 'table_modules'):
        request.ctx.table_modules = {}
    if not hasattr(request.ctx, 'modules_tree'):
        request.ctx.modules_tree = {}

def check_auth(f):
    @wraps(f)
    async def decorated_function(request, *args, **kwargs):
        token = request.cookies.get("auth_token")
        if not token:
            return response.redirect("/login")
        
        # ONLY catch JWT token errors here so we don't swallow app errors!
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except Exception as e:
            print(f"JWT Auth Error: {e}")
            res = response.redirect("/login")
            res.delete_cookie("auth_token")
            return add_security_headers(res)

        user_id = payload.get("user_id")
        session_id = payload.get("session_id")
        
        now = time.time()
        cached = USER_AUTH_CACHE.get(user_id)
        
        # Use RAM Cache for 30 seconds for blazing fast clicks
        if cached and now < cached["expires"] and "all_tables" in cached:
            if cached["session_id"] != session_id:
                res = response.redirect("/login")
                res.delete_cookie("auth_token")
                return add_security_headers(res)
            request.ctx.user_id = user_id
            request.ctx.username = payload.get("username")
            request.ctx.role = cached["role"]
            request.ctx.all_tables = cached.get("all_tables", {})
            request.ctx.table_modules = cached.get("table_modules", {})
            request.ctx.modules_tree = cached.get("modules_tree", {})
        else:
            # Live query to check if their Role changed or if they logged in elsewhere
            try:
                async with app.ctx.pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT pus_session_id, pus_user_type FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if not user or user["pus_session_id"] != session_id:
                        res = response.redirect("/login")
                        res.delete_cookie("auth_token")
                        return add_security_headers(res)
                    
                    role = user["pus_user_type"] or "STD"
                    auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
                    modules_tree = build_modules_tree(auth_tables, table_modules)

                    USER_AUTH_CACHE[user_id] = {
                        "session_id": session_id,
                        "role": role,
                        "all_tables": auth_tables,
                        "table_modules": table_modules,
                        "modules_tree": modules_tree,
                        "expires": now + CACHE_TTL
                    }
                    request.ctx.user_id = user_id
                    request.ctx.username = payload.get("username")
                    request.ctx.role = role
                    request.ctx.all_tables = auth_tables
                    request.ctx.table_modules = table_modules
                    request.ctx.modules_tree = modules_tree
            except Exception as db_err:
                print(f"Database Auth Check Error: {db_err}")
                return response.text(f"Database connection error: {db_err}", status=500)

        # Execute the actual route. Any error here will now correctly show in console/browser!
        return await f(request, *args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        return render_template('login.html', request=request)
    
    # Safely handle requests missing a JSON payload to prevent NoneType crashes
    data = request.json or {}
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
                # Checks plain-text passwords if bcrypt fails
                if password == stored_pwd:
                    is_valid = True
                    
            if is_valid:
                session_id = str(uuid.uuid4())
                await conn.execute("UPDATE phc_users_t SET pus_session_id = $1 WHERE pus_user_id = $2", session_id, user['pus_user_id'])
                
                token_payload = {
                    "user_id": user['pus_user_id'],
                    "username": user['pus_user_name'],
                    "session_id": session_id,
                    "exp": time.time() + 86400  # Deprecation fix for datetime.utcnow()
                }
                token = jwt.encode(token_payload, JWT_SECRET, algorithm="HS256")
                USER_AUTH_CACHE.pop(user['pus_user_id'], None)
                
                res = response.json({"status": "success", "message": "Login successful"})
                # Set Session Cookie (No max_age!) - It will expire on browser close
                res.add_cookie("auth_token", token, httponly=True, samesite="Lax")
                return add_security_headers(res)
        
        return response.json({"status": "error", "message": "Invalid credentials"}, status=401)

@app.route('/logout', methods=['GET'])
async def logout(request):
    res = response.redirect("/login")
    res.delete_cookie("auth_token")
    return add_security_headers(res)

async def get_pk_column(conn, table_name):
    if table_name in SCHEMA_CACHE["pks"]:
        return SCHEMA_CACHE["pks"][table_name]
    query = """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1::regclass AND i.indisprimary;
    """
    try:
        pk = await conn.fetchval(query, table_name)
        if pk:
            SCHEMA_CACHE["pks"][table_name] = pk
        return pk
    except Exception:
        return None

# --- SMART FOREIGN KEY RESOLUTION ---
FK_HEURISTICS = {
    'company_id': 'phc_companies_t',
    'dept_id': 'phc_dept_t',
    'services_id': 'phc_services_t',
    'cost_center_id': 'phc_cost_center_t',
    'module_id': 'phc_module_t',
    'screen_id': 'phc_screens_t',
    'role_id': 'phc_roles_t',
    'user_id': 'phc_users_t',
}

async def get_fk_map(conn, table_name):
    if "fks" not in SCHEMA_CACHE: SCHEMA_CACHE["fks"] = {}
    if table_name in SCHEMA_CACHE["fks"]: return SCHEMA_CACHE["fks"][table_name]
    query = """
        SELECT kcu.column_name AS col, ccu.table_name AS f_table, ccu.column_name AS f_col
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1
    """
    try:
        fks = await conn.fetch(query, table_name)
        fk_map = {row['col']: {'table': row['f_table'], 'pk': row['f_col']} for row in fks}
        SCHEMA_CACHE["fks"][table_name] = fk_map
        return fk_map
    except Exception: return {}

async def resolve_fk_details(conn, table_name, column_name):
    fk_map = await get_fk_map(conn, table_name)
    if column_name in fk_map: return fk_map[column_name]['table'], fk_map[column_name]['pk']
    for suffix, target_table in FK_HEURISTICS.items():
        if column_name.endswith(suffix):
            target_pk = await get_pk_column(conn, target_table)
            if target_pk: return target_table, target_pk
    return None, None

async def get_fk_display_dict(conn, f_table, f_pk, specific_ids=None):
    if "display_cols" not in SCHEMA_CACHE: SCHEMA_CACHE["display_cols"] = {}
    if f_table not in SCHEMA_CACHE["display_cols"]:
        cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = $1", f_table)
        col_names = [c['column_name'] for c in cols]
        display_col = f_pk
        for c in col_names:
            if c.endswith('_name') or c == 'name':
                display_col = c
                break
        status_col = None
        for c in col_names:
            if c.endswith('_status') or c == 'status':
                status_col = c
                break
        SCHEMA_CACHE["display_cols"][f_table] = {"display": display_col, "status": status_col}
        
    info = SCHEMA_CACHE["display_cols"][f_table]
    display_col = info["display"]
    status_col = info["status"]
    
    if specific_ids is not None:
        if not specific_ids: return {}
        q = f"SELECT {f_pk} as id, {display_col} as name FROM {f_table} WHERE {f_pk} = ANY($1)"
        rows = await conn.fetch(q, list(specific_ids))
        return {r['id']: r['name'] for r in rows}
    else:
        q = f"SELECT {f_pk} as id, {display_col} as name FROM {f_table}"
        if status_col: q += f" WHERE {status_col} = 'ACT'"
        rows = await conn.fetch(q)
        return [{"id": str(r['id']), "name": f"{r['name']} (ID: {r['id']})"} for r in rows]

async def get_dropdown_options(conn, table_name, column_name):
    if column_name.endswith('_org_id'): return []
    f_table, f_pk = await resolve_fk_details(conn, table_name, column_name)
    if f_table and f_pk:
        try:
            return await get_fk_display_dict(conn, f_table, f_pk)
        except Exception: pass
    return []


def _sanitize_payload(data, pk_column, schema_map, is_update=False):
    clean_data = {}
    for k, v in data.items():
        if v == "" or v is None: continue 
        if k == pk_column: continue 
        if k.endswith(('_created', '_modified', '_created_by', '_modified_by')): continue

        if k == 'pus_pwd' and v:
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
        
        if target_type in ('integer', 'bigint', 'smallint') and isinstance(v, str):
            try:
                clean_data[k] = int(v)
            except (ValueError, TypeError):
                pass  # Skip values that can't be converted to int
        elif target_type == 'numeric' and isinstance(v, str):
            try:
                clean_data[k] = float(v) if '.' in v else int(v)
            except (ValueError, TypeError):
                pass  # Skip values that can't be converted to a number
        else:
            clean_data[k] = v

    return clean_data

@app.route('/')
@check_auth
async def dashboard(request):
    stats = {}
    return render_template('dashboard.html', request=request, stats=stats)

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    
    page = int(request.args.get("page", 1))
    per_page = 50
    offset = (page - 1) * per_page
    search_query = request.args.get("q", "").strip()

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with app.ctx.pool.acquire() as conn:
        if auth_tables is None or table_modules is None:
            auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            raise NotFound("Table configuration error: No Primary Key")

        columns_data = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)

        if not columns_data:
            raise NotFound("Table does not exist")

        columns = []
        date_columns = []
        for c in columns_data:
            cname = c['column_name']
            if cname in (pk_column, 'psn_screen_id'): continue
            if 'created' in cname.lower() or 'modified' in cname.lower(): continue
            if 'company_id' in cname.lower() and role != 'ADM': continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            col_def = {"raw": cname, "label": clean_label}
            if 'date' in c['data_type'] or 'timestamp' in c['data_type']:
                date_columns.append(col_def)
            else:
                columns.append(col_def)
                
        columns.extend(date_columns)

        base_query = f"SELECT * FROM {table_name}"
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        params = []

        if search_query:
            text_cols = [c['column_name'] for c in columns_data if c['data_type'] in ('character varying', 'text', 'character')]
            if text_cols:
                clauses = [f"{col} ILIKE ${i+1}" for i, col in enumerate(text_cols)]
                where_clause = " WHERE " + " OR ".join(clauses)
                base_query += where_clause
                count_query += where_clause
                params = [f"%{search_query}%" for _ in text_cols]

        base_query += f" ORDER BY {pk_column} DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
        
        total_count = await conn.fetchval(count_query, *params)
        raw_rows = await conn.fetch(base_query, *(params + [per_page, offset]))

        # --- DYNAMIC FOREIGN KEY RESOLUTION FOR TABLE VIEW ---
        resolved_rows = [dict(r) for r in raw_rows]
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
            f_table, f_pk = await resolve_fk_details(conn, table_name, cname)
            if f_table and f_pk:
                ids = set(r[cname] for r in resolved_rows if r[cname] is not None)
                if ids:
                    try:
                        lookup_dict = await get_fk_display_dict(conn, f_table, f_pk, specific_ids=ids)
                        for r in resolved_rows:
                            val = r[cname]
                            if val in lookup_dict:
                                r[cname] = f"{lookup_dict[val]} (ID: {val})"
                    except Exception as e:
                        print(f"Error resolving table FK {cname}: {e}")
        rows = resolved_rows

    total_pages = (total_count + per_page - 1) // per_page
    start_row = offset + 1 if total_count > 0 else 0
    end_row = min(offset + per_page, total_count)

    return render_template(
        'table_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        rows=rows,
        pk_column=pk_column,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        start_row=start_row,
        end_row=end_row,
        search_query=search_query
    )

@app.route('/new/<table_name>', methods=['GET'])
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

async def render_form(request, table_name, is_update=False, pk_val=None):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with app.ctx.pool.acquire() as conn:
        if auth_tables is None or table_modules is None:
            auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)

        columns_data = await conn.fetch("""
            SELECT column_name, data_type, is_nullable, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)

        row_data = {}
        if is_update:
            row_data = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE {pk_column} = $1", int(pk_val) if pk_val.isdigit() else pk_val)
            if not row_data:
                raise NotFound("Record not found")

        columns = []
        for c in columns_data:
            cname = c['column_name']
            if 'company_id' in cname.lower() and role != 'ADM': continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            val = row_data.get(cname, '') if is_update else ''
            options = await get_dropdown_options(conn, table_name, cname)

            json_options = None
            if table_name == 'phc_role_screen_assignment_t' and cname == 'prs_screen_id' and not is_update:
                json_options = await get_dropdown_options(conn, table_name, cname)
                options = [] 
                
            columns.append({
                "column_name": cname,
                "label": clean_label,
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO' and 'default' not in cname.lower(),
                "is_pk": cname == pk_column,
                "value": val,
                "options": options,
                "json_options": json_options
            })

    return render_template(
        'form_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        is_update=is_update,
        pk_val=pk_val
    )

@app.route('/export/<table_name>')
@check_auth
async def export_table_csv(request, table_name):
    import csv
    import io
    
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    async with app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")

        pk_column = await get_pk_column(conn, table_name)

        columns_data = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)

        # Build clean column list (same filtering logic as table_view)
        export_cols = []
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            if 'created' in cname.lower() or 'modified' in cname.lower(): continue
            if 'company_id' in cname.lower() and role != 'ADM': continue
            export_cols.append(cname)

        if not export_cols:
            return response.text("No exportable columns found.", status=400)

        col_list = ", ".join(export_cols)
        rows = await conn.fetch(f"SELECT {col_list} FROM {table_name} ORDER BY {pk_column} DESC")

    # Build CSV in-memory
    output = io.StringIO()
    writer = csv.writer(output)

    # Header row — clean labels
    header = [col.split('_', 1)[-1].replace('_', ' ').title() for col in export_cols]
    writer.writerow(header)

    # Data rows
    for row in rows:
        csv_row = []
        for col in export_cols:
            val = row[col]
            if val is None:
                csv_row.append('')
            elif isinstance(val, datetime):
                csv_row.append(val.strftime('%Y-%m-%d'))
            else:
                csv_row.append(str(val))
        writer.writerow(csv_row)

    csv_content = output.getvalue()
    output.close()

    table_title = auth_tables.get(table_name, table_name)
    filename = f"{table_title.replace(' ', '_')}_Export.csv"

    res = response.text(csv_content, content_type="text/csv")
    res.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return add_security_headers(res)

@app.route('/api/<table_name>', methods=['POST'], name="api_create")
@check_auth
async def api_create_record(request, table_name):
    return await process_api_action(request, table_name, None)


@app.route('/api/<table_name>/<pk_val>', methods=['PUT', 'DELETE', 'POST'], name="api_update_delete")
@check_auth
async def api_modify_record(request, table_name, pk_val):
    return await process_api_action(request, table_name, pk_val)

async def process_api_action(request, table_name, pk_val):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    
    # Forms sometimes send PUT/DELETE as POST with a special _method field
    method = request.method
    if request.form and request.form.get('_method'):
        method = request.form.get('_method')[0].upper()

    async with app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            return response.json({"error": "Unauthorized"}, status=403)
        
        pk_column = await get_pk_column(conn, table_name)

        if method == 'DELETE':
            try:
                await conn.execute(f"DELETE FROM {table_name} WHERE {pk_column} = $1", int(pk_val) if pk_val.isdigit() else pk_val)
                return response.json({"status": "success"})
            except Exception as e:
                return response.json({"error": str(e)}, status=400)

        data = request.form if request.form else request.json
        if not data:
            return response.json({"error": "No data provided"}, status=400)
            
        data_dict = {k: v[0] if isinstance(v, list) else v for k, v in data.items() if k != '_method'}

        columns_info = await conn.fetch("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public'
        """, table_name)
        schema_map = {c['column_name']: dict(c) for c in columns_info}

        clean_data = _sanitize_payload(data_dict, pk_column, schema_map, is_update=(method == 'PUT'))

        company_col = next((c for c in schema_map if c.endswith('_company_id') or c == 'company_id'), None)
        if company_col and role != 'ADM':
            user_company = await conn.fetchval("SELECT pus_company_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
            if user_company:
                clean_data[company_col] = user_company

        who_cols = [c for c in schema_map if c.endswith(('_created', '_modified', '_created_by', '_modified_by'))]
        for wc in who_cols:
            if 'modified' in wc and 'by' not in wc: clean_data[wc] = datetime.now()
            elif 'modified_by' in wc: clean_data[wc] = "System"
            elif method == 'POST':
                if 'created' in wc and 'by' not in wc: clean_data[wc] = datetime.now()
                elif 'created_by' in wc: clean_data[wc] = "System"

        try:
            if method == 'POST':
                if schema_map[pk_column]['data_type'] in ('integer', 'bigint', 'numeric'):
                    max_id = await conn.fetchval(f"SELECT MAX({pk_column}) FROM {table_name}")
                    clean_data[pk_column] = (max_id or 0) + 1

                cols = list(clean_data.keys())
                vals = list(clean_data.values())
                placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
                q = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"
                await conn.execute(q, *vals)

            elif method == 'PUT':
                cols = list(clean_data.keys())
                vals = list(clean_data.values())
                set_clause = ", ".join([f"{c} = ${i+1}" for i, c in enumerate(cols)])
                q = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ${len(vals)+1}"
                await conn.execute(q, *(vals + [int(pk_val) if pk_val.isdigit() else pk_val]))

            # Handle both JSON responses and Standard HTML Forms
            if request.headers.get("HX-Request"):
                res = response.json({"status": "success"})
                res.headers["HX-Redirect"] = f"/table/{table_name}"
                return add_security_headers(res)
            else:
                return response.redirect(f"/table/{table_name}")
            
        except Exception as e:
            return response.json({"error": str(e)}, status=400)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=PORT, single_process=True)
