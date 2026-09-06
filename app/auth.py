import time
from functools import wraps
import jwt
from sanic import response

from app.config import (
    JWT_SECRET, CACHE_TTL, RATE_LIMIT_WINDOW, MAX_LOGIN_ATTEMPTS, logger
)
from app.utils import add_security_headers, get_module_icon
from app.database import register_auth_cache_clear_cb

USER_AUTH_CACHE = {}

def prune_user_auth_cache():
    """Prunes expired entries and caps USER_AUTH_CACHE size."""
    now = time.time()
    expired = [k for k, v in USER_AUTH_CACHE.items() if now >= v.get("expires", 0)]
    for k in expired:
        USER_AUTH_CACHE.pop(k, None)
    if len(USER_AUTH_CACHE) > 500:
        keys = list(USER_AUTH_CACHE.keys())[:100]
        for k in keys:
            USER_AUTH_CACHE.pop(k, None)

def clear_auth_cache():
    USER_AUTH_CACHE.clear()

# Register callback so database schema clear can flush auth cache without circular dependencies
register_auth_cache_clear_cb(clear_auth_cache)

async def build_modules_tree(conn, all_tables, table_modules, schema_cols_map):
    """Constructs the hierarchical module and screen navigation tree with icons and cached search indexing."""
    if not all_tables:
        return {}
        
    table_codes = list(all_tables.keys())
    if schema_cols_map is None:
        try:
            cols_query = """
                SELECT table_name, column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = ANY($1)
            """
            col_rows = await conn.fetch(cols_query, table_codes)
            cols_map = {}
            for r in col_rows:
                t = r['table_name']
                if t not in cols_map:
                    cols_map[t] = []
                cols_map[t].append(r['column_name'].lower().replace('_', ' '))
            schema_cols_map = cols_map
        except Exception:
            cols_map = {}
    else:
        cols_map = schema_cols_map

    mod_icons = {}
    try:
        mod_rows = await conn.fetch("SELECT pmd_module_name, pmd_module_icon FROM phc_module_t WHERE pmd_status = 'ACT' OR pmd_status IS NULL")
        for mr in mod_rows:
            if mr['pmd_module_name']:
                mod_icons[mr['pmd_module_name']] = mr.get('pmd_module_icon')
    except Exception:
        pass

    modules_tree = {}
    for tbl_code, tbl_name in all_tables.items():
        mod_name = table_modules.get(tbl_code, 'General')
        if mod_name not in modules_tree:
            modules_tree[mod_name] = {
                "name": mod_name,
                "icon": get_module_icon(mod_name, mod_icons.get(mod_name)),
                "screens": []
            }
            
        search_str = f"{tbl_name.lower()} {' '.join(cols_map.get(tbl_code, []))}"
        modules_tree[mod_name]["screens"].append({
            "code": tbl_code, 
            "name": tbl_name,
            "search_terms": search_str
        })
    return dict(sorted(modules_tree.items())), schema_cols_map

async def get_authorized_tables(conn, user_id, role):
    auth_tables = {}
    table_modules = {}

    if role == 'ADM':
        query = """
            SELECT 
                s.psn_screen_code, 
                s.psn_screen_name, 
                COALESCE(m.pmd_module_name, 'General') AS module_name
            FROM phc_screens_t s
            LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
            WHERE s.psn_status = 'ACT' OR s.psn_status IS NULL
            ORDER BY m.pmd_module_name, s.psn_screen_name
        """
        rows = await conn.fetch(query)
    else:
        # Try fast compiled Database View first; gracefully fallback to base tables if view is not yet created
        try:
            rows = await conn.fetch("""
                SELECT psn_screen_code, psn_screen_name, module_name
                FROM v_user_authorized_screens
                WHERE pua_user_id = $1
                ORDER BY module_name, psn_screen_name
            """, user_id)
        except Exception:
            query = """
                SELECT DISTINCT 
                    s.psn_screen_code, 
                    s.psn_screen_name, 
                    COALESCE(m.pmd_module_name, 'General') AS module_name
                FROM phc_screens_t s
                JOIN phc_role_screen_assignment_t rsa ON s.psn_screen_id = rsa.prs_screen_id
                JOIN phc_user_roles_assignment_t ura ON rsa.prs_role_id = ura.pua_role_id
                LEFT JOIN phc_module_t m ON s.psn_module_id = m.pmd_module_id
                WHERE ura.pua_user_id = $1 
                  AND (s.psn_status = 'ACT' OR s.psn_status IS NULL)
                ORDER BY m.pmd_module_name, s.psn_screen_name
            """
            rows = await conn.fetch(query, user_id)

    for r in rows:
        code = r['psn_screen_code'].lower()
        if role != 'ADM' and r['module_name'].lower() == 'erpadmin':
            continue
        auth_tables[code] = r['psn_screen_name']
        table_modules[code] = r['module_name']

    return auth_tables, table_modules

def check_auth(f):
    @wraps(f)
    async def decorated_function(request, *args, **kwargs):
        prune_user_auth_cache()

        def unauth_response(req):
            if req.path.startswith('/api/'):
                return add_security_headers(response.json({"error": "Unauthorized"}, status=401))
            res = response.redirect("/login")
            res.delete_cookie("auth_token", path="/")
            return add_security_headers(res)

        token = request.cookies.get("auth_token")
        if not token:
            return unauth_response(request)
        
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except Exception as e:
            logger.warning(f"JWT Auth Error: {e}")
            return unauth_response(request)

        user_id = payload.get("user_id")
        session_id = payload.get("session_id")
        
        now = time.time()
        cached = USER_AUTH_CACHE.get(user_id)
        
        if cached and now < cached["expires"] and "all_tables" in cached:
            if cached["session_id"] != session_id:
                return unauth_response(request)
            request.ctx.user_id = user_id
            request.ctx.username = payload.get("username")
            request.ctx.session_id = session_id
            request.ctx.role = cached["role"]
            request.ctx.all_tables = cached.get("all_tables", {})
            request.ctx.table_modules = cached.get("table_modules", {})
            request.ctx.modules_tree = cached.get("modules_tree", {})
        else:
            if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
                return add_security_headers(response.text("Database connection error: DATABASE_URL is not configured in .env", status=503))
            try:
                from app.database import SCHEMA_CACHE
                
                async with request.app.ctx.pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT pus_session_id, pus_user_type, pus_status FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if not user or user["pus_session_id"] != session_id:
                        return unauth_response(request)
                    if user.get("pus_status") and user["pus_status"] == 'INA':
                        return unauth_response(request)
                    
                    role = user["pus_user_type"] or "STD"
                    auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
                    
                    modules_tree, new_cols_map = await build_modules_tree(conn, auth_tables, table_modules, SCHEMA_CACHE["cols_map"])
                    SCHEMA_CACHE["cols_map"] = new_cols_map

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
                    request.ctx.session_id = session_id
                    request.ctx.role = role
                    request.ctx.all_tables = auth_tables
                    request.ctx.table_modules = table_modules
                    request.ctx.modules_tree = modules_tree
            except Exception as db_err:
                logger.error(f"Database Auth Check Error: {db_err}")
                return add_security_headers(response.text(f"Database connection error: {db_err}", status=500))

        return await f(request, *args, **kwargs)
    return decorated_function

# Enterprise Sliding-Window Rate Limiter
LOGIN_ATTEMPTS = {}  # ip -> list of timestamp floats

def check_login_rate_limit(ip: str):
    """Returns (is_allowed, seconds_remaining)."""
    now = time.time()
    attempts = LOGIN_ATTEMPTS.get(ip, [])
    attempts = [t for t in attempts if now - t < RATE_LIMIT_WINDOW]
    LOGIN_ATTEMPTS[ip] = attempts
    if len(attempts) >= MAX_LOGIN_ATTEMPTS:
        oldest = attempts[0]
        remaining = int(RATE_LIMIT_WINDOW - (now - oldest))
        return False, max(1, remaining)
    return True, 0

def record_login_attempt(ip: str, success: bool = False):
    """Records attempt timestamp or clears on successful authentication."""
    if success:
        LOGIN_ATTEMPTS.pop(ip, None)
    else:
        now = time.time()
        attempts = LOGIN_ATTEMPTS.get(ip, [])
        attempts.append(now)
        LOGIN_ATTEMPTS[ip] = [t for t in attempts if now - t < RATE_LIMIT_WINDOW]

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
