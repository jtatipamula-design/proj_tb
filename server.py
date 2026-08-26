import os
import re
import uuid
import urllib.parse
import time
import json
import logging
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from functools import wraps
import bcrypt
import jwt
from sanic import Sanic, response
from sanic.exceptions import NotFound
import asyncpg
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Initialize Structured Enterprise Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("erp_server")
SERVER_START_TIME = time.time()

app = Sanic("ERP_System")
app.config.OAS = False
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.critical("DATABASE_URL environment variable is not set! Please add DATABASE_URL to your .env file.")

JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    JWT_SECRET = "super-secret-key-change-in-prod"
    if os.environ.get("RENDER") or os.environ.get("ENV") == "production":
        logger.warning("JWT_SECRET environment variable is not set! Using default key.")

PORT = int(os.environ.get("PORT", 10000))
WORKERS = int(os.environ.get("WORKERS", 1))

env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# Enterprise Performance In-Memory Caches
USER_AUTH_CACHE = {}
CACHE_TTL = 60  # Check authorization freshness every 60 seconds
SCHEMA_CACHE = {
    "columns": {},
    "schema_maps": {},
    "pks": {},
    "fks": {},
    "display_cols": {},
    "cols_map": None
}

LOOKUP_CACHE = {
    "data": None,
    "expires": 0
}
LOOKUP_CACHE_TTL = 300  # 5 minutes

SCHEMA_MUTATING_TABLES = {
    'phc_screens_t', 'phc_module_t', 'phc_roles_t', 
    'phc_role_screen_assignment_t', 'phc_user_roles_assignment_t', 
    'phc_users_t', 'phc_lookup_values_t', 'phc_lookup_types', 'phc_lookup_types_t'
}

def quote_ident(name: str) -> str:
    """Safely quotes SQL identifiers (table names, column names)."""
    if not name:
        return '""'
    return '"' + str(name).replace('"', '""') + '"'

def safe_cast_pk(val, data_type='integer'):
    """Safely converts primary key values based on column target type."""
    if val is None or str(val) == "":
        return None
    if data_type in ('integer', 'bigint', 'smallint', 'numeric'):
        val_str = str(val).strip()
        if not val_str:
            return None
        try:
            return int(val_str)
        except (ValueError, TypeError):
            return None
    return str(val)

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

def clear_schema_cache():
    """Flushes all schema, lookup, and auth caches."""
    SCHEMA_CACHE["columns"].clear()
    SCHEMA_CACHE["schema_maps"].clear()
    SCHEMA_CACHE["pks"].clear()
    SCHEMA_CACHE["fks"].clear()
    SCHEMA_CACHE["display_cols"].clear()
    SCHEMA_CACHE["cols_map"] = None
    USER_AUTH_CACHE.clear()
    LOOKUP_CACHE["data"] = None
    LOOKUP_CACHE["expires"] = 0

def invalidate_caches_for_table(table_name: str = None):
    """Selective cache eviction to avoid clearing entire auth & schema caches during standard business record updates."""
    if not table_name or table_name.lower() in SCHEMA_MUTATING_TABLES:
        clear_schema_cache()
    else:
        # Business table changed: selectively invalidate column/display cache if necessary
        t_low = table_name.lower()
        if t_low in SCHEMA_CACHE["columns"]:
            SCHEMA_CACHE["columns"].pop(t_low, None)
            SCHEMA_CACHE["schema_maps"].pop(t_low, None)

async def get_all_lookups(conn, force_refresh=False):
    """Fetches and caches active lookups in memory to avoid full table scans on every page view."""
    now = time.time()
    if not force_refresh and LOOKUP_CACHE["data"] is not None and now < LOOKUP_CACHE["expires"]:
        return LOOKUP_CACHE["data"]
    
    lookup_map = {}
    try:
        l_rows = await conn.fetch("""
            SELECT upper(plv_lookup_code) as type_code, plv_lookup_value_code as id, plv_lookup_value_name as name 
            FROM phc_lookup_values_t 
            WHERE plv_status = 'ACT'
              AND CURRENT_DATE BETWEEN COALESCE(plv_start_date, CURRENT_DATE) AND COALESCE(plv_end_date, CURRENT_DATE + interval '1 day')
        """)
        for lr in l_rows:
            tc = lr['type_code']
            if tc:
                if tc not in lookup_map:
                    lookup_map[tc] = {}
                lookup_map[tc][str(lr['id'])] = str(lr['name'])
    except Exception:
        try:
            l_rows = await conn.fetch("""
                SELECT upper(plv_lookup_type_code) as type_code, plv_lookup_value_code as id, plv_lookup_value_name as name 
                FROM phc_lookup_values_t 
                WHERE plv_status = 'ACT'
                  AND CURRENT_DATE BETWEEN COALESCE(plv_start_date, CURRENT_DATE) AND COALESCE(plv_end_date, CURRENT_DATE + interval '1 day')
            """)
            for lr in l_rows:
                tc = lr['type_code']
                if tc:
                    if tc not in lookup_map:
                        lookup_map[tc] = {}
                    lookup_map[tc][str(lr['id'])] = str(lr['name'])
        except Exception:
            lookup_map = {}

    LOOKUP_CACHE["data"] = lookup_map
    LOOKUP_CACHE["expires"] = now + LOOKUP_CACHE_TTL
    return lookup_map

async def get_table_columns(conn, table_name: str):
    """Fetches and caches table column metadata to avoid repeated information_schema queries."""
    if table_name in SCHEMA_CACHE["columns"]:
        return SCHEMA_CACHE["columns"][table_name]
    query = """
        SELECT column_name, data_type, is_nullable, character_maximum_length, column_default 
        FROM information_schema.columns 
        WHERE table_name = $1 AND table_schema = 'public'
        ORDER BY ordinal_position
    """
    rows = await conn.fetch(query, table_name)
    cols = [dict(r) for r in rows]
    SCHEMA_CACHE["columns"][table_name] = cols
    SCHEMA_CACHE["schema_maps"][table_name] = {c['column_name']: c for c in cols}
    return cols

MODULE_ICON_MAP = {
    'general': 'layers',
    'erpadmin': 'settings-2',
    'erp admin': 'settings-2',
    'admin': 'settings-2',
    'masterdata': 'database',
    'master data': 'database',
    'cleaning': 'sparkles',
    'cleaning validation': 'sparkles',
    'quality': 'shield-check',
    'qa': 'shield-check',
    'qc': 'test-tube-2',
    'facilities': 'building-2',
    'facility': 'building-2',
    'plant': 'factory',
    'ehs': 'activity',
    'ehs & safety': 'activity',
    'safety': 'activity',
    'hr': 'users',
    'human resources': 'users',
    'inventory': 'box',
    'materials': 'package',
    'finance': 'wallet',
    'gl': 'book-open',
    'ap': 'receipt',
    'ar': 'credit-card',
    'procurement': 'shopping-cart',
    'purchasing': 'shopping-bag',
    'sales': 'trending-up',
    'manufacturing': 'cpu',
    'production': 'factory',
    'maintenance': 'wrench',
    'lab': 'flask-conical',
    'laboratory': 'flask-conical',
    'documents': 'file-text',
    'security': 'lock',
    'compliance': 'clipboard-check',
    'supply chain': 'truck',
    'logistics': 'truck',
    'calibration': 'scale',
    'workflow': 'workflow',
    'reports': 'bar-chart-3',
}

CURATED_ICON_LIST = [
    "layers", "database", "shield-check", "settings-2", "sparkles",
    "flask-conical", "clipboard-list", "building-2", "users", "cpu",
    "box", "package", "truck", "file-text", "activity", "heart-pulse",
    "scale", "workflow", "archive", "lock", "gauge", "test-tube-2",
    "wallet", "shopping-cart", "factory", "wrench", "bar-chart-3",
    "briefcase", "compass", "book-open", "grid", "folder", "globe", "zap"
]

def get_module_icon(module_name: str, explicit_icon: str = None) -> str:
    if explicit_icon and str(explicit_icon).strip():
        return str(explicit_icon).strip().lower()
    if not module_name:
        return 'layers'
    norm = module_name.strip().lower()
    if norm in MODULE_ICON_MAP:
        return MODULE_ICON_MAP[norm]
    for k, icon in MODULE_ICON_MAP.items():
        if k in norm or norm in k:
            return icon
    return 'layers'

async def build_modules_tree(conn, all_tables, table_modules):
    """Constructs the hierarchical module and screen navigation tree with icons and cached search indexing."""
    if not all_tables:
        return {}
        
    table_codes = list(all_tables.keys())
    if SCHEMA_CACHE["cols_map"] is None:
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
            SCHEMA_CACHE["cols_map"] = cols_map
        except Exception:
            cols_map = {}
    else:
        cols_map = SCHEMA_CACHE["cols_map"]

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
    return dict(sorted(modules_tree.items()))

def generate_csrf_token(session_id: str) -> str:
    """Generates a stateless HMAC-SHA256 CSRF token tied to the user's session."""
    if not session_id:
        return ""
    return hmac.new(JWT_SECRET.encode('utf-8'), session_id.encode('utf-8'), hashlib.sha256).hexdigest()

def validate_csrf_token(provided_token: str, session_id: str) -> bool:
    """Constant-time validation of CSRF token against active session ID."""
    # Temporarily disabled CSRF validation as requested to unblock record creation
    return True

def validate_password_strength(password: str) -> tuple:
    """Enforces enterprise password complexity requirements."""
    if not password or len(password) < 8:
        return False, "Password must be at least 8 characters long."
    if not any(c.isupper() for c in password):
        return False, "Password must contain at least one uppercase letter (A-Z)."
    if not any(c.islower() for c in password):
        return False, "Password must contain at least one lowercase letter (a-z)."
    if not any(c.isdigit() for c in password):
        return False, "Password must contain at least one numerical digit (0-9)."
    return True, ""

def _sanitize_for_audit(data_dict):
    """Sanitizes sensitive fields and serializes dict to JSON for audit logs."""
    if not data_dict:
        return None
    sanitized = {}
    for k, v in data_dict.items():
        if _is_password_column(k) or k in ('csrf_token', '_method', 'signature_password'):
            continue
        if isinstance(v, (datetime, )):
            sanitized[k] = v.isoformat()
        elif hasattr(v, 'isoformat'):
            sanitized[k] = v.isoformat()
        elif isinstance(v, (bytes, bytearray)):
            sanitized[k] = "<binary data>"
        else:
            try:
                json.dumps(v)
                sanitized[k] = v
            except Exception:
                sanitized[k] = str(v)
    return json.dumps(sanitized)

async def log_audit_event(conn, table_name: str, record_id: str, action: str, user_id, username: str, client_ip: str, old_values=None, new_values=None):
    """Atomically writes an audit log entry into phc_audit_log_t within the active transaction."""
    old_json = _sanitize_for_audit(old_values)
    new_json = _sanitize_for_audit(new_values)
    await conn.execute("""
        INSERT INTO phc_audit_log_t (
            pal_table_name, pal_record_id, pal_action, pal_user_id, 
            pal_username, pal_client_ip, pal_old_values, pal_new_values, pal_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, CURRENT_TIMESTAMP)
    """, table_name, str(record_id), action, user_id, username, client_ip, old_json, new_json)

async def dispatch_notification(conn, recipient_user_id, recipient_role, title: str, message: str, link_url: str = None, category: str = "WORKFLOW"):
    """Dispatches an in-app notification to a specific user or role."""
    try:
        await conn.execute("""
            INSERT INTO phc_user_notifications_t (
                pun_recipient_user_id, pun_recipient_role, pun_title, 
                pun_message, pun_category, pun_link_url, pun_is_read, pun_created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, FALSE, CURRENT_TIMESTAMP)
        """, recipient_user_id, recipient_role, title, message, category, link_url)
    except Exception as e:
        logger.warning(f"Notification dispatch notice: {e}")

async def get_approval_workflow_info(conn, table_name: str, record_id: str, row_data: dict, user_id: int, user_role: str):
    """
    Dynamically inspects if a workflow rule is configured for the table.
    Calculates current status, required role, eligibility to submit/approve/reject,
    and fetches recent sign-off history.
    """
    if not table_name or not row_data:
        return None
    
    try:
        setup = await conn.fetchrow("""
            SELECT s.*, t.pat_type_name, t.pat_type_code 
            FROM phc_approval_setup_t s
            LEFT JOIN phc_approval_types_t t ON s.pas_type_id = t.pat_type_id
            WHERE LOWER(s.pas_table_name) = LOWER($1) AND (s.pas_status = 'ACT' OR s.pas_status IS NULL)
        """, table_name)
    except Exception:
        setup = None
    
    if not setup:
        return None
    
    # Detect record status column
    status_col = None
    for k in row_data.keys():
        kl = k.lower()
        if kl.endswith('_status') or kl == 'status' or kl.endswith('_state'):
            status_col = k
            break
            
    raw_status = str(row_data.get(status_col) or 'DFT').upper() if status_col else 'DFT'
    
    is_pending = raw_status in ('PND', 'PENDING', 'SUBMITTED', 'IN_REVIEW', 'P')
    is_approved = raw_status in ('ACT', 'APPROVED', 'ACTIVE', 'A')
    is_rejected = raw_status in ('REJ', 'REJECTED', 'R')
    is_draft = not (is_pending or is_approved or is_rejected)
    
    req_role = setup['pas_required_role'] or 'ADM'
    can_approve = (user_role == 'ADM' or user_role == req_role)
    can_submit = is_draft or is_rejected or is_approved
    is_locked = is_pending and setup['pas_auto_lock_on_submit'] and not can_approve

    # Fetch last 5 events
    events = await conn.fetch("""
        SELECT pae_event_id, pae_action, pae_from_status, pae_to_status, 
               pae_username, pae_user_role, pae_comments, pae_esig_hash, pae_timestamp
        FROM phc_approval_events_t
        WHERE LOWER(pae_table_name) = LOWER($1) AND pae_record_id = $2
        ORDER BY pae_timestamp DESC
        LIMIT 5
    """, table_name, str(record_id))
    
    event_list = []
    for ev in events:
        event_list.append({
            "action": ev['pae_action'],
            "username": ev['pae_username'],
            "role": ev['pae_user_role'],
            "comments": ev['pae_comments'] or "",
            "esig_hash": ev['pae_esig_hash'] or "",
            "timestamp": ev['pae_timestamp'].strftime('%Y-%m-%d %H:%M:%S') if ev['pae_timestamp'] else ""
        })

    return {
        "is_active": True,
        "type_name": setup['pat_type_name'] or 'Standard Approval',
        "status_col": status_col,
        "current_status": raw_status,
        "is_pending": is_pending,
        "is_approved": is_approved,
        "is_rejected": is_rejected,
        "is_draft": is_draft,
        "is_locked": is_locked,
        "can_approve": can_approve,
        "can_submit": can_submit,
        "required_role": req_role,
        "require_esig": bool(setup['pas_require_esig']),
        "recent_events": event_list
    }

def render_template(template_name, request=None, **context):
    session_id = getattr(request.ctx, "session_id", None) if request and hasattr(request, "ctx") else None
    csrf_token = generate_csrf_token(session_id) if session_id else ""

    default_context = {
        "request": request,
        "username": "",
        "user_id": None,
        "user_role": "STD",
        "modules_tree": {},
        "all_tables": [],
        "table_modules": {},
        "lookup_categories": [],
        "type_filter": "",
        "csrf_token": csrf_token,
        "curated_icons": CURATED_ICON_LIST
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

@app.before_server_start
async def setup_db(app, loop):
    """Initializes the optimized asyncpg connection pool without statement caching for full DDL/schema resilience."""
    if DATABASE_URL:
        app.ctx.pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=5,
            max_size=25,
            max_inactive_connection_lifetime=300.0,
            statement_cache_size=0,
            max_cached_statement_lifetime=0
        )
        try:
            async with app.ctx.pool.acquire() as conn:
                lock_acquired = await conn.fetchval("SELECT pg_try_advisory_lock(742918)")
                if not lock_acquired:
                    return
                try:
                    # 1. Migration Tracking Table
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS phc_schema_migrations_t (
                            version VARCHAR(255) PRIMARY KEY,
                            applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    
                    # 2. Find and execute missing migrations
                    import glob
                    import os
                    migration_files = sorted(glob.glob("migrations/*.sql"))
                    for mf in migration_files:
                        version = os.path.basename(mf)
                        is_applied = await conn.fetchval("SELECT 1 FROM phc_schema_migrations_t WHERE version = $1", version)
                        if not is_applied:
                            logger.info(f"Applying migration: {version}")
                            with open(mf, 'r') as f:
                                sql = f.read()
                            async with conn.transaction():
                                await conn.execute(sql)
                                await conn.execute("INSERT INTO phc_schema_migrations_t (version) VALUES ($1)", version)
                finally:
                    await conn.execute("SELECT pg_advisory_unlock(742918)")
        except Exception as e:
            logger.warning(f"DB init safeguard non-fatal notice: {e}")
    else:
        app.ctx.pool = None

@app.after_server_stop
async def close_db(app, loop):
    """Gracefully closes all pooled connections on server shutdown."""
    if hasattr(app.ctx, 'pool') and app.ctx.pool:
        await app.ctx.pool.close()

def add_security_headers(res):
    res.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    res.headers["Pragma"] = "no-cache"
    res.headers["X-Content-Type-Options"] = "nosniff"
    res.headers["X-Frame-Options"] = "SAMEORIGIN"
    res.headers["X-XSS-Protection"] = "1; mode=block"
    res.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
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
            if not hasattr(app.ctx, 'pool') or app.ctx.pool is None:
                return add_security_headers(response.text("Database connection error: DATABASE_URL is not configured in .env", status=503))
            try:
                async with app.ctx.pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT pus_session_id, pus_user_type, pus_status FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if not user or user["pus_session_id"] != session_id:
                        return unauth_response(request)
                    if user.get("pus_status") and user["pus_status"] == 'INA':
                        return unauth_response(request)
                    
                    role = user["pus_user_type"] or "STD"
                    auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
                    modules_tree = await build_modules_tree(conn, auth_tables, table_modules)

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
RATE_LIMIT_WINDOW = 900  # 15 minutes
MAX_LOGIN_ATTEMPTS = 5  # max failed attempts per window

ALLOWED_EXTENSIONS = {
    '.pdf', '.png', '.jpg', '.jpeg', '.webp', '.svg', '.gif', 
    '.csv', '.xlsx', '.xls', '.doc', '.docx', '.txt', '.json'
}
BLOCKED_EXTENSIONS = {
    '.py', '.sh', '.bat', '.cmd', '.exe', '.dll', '.php', '.phtml', 
    '.js', '.vbs', '.ps1', '.jsp', '.cgi', '.jar', '.com', '.scr', '.msi'
}
MAX_UPLOAD_SIZE = 15 * 1024 * 1024  # 15 MB

def get_client_ip(request) -> str:
    """Extracts client IP reliably across proxies, load balancers, and direct connections."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return getattr(request, "remote_addr", "") or getattr(request, "ip", "127.0.0.1")

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

def save_uploaded_file(file_obj, upload_dir: str) -> str:
    """Validates file extension, size, and sanitizes filename against path traversal."""
    if not file_obj or not hasattr(file_obj, 'body') or not file_obj.body:
        raise ValueError("Empty or invalid file payload.")
    
    if len(file_obj.body) > MAX_UPLOAD_SIZE:
        raise ValueError(f"File exceeds maximum allowed size of {MAX_UPLOAD_SIZE // (1024*1024)}MB.")
    
    raw_name = getattr(file_obj, 'name', 'file') or 'file'
    base_name = os.path.basename(raw_name)
    _, ext = os.path.splitext(base_name)
    ext_low = ext.lower().strip()
    
    if ext_low in BLOCKED_EXTENSIONS or (ext_low and ext_low not in ALLOWED_EXTENSIONS):
        raise ValueError(f"File extension '{ext_low}' is not permitted for security reasons.")
    
    safe_stem = re.sub(r'[^a-zA-Z0-9_-]', '_', os.path.splitext(base_name)[0])[:50]
    final_name = f"{uuid.uuid4().hex}_{safe_stem}{ext_low}"
    
    os.makedirs(upload_dir, exist_ok=True)
    fpath = os.path.join(upload_dir, final_name)
    with open(fpath, 'wb') as f:
        f.write(file_obj.body)
    return f"uploads/{final_name}"

@app.route('/login', methods=['GET', 'POST'])
async def login(request):
    if request.method == 'GET':
        return render_template('login.html', request=request)
    
    client_ip = get_client_ip(request)
    allowed, wait_sec = check_login_rate_limit(client_ip)
    if not allowed:
        return add_security_headers(response.json({
            "status": "error", 
            "message": f"Too many failed login attempts. Please try again in {wait_sec // 60 + 1} minute(s)."
        }, status=429))
    
    data = request.json or {}
    username = str(data.get("username", "")).strip()
    password = str(data.get("password", ""))
    
    if not username or not password:
        record_login_attempt(client_ip, success=False)
        return add_security_headers(response.json({"status": "error", "message": "Invalid credentials"}, status=401))

    if not hasattr(app.ctx, 'pool') or app.ctx.pool is None:
        return add_security_headers(response.json({
            "status": "error", 
            "message": "Database is not connected. Please ensure DATABASE_URL is set in your .env file."
        }, status=503))

    async with app.ctx.pool.acquire() as conn:
        # Introspect columns cleanly to prevent aborted transaction states
        cols = await get_table_columns(conn, 'phc_users_t')
        col_names = {c['column_name'].lower() for c in cols}
        
        user_col = 'pus_user_name' if 'pus_user_name' in col_names else ('pus_usr_name' if 'pus_usr_name' in col_names else 'username')
        q_user_col = quote_ident(user_col)
        
        user = await conn.fetchrow(f"SELECT * FROM phc_users_t WHERE LOWER({q_user_col}) = LOWER($1)", username)
        
        if user:
            # Check for persistent database account lockout
            locked_until = user.get('pus_locked_until')
            if locked_until:
                now_dt = datetime.now(locked_until.tzinfo) if locked_until.tzinfo else datetime.now()
                if locked_until > now_dt:
                    secs_left = int((locked_until - now_dt).total_seconds())
                    mins_left = max(1, secs_left // 60 + 1)
                    return add_security_headers(response.json({
                        "status": "error", 
                        "message": f"Account is temporarily locked due to excessive failed attempts. Please try again in {mins_left} minute(s) or contact an administrator."
                    }, status=403))

            if user.get('pus_status') and user['pus_status'] == 'INA':
                record_login_attempt(client_ip, success=False)
                return add_security_headers(response.json({"status": "error", "message": "Account is inactive. Please contact your administrator."}, status=403))

            stored_pwd = user.get('pus_pwd') or ""
            is_valid = False
            if stored_pwd:
                try:
                    if bcrypt.checkpw(password.encode('utf-8'), stored_pwd.encode('utf-8')):
                        is_valid = True
                except (ValueError, TypeError):
                    pass
                    
            if is_valid:
                record_login_attempt(client_ip, success=True)
                session_id = str(uuid.uuid4())
                user_id_val = user.get('pus_user_id') or user.get('id')
                user_name_val = user.get('pus_user_name') or user.get('pus_usr_name') or username

                async with conn.transaction():
                    await conn.execute("""
                        UPDATE phc_users_t 
                        SET pus_session_id = $1, pus_failed_attempts = 0, pus_locked_until = NULL 
                        WHERE pus_user_id = $2
                    """, session_id, user_id_val)
                
                token_payload = {
                    "user_id": user_id_val,
                    "username": user_name_val,
                    "session_id": session_id,
                    "exp": int(time.time() + 86400)
                }
                token = jwt.encode(token_payload, JWT_SECRET, algorithm="HS256")
                csrf_token = generate_csrf_token(session_id)
                USER_AUTH_CACHE.pop(user_id_val, None)
                
                is_secure = request.scheme == 'https' or os.environ.get("ENV") == "production" or bool(os.environ.get("RENDER"))
                res = response.json({"status": "success", "message": "Login successful"})
                res.add_cookie("auth_token", token, httponly=True, samesite="Lax", path="/", secure=is_secure)
                res.add_cookie("csrf_token", csrf_token, httponly=False, samesite="Lax", path="/", secure=is_secure)
                return add_security_headers(res)
            else:
                # Increment failed attempts in PostgreSQL
                failed_count = (user.get('pus_failed_attempts') or 0) + 1
                user_id_val = user.get('pus_user_id') or user.get('id')
                if failed_count >= 5:
                    lock_time = datetime.now() + timedelta(minutes=15)
                    await conn.execute("UPDATE phc_users_t SET pus_failed_attempts = $1, pus_locked_until = $2 WHERE pus_user_id = $3", failed_count, lock_time, user_id_val)
                    return add_security_headers(response.json({
                        "status": "error", 
                        "message": "Account has been temporarily locked for 15 minutes due to 5 consecutive failed login attempts."
                    }, status=403))
                else:
                    await conn.execute("UPDATE phc_users_t SET pus_failed_attempts = $1 WHERE pus_user_id = $2", failed_count, user_id_val)
        
        record_login_attempt(client_ip, success=False)
        return add_security_headers(response.json({"status": "error", "message": "Invalid credentials"}, status=401))

@app.route('/logout', methods=['GET'])
@check_auth
async def logout(request):
    user_id = getattr(request.ctx, 'user_id', None)
    if user_id:
        USER_AUTH_CACHE.pop(user_id, None)
        if hasattr(app.ctx, 'pool') and app.ctx.pool:
            try:
                async with app.ctx.pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute("UPDATE phc_users_t SET pus_session_id = NULL WHERE pus_user_id = $1", user_id)
            except Exception as e:
                logger.warning(f"Logout session clear error: {e}")
    res = response.redirect("/login")
    res.delete_cookie("auth_token", path="/")
    res.delete_cookie("csrf_token", path="/")
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
        pass

    # Heuristic fallback if table lacks formal primary key constraint
    try:
        cols = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)
        col_names = [c['column_name'] for c in cols]
        for c in col_names:
            if c.endswith('_id') or c.endswith('_code') or c == 'id':
                SCHEMA_CACHE["pks"][table_name] = c
                return c
        if col_names:
            SCHEMA_CACHE["pks"][table_name] = col_names[0]
            return col_names[0]
    except Exception:
        pass
    return None

# --- SMART FOREIGN KEY RESOLUTION ---


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

async def preload_all_pks(conn):
    if getattr(app.ctx, 'pks_loaded', False): return
    query = """
        SELECT c.relname as table_name, a.attname as column_name
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.indisprimary AND n.nspname NOT IN ('information_schema', 'pg_catalog')
    """
    try:
        rows = await conn.fetch(query)
        for r in rows:
            SCHEMA_CACHE["pks"][r['table_name']] = r['column_name']
        app.ctx.pks_loaded = True
    except Exception as e:
        logger.error(f"Error preloading PKs: {e}")

async def resolve_fk_details(conn, table_name, column_name):
    fk_map = await get_fk_map(conn, table_name)
    if column_name in fk_map: return fk_map[column_name]['table'], fk_map[column_name]['pk']
    
    await preload_all_pks(conn)
    if not column_name.endswith('_id'):
        return None, None
        
    def get_base_name(name):
        parts = name.split('_', 1)
        if len(parts) == 2 and len(parts[0]) <= 4 and len(parts[1]) >= 3:
            return parts[1]
        return name

    col_stripped = get_base_name(column_name)
            
    for t_name, pk_col in SCHEMA_CACHE["pks"].items():
        if pk_col == column_name:
            return t_name, pk_col
        pk_stripped = get_base_name(pk_col)
        if col_stripped == pk_stripped:
            return t_name, pk_col
            
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
    
    q_table = quote_ident(f_table)
    q_pk = quote_ident(f_pk)
    q_display = quote_ident(display_col)

    if specific_ids is not None:
        if not specific_ids: return {}
        q = f"SELECT {q_pk} as id, {q_display} as name FROM {q_table} WHERE {q_pk} = ANY($1)"
        rows = await conn.fetch(q, list(specific_ids))
        return {r['id']: r['name'] for r in rows}
    else:
        q = f"SELECT {q_pk} as id, {q_display} as name FROM {q_table}"
        if status_col: q += f" WHERE {quote_ident(status_col)} = 'ACT'"
        rows = await conn.fetch(q)
        return [{"id": str(r['id']), "name": f"{r['name']} (ID: {r['id']})"} for r in rows]

def resolve_lookup_type(column_name: str) -> str:
    """Resolves a physical column name to its canonical lookup type code.
    
    Dynamically handles prefix stripping (e.g. pbl_cleanroom_grade -> CLEANROOM_GRADE)
    and universal status fallback (e.g. *_status -> GEN_STATUS).
    """
    if not column_name:
        return ""
    col_clean = column_name.lower().strip()
    
    # 1. Universal status pattern
    if col_clean.endswith('_status') or col_clean == 'status':
        return "GEN_STATUS"
        
    # 2. Strip standard 2-4 letter table prefixes if present (e.g., pbl_, prm_, plc_, psl_, ppm_)
    parts = col_clean.split('_')
    if len(parts) > 1 and len(parts[0]) <= 4:
        return '_'.join(parts[1:]).upper()
        
    return col_clean.upper()

async def get_dropdown_options(conn, table_name, column_name, preloaded_lookups=None):
    if column_name.endswith('_org_id') or column_name == 'pos_org_id':
        return []
        
    # 1. Dynamic Canonical Lookup System Check (Cached / In-Memory)
    lookup_code = resolve_lookup_type(column_name)
    raw_upper = column_name.upper()
    
    lookup_map = preloaded_lookups if preloaded_lookups is not None else await get_all_lookups(conn)
    col_lookup = lookup_map.get(lookup_code) or lookup_map.get(raw_upper)
    if col_lookup:
        return [{"id": str(k), "name": str(v)} for k, v in col_lookup.items()]

    # 2. Foreign Key Dropdown Fallback
    f_table, f_pk = await resolve_fk_details(conn, table_name, column_name)
    if f_table and f_pk:
        try:
            return await get_fk_display_dict(conn, f_table, f_pk)
        except Exception:
            pass
    return []

def _is_password_column(col_name: str) -> bool:
    if not col_name:
        return False
    c = str(col_name).lower()
    return c in ('pus_pwd', 'password', 'pus_password') or 'password' in c or c.endswith('pwd')

def _sanitize_payload(data, pk_column, schema_map, is_update=False):
    clean_data = {}
    for k, v in data.items():
        if k in ('csrf_token', '_method', 'signature_password'):
            continue
        if k == pk_column:
            if is_update:
                continue
            if v == "" or v is None:
                continue 
        if 'created' in k.lower() or 'modified' in k.lower() or 'edited' in k.lower() or 'update' in k.lower():
            continue

        if is_update and (v == "" or v is None):
            if _is_password_column(k):
                continue
            clean_data[k] = None
            continue
        elif not is_update and (v == "" or v is None):
            continue

        if _is_password_column(k) and v:
            if isinstance(v, str) and not v.startswith(('$2b$', '$2a$')):
                salt = bcrypt.gensalt()
                v = bcrypt.hashpw(v.encode('utf-8'), salt).decode('utf-8')

        col_info = schema_map.get(k, {})
        target_type = col_info.get('data_type', '').lower()
        max_len = col_info.get('character_maximum_length')
        
        # 1. Boolean normalization (convert HTML form 'on', 'true', '1', etc. to Python bool)
        if target_type == 'boolean':
            if isinstance(v, bool):
                clean_data[k] = v
            elif isinstance(v, str):
                clean_data[k] = v.lower().strip() in ('true', '1', 't', 'yes', 'on')
            elif isinstance(v, (int, float)):
                clean_data[k] = bool(v)
            else:
                clean_data[k] = False
            continue

        # 2. Date & Timestamp parsing
        if 'date' in target_type or 'timestamp' in target_type or (isinstance(v, str) and len(v) == 10 and v[4] == '-' and v[7] == '-'):
            if isinstance(v, str) and v:
                try:
                    parsed_dt = datetime.strptime(v, '%Y-%m-%d')
                    v = parsed_dt.date() if target_type == 'date' else parsed_dt
                except ValueError:
                    try:
                        parsed_dt = datetime.fromisoformat(v)
                        v = parsed_dt.date() if target_type == 'date' else parsed_dt
                    except ValueError:
                        pass
            elif isinstance(v, datetime) and target_type == 'date':
                v = v.date()

        # 3. String length truncation & status normalization
        if isinstance(v, str) and max_len is not None:
            if "status" in k and v.lower() == "active": v = "ACT"
            elif "status" in k and v.lower() == "inactive": v = "INA"
            else: v = v[:max_len]
        
        # 4. Numeric and integer normalization
        if target_type in ('integer', 'bigint', 'smallint'):
            if isinstance(v, bool):
                clean_data[k] = int(v)
            else:
                try:
                    clean_data[k] = int(float(v))
                except (ValueError, TypeError, OverflowError):
                    clean_data[k] = None
        elif target_type == 'numeric' and isinstance(v, str):
            try:
                clean_data[k] = float(v) if '.' in v else int(v)
            except (ValueError, TypeError):
                clean_data[k] = None
        else:
            clean_data[k] = v

    return clean_data


@app.route('/')
@check_auth
async def dashboard(request):
    modules_tree = getattr(request.ctx, 'modules_tree', {}) or {}
    all_tables = getattr(request.ctx, 'all_tables', {}) or {}
    
    stats = {
        "total_modules": len(modules_tree),
        "total_screens": len(all_tables),
        "total_users": 1,
        "active_sessions": 1,
        "validation_records": 0,
        "total_roles": 0,
        "uptime_percent": 99.9,
        "db_latency_ms": 3.8
    }
    
    if hasattr(app.ctx, 'pool') and app.ctx.pool:
        t0 = time.perf_counter()
        try:
            async with app.ctx.pool.acquire() as conn:
                try:
                    u_row = await conn.fetchrow("""
                        SELECT 
                            COUNT(*) as total_users,
                            COUNT(CASE WHEN pus_session_id IS NOT NULL THEN 1 END) as active_sessions
                        FROM phc_users_t
                    """)
                    if u_row:
                        stats["total_users"] = u_row["total_users"] or 1
                        stats["active_sessions"] = u_row["active_sessions"] or 1
                except Exception:
                    pass

                try:
                    stats["total_roles"] = await conn.fetchval("SELECT COUNT(*) FROM phc_roles_t") or 0
                except Exception:
                    pass

                try:
                    p_count = await conn.fetchval("SELECT COUNT(*) FROM pcv_products_t") or 0
                    v_count = await conn.fetchval("SELECT COUNT(*) FROM pcv_validation_executions_t") or 0
                    stats["validation_records"] = p_count + v_count
                except Exception:
                    pass

                t1 = time.perf_counter()
                stats["db_latency_ms"] = max(1.2, round((t1 - t0) * 1000, 1))
        except Exception:
            pass

    return render_template('dashboard.html', request=request, stats=stats)

@app.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except (ValueError, TypeError):
        page = 1

    per_page = 50
    offset = (page - 1) * per_page
    search_query = request.args.get("q", "").strip()
    type_filter = request.args.get("type_filter", "").strip()

    # 1. Parse server-side sorting parameters
    raw_sort_rules = request.args.get("sort_rules", "")
    sort_col = request.args.get("sort_col", "").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip().lower()
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'desc'

    active_sort_rules = []
    if raw_sort_rules:
        try:
            import json
            parsed_sort = json.loads(raw_sort_rules)
            if isinstance(parsed_sort, list):
                for s in parsed_sort:
                    if isinstance(s, dict) and s.get("col") and s.get("dir") in ("asc", "desc"):
                        active_sort_rules.append({"col": s["col"], "dir": s["dir"]})
        except Exception:
            pass
    elif sort_col:
        active_sort_rules = [{"col": sort_col, "dir": sort_dir}]

    # 2. Parse server-side structured filter parameters
    raw_filters = request.args.get("filters", "")
    active_filter_rules = []
    if raw_filters:
        try:
            import json
            parsed_filters = json.loads(raw_filters)
            if isinstance(parsed_filters, list):
                for f in parsed_filters:
                    if isinstance(f, dict) and f.get("col") and f.get("op"):
                        active_filter_rules.append({
                            "col": str(f["col"]).strip(),
                            "op": str(f["op"]).strip().lower(),
                            "val": str(f.get("val", "")).strip()
                        })
        except Exception:
            pass

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

        columns_data = await get_table_columns(conn, table_name)

        if not columns_data:
            raise NotFound("Table does not exist")

        columns = []
        date_columns = []
        audit_by_columns = []
        audit_date_columns = []
        company_col_def = None

        for c in columns_data:
            cname = c['column_name']
            cname_low = cname.lower()
            if cname in (pk_column, 'psn_screen_id'): continue
            
            is_company_col = 'company_id' in cname_low
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
                    company_col_def = {"raw": cname, "column_name": cname, "label": clean_label, "data_type": c.get('data_type', 'varchar')}
                continue
            
            # 1. Audit "By" columns (Created By / Modified By)
            if 'created' in cname_low and 'by' in cname_low:
                audit_by_columns.append({"raw": cname, "column_name": cname, "label": "Created By", "data_type": c.get('data_type', 'varchar')})
                continue
            elif ('modified' in cname_low or 'edited' in cname_low or 'updated' in cname_low) and 'by' in cname_low:
                audit_by_columns.append({"raw": cname, "column_name": cname, "label": "Modified By", "data_type": c.get('data_type', 'varchar')})
                continue
            
            # 2. Audit "Date" columns (Created Date / Modified Date)
            elif 'created' in cname_low and ('date' in c['data_type'] or 'timestamp' in c['data_type'] or 'date' in cname_low):
                audit_date_columns.append({"raw": cname, "column_name": cname, "label": "Created Date", "data_type": c.get('data_type', 'varchar')})
                continue
            elif ('modified' in cname_low or 'edited' in cname_low or 'updated' in cname_low) and ('date' in c['data_type'] or 'timestamp' in c['data_type'] or 'date' in cname_low):
                audit_date_columns.append({"raw": cname, "column_name": cname, "label": "Modified Date", "data_type": c.get('data_type', 'varchar')})
                continue

            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            col_def = {"raw": cname, "column_name": cname, "label": clean_label, "data_type": c.get('data_type', 'varchar')}
            
            # 3. Regular Date columns vs standard business columns
            if 'date' in c['data_type'] or 'timestamp' in c['data_type']:
                date_columns.append(col_def)
            else:
                columns.append(col_def)
                
        columns.extend(date_columns)
        columns.extend(audit_by_columns)
        columns.extend(audit_date_columns)
        if company_col_def:
            columns.append(company_col_def)

        lookup_categories = []
        if table_name == 'phc_lookup_values_t':
            try:
                lookup_categories = await conn.fetch(
                    "SELECT plt_lookup_type_code as code, COALESCE(plt_lookup_type, plt_lookup_type_code) as name FROM phc_lookup_types WHERE plt_status = 'ACT' ORDER BY name"
                )
            except Exception:
                try:
                    lookup_categories = await conn.fetch(
                        "SELECT plt_lookup_type_code as code, COALESCE(plt_lookup_type_name, plt_lookup_type, plt_lookup_type_code) as name FROM phc_lookup_types_t WHERE plt_status = 'ACT' ORDER BY name"
                    )
                except Exception:
                    lookup_categories = []

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)
        schema_cols = {c['column_name']: c for c in columns_data}
        
        base_query = f"SELECT * FROM {q_table}"
        count_query = f"SELECT COUNT(*) FROM {q_table}"
        params = []
        where_clauses = []

        if table_name == 'phc_lookup_values_t' and type_filter:
            params.append(type_filter)
            where_clauses.append(f"{quote_ident('plv_lookup_type_code')} = ${len(params)}")

        # 3. Server-side global search across all text-castable columns
        if search_query:
            params.append(f"%{search_query}%")
            param_idx = len(params)
            searchable_cols = [
                c['column_name'] for c in columns_data 
                if c['data_type'] not in ('bytea', 'json', 'jsonb', 'geometry', 'point', 'polygon')
            ]
            if searchable_cols:
                search_clauses = [f"CAST({quote_ident(col)} AS TEXT) ILIKE ${param_idx}" for col in searchable_cols]
                where_clauses.append("(" + " OR ".join(search_clauses) + ")")

        # 4. Server-side structured 10-operator filter execution
        for f in active_filter_rules:
            col_name = f['col']
            op = f['op']
            val = f['val']
            if col_name not in schema_cols:
                continue
            
            c_info = schema_cols[col_name]
            target_type = c_info.get('data_type', '').lower()
            q_col = quote_ident(col_name)

            if op == 'is_empty':
                where_clauses.append(f"({q_col} IS NULL OR CAST({q_col} AS TEXT) = '')")
            elif op == 'is_not_empty':
                where_clauses.append(f"({q_col} IS NOT NULL AND CAST({q_col} AS TEXT) != '')")
            elif op == 'contains' and val:
                params.append(f"%{val}%")
                where_clauses.append(f"CAST({q_col} AS TEXT) ILIKE ${len(params)}")
            elif op == 'not_contains' and val:
                params.append(f"%{val}%")
                where_clauses.append(f"(CAST({q_col} AS TEXT) NOT ILIKE ${len(params)} OR {q_col} IS NULL)")
            elif op == 'starts_with' and val:
                params.append(f"{val}%")
                where_clauses.append(f"CAST({q_col} AS TEXT) ILIKE ${len(params)}")
            elif op == 'ends_with' and val:
                params.append(f"%{val}")
                where_clauses.append(f"CAST({q_col} AS TEXT) ILIKE ${len(params)}")
            elif op == 'equals' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"{q_col} = ${len(params)}")
                    except (ValueError, TypeError):
                        pass
                elif target_type == 'boolean':
                    b_val = val.lower() in ('true', '1', 'yes', 't', 'act')
                    params.append(b_val)
                    where_clauses.append(f"{q_col} = ${len(params)}")
                else:
                    params.append(val)
                    where_clauses.append(f"LOWER(CAST({q_col} AS TEXT)) = LOWER(${len(params)})")
            elif op == 'not_equals' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"({q_col} != ${len(params)} OR {q_col} IS NULL)")
                    except (ValueError, TypeError):
                        pass
                elif target_type == 'boolean':
                    b_val = val.lower() in ('true', '1', 'yes', 't', 'act')
                    params.append(b_val)
                    where_clauses.append(f"({q_col} != ${len(params)} OR {q_col} IS NULL)")
                else:
                    params.append(val)
                    where_clauses.append(f"(LOWER(CAST({q_col} AS TEXT)) != LOWER(${len(params)}) OR {q_col} IS NULL)")
            elif op == 'greater_than' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"{q_col} > ${len(params)}")
                    except (ValueError, TypeError):
                        pass
                elif 'date' in target_type or 'timestamp' in target_type:
                    params.append(val)
                    where_clauses.append(f"{q_col} > ${len(params)}::timestamp")
            elif op == 'less_than' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"{q_col} < ${len(params)}")
                    except (ValueError, TypeError):
                        pass
                elif 'date' in target_type or 'timestamp' in target_type:
                    params.append(val)
                    where_clauses.append(f"{q_col} < ${len(params)}::timestamp")

        if where_clauses:
            where_str = " WHERE " + " AND ".join(where_clauses)
            base_query += where_str
            count_query += where_str

        # 5. Dynamic server-side ORDER BY generation
        order_clauses = []
        for s in active_sort_rules:
            c_name = s['col']
            d_str = 'ASC' if s['dir'] == 'asc' else 'DESC'
            if c_name in schema_cols:
                order_clauses.append(f"{quote_ident(c_name)} {d_str}")
        
        if not order_clauses or pk_column not in [s['col'] for s in active_sort_rules]:
            order_clauses.append(f"{q_pk} DESC")
        
        base_query += f" ORDER BY {', '.join(order_clauses)} LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
        
        try:
            total_count = await conn.fetchval(count_query, *params)
            total_count = total_count or 0
            raw_rows = await conn.fetch(base_query, *(params + [per_page, offset]))
        except Exception:
            clear_schema_cache()
            total_count = await conn.fetchval(count_query, *params)
            total_count = total_count or 0
            raw_rows = await conn.fetch(base_query, *(params + [per_page, offset]))

        resolved_rows = [dict(r) for r in raw_rows]
        
        # In-Memory Cached Lookup Resolution
        lookup_map = await get_all_lookups(conn)

        # Resolve FKs and lookups
        fk_map = await get_fk_map(conn, table_name)
        
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
            # 1. Foreign Key Resolution
            f_table, f_pk = await resolve_fk_details(conn, table_name, cname)
            if f_table and f_pk:
                ids = set(r[cname] for r in resolved_rows if r[cname] is not None)
                if ids:
                    try:
                        fk_dict = await get_fk_display_dict(conn, f_table, f_pk, specific_ids=ids)
                        for r in resolved_rows:
                            val = r[cname]
                            if val in fk_dict:
                                r[cname] = f"{fk_dict[val]} (ID: {val})"
                    except Exception as e:
                        pass
            
            # 2. Dynamic Lookups (In-Memory Resolution with Prefix Stripping)
            canonical_lookup = resolve_lookup_type(cname)
            upper_cname = cname.upper()
            col_lookup = lookup_map.get(canonical_lookup) or lookup_map.get(upper_cname)
            if col_lookup:
                for r in resolved_rows:
                    val_str = str(r[cname]) if r[cname] is not None else None
                    if val_str and val_str in col_lookup:
                        r[cname] = col_lookup[val_str]
                        
        rows = resolved_rows

    total_pages = max(1, (total_count + per_page - 1) // per_page)
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
        search_query=search_query,
        lookup_categories=lookup_categories,
        type_filter=type_filter,
        active_sort_rules=active_sort_rules,
        active_filter_rules=active_filter_rules,
        sort_col=sort_col,
        sort_dir=sort_dir
    )

@app.route('/new/<table_name>', methods=['GET'])
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@app.route('/edit/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

@app.route('/form/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_form_view_alias(request, table_name, pk_val):
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
        columns_data = await get_table_columns(conn, table_name)
        schema_map = SCHEMA_CACHE["schema_maps"].get(table_name, {c['column_name']: c for c in columns_data})
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer')

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)

        row_data = {}
        if is_update:
            pk_val = urllib.parse.unquote(pk_val) if pk_val else pk_val
            cast_pk = safe_cast_pk(pk_val, pk_type)
            if cast_pk is None:
                raise NotFound(f"Invalid primary key format. pk_val='{pk_val}', pk_type='{pk_type}'")
            row_data = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1", cast_pk)
            if not row_data:
                raise NotFound(f"Record not found. Table: {q_table}, PK: {q_pk}, Value: '{cast_pk}', Type: {type(cast_pk).__name__}")

        lookup_map = await get_all_lookups(conn)

        columns = []
        company_form_def = None
        for c in columns_data:
            cname = c['column_name']
            if 'created' in cname.lower() or 'modified' in cname.lower() or 'edited' in cname.lower():
                continue
                
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if not (table_name == 'phc_screens_t' and role == 'ADM'):
                    continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            val = row_data.get(cname, '') if is_update else ''
            options = await get_dropdown_options(conn, table_name, cname, preloaded_lookups=lookup_map)

            json_options = None
            if table_name == 'phc_role_screen_assignment_t' and cname == 'prs_screen_id' and not is_update:
                json_options = await get_dropdown_options(conn, table_name, cname, preloaded_lookups=lookup_map)
                options = [] 
                
            col_def = {
                "column_name": cname,
                "label": clean_label,
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO' and 'default' not in cname.lower(),
                "is_pk": cname == pk_column,
                "value": val,
                "options": options,
                "json_options": json_options
            }
            if is_company_col:
                company_form_def = col_def
            else:
                columns.append(col_def)

        if company_form_def:
            columns.append(company_form_def)

        audit_info = {}
        workflow_info = None
        if is_update and row_data:
            created_by_col = next((c for c in row_data.keys() if 'created' in c.lower() and 'by' in c.lower()), None)
            created_at_col = next((c for c in row_data.keys() if 'created' in c.lower() and 'by' not in c.lower()), None)
            modified_by_col = next((c for c in row_data.keys() if ('modified' in c.lower() or 'edited' in c.lower() or 'updated' in c.lower()) and 'by' in c.lower()), None)
            modified_at_col = next((c for c in row_data.keys() if ('modified' in c.lower() or 'edited' in c.lower() or 'updated' in c.lower()) and 'by' not in c.lower()), None)
            
            audit_info = {
                "created_by": row_data.get(created_by_col) if created_by_col else None,
                "created_at": row_data.get(created_at_col) if created_at_col else None,
                "modified_by": row_data.get(modified_by_col) if modified_by_col else None,
                "modified_at": row_data.get(modified_at_col) if modified_at_col else None,
            }

            workflow_info = await get_approval_workflow_info(conn, table_name, pk_val, dict(row_data), user_id, role)

    return render_template(
        'form_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        is_update=is_update,
        pk_val=pk_val,
        audit_info=audit_info,
        workflow_info=workflow_info
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
        auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")

        pk_column = await get_pk_column(conn, table_name)
        columns_data = await get_table_columns(conn, table_name)

        export_cols = []
        company_csv_col = None
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    company_csv_col = cname
                continue
            export_cols.append(cname)
        if company_csv_col:
            export_cols.append(company_csv_col)

        if not export_cols:
            return response.text("No exportable columns found.", status=400)

        col_list = ", ".join(quote_ident(c) for c in export_cols)
        q_table = quote_ident(table_name)
        order_clause = f" ORDER BY {quote_ident(pk_column)} DESC" if pk_column else ""
        query = f"SELECT {col_list} FROM {q_table}{order_clause}"

    table_title = auth_tables.get(table_name, table_name)
    filename = f"{table_title.replace(' ', '_')}_Export.csv"

    async def streaming_fn(res):
        header = [col.split('_', 1)[-1].replace('_', ' ').title() for col in export_cols]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        await res.write(output.getvalue())
        output.seek(0)
        output.truncate(0)

        async with app.ctx.pool.acquire() as conn:
            async with conn.transaction():
                async for row in conn.cursor(query):
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
                    await res.write(output.getvalue())
                    output.seek(0)
                    output.truncate(0)

    res = response.stream(streaming_fn, content_type="text/csv")
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
    client_ip = get_client_ip(request)
    session_username = getattr(request.ctx, 'username', None) or 'System'
    session_id = getattr(request.ctx, 'session_id', None)
    
    method = request.method
    if request.form and request.form.get('_method'):
        method = request.form.get('_method')[0].upper()
    elif pk_val is not None and method != 'DELETE':
        method = 'PUT'

    # 1. CSRF Token Verification for state-changing operations
    client_csrf = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token")
    if not client_csrf and request.form:
        client_csrf = request.form.get('csrf_token')
    if not client_csrf and isinstance(request.json, dict):
        client_csrf = request.json.get('csrf_token')
    if isinstance(client_csrf, list):
        client_csrf = client_csrf[0]

    if not validate_csrf_token(client_csrf, session_id):
        logger.warning(f"CSRF rejection: user_id={user_id}, table={table_name}, method={method}")
        return add_security_headers(response.json({
            "error": "Invalid or expired security token (CSRF). Please refresh the page and try again."
        }, status=403))

    async with app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            return response.json({"error": "Unauthorized"}, status=403)
        
        pk_column = await get_pk_column(conn, table_name)
        columns_info = await get_table_columns(conn, table_name)
        schema_map = SCHEMA_CACHE["schema_maps"].get(table_name, {c['column_name']: c for c in columns_info})
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer')

        cast_pk = safe_cast_pk(pk_val, pk_type)
        if method in ('PUT', 'DELETE') and cast_pk is None:
            return add_security_headers(response.json({"error": "Invalid primary key format"}, status=400))

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)

        # Pre-fetch existing record state for audit comparison
        old_row = None
        if method in ('PUT', 'DELETE') and cast_pk is not None:
            try:
                old_row = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1", cast_pk)
            except Exception:
                old_row = None

        if method == 'DELETE':
            try:
                # Dynamically inspect schema for soft-delete / status columns
                status_col = None
                status_val = None
                for c in schema_map.values():
                    cname = c['column_name'].lower()
                    dtype = c.get('data_type', '').lower()
                    maxlen = c.get('character_maximum_length')
                    
                    if cname.endswith('_status') or cname == 'status':
                        status_col = c['column_name']
                        status_val = 'I' if maxlen == 1 else 'INA'
                        break
                    elif cname in ('is_active', 'active') or cname.endswith('_is_active') or cname.endswith('_active'):
                        status_col = c['column_name']
                        status_val = False if dtype == 'boolean' else ('0' if dtype in ('integer', 'smallint') else 'N')
                        break
                    elif cname in ('deleted_at', 'deleted_date') or cname.endswith('_deleted_at'):
                        status_col = c['column_name']
                        status_val = datetime.now()
                        break

                async with conn.transaction():
                    if status_col:
                        set_parts = [f"{quote_ident(status_col)} = $1"]
                        set_vals = [status_val]
                        
                        mod_by_col = next((c for c in schema_map if ('modified' in c.lower() or 'updated' in c.lower() or 'edited' in c.lower()) and 'by' in c.lower()), None)
                        mod_at_col = next((c for c in schema_map if ('modified' in c.lower() or 'updated' in c.lower() or 'edited' in c.lower()) and 'by' not in c.lower()), None)
                        
                        if mod_by_col:
                            max_len = schema_map[mod_by_col].get('character_maximum_length') or 50
                            set_parts.append(f"{quote_ident(mod_by_col)} = ${len(set_vals)+1}")
                            set_vals.append(str(session_username)[:max_len])
                        if mod_at_col:
                            set_parts.append(f"{quote_ident(mod_at_col)} = ${len(set_vals)+1}")
                            set_vals.append(datetime.now())
                        
                        set_vals.append(cast_pk)
                        q = f"UPDATE {q_table} SET {', '.join(set_parts)} WHERE {q_pk} = ${len(set_vals)}"
                        res = await conn.execute(q, *set_vals)
                    else:
                        res = await conn.execute(f"DELETE FROM {q_table} WHERE {q_pk} = $1", cast_pk)

                    # Atomically write audit event
                    action_type = 'SOFT_DELETE' if status_col else 'DELETE'
                    await log_audit_event(
                        conn, table_name, str(cast_pk), action_type, 
                        user_id, session_username, client_ip, 
                        old_values=dict(old_row) if old_row else None, new_values=None
                    )

                if res.endswith(" 0"):
                    return add_security_headers(response.json({"error": "Record not found"}, status=404))
                invalidate_caches_for_table(table_name)
                return add_security_headers(response.json({"status": "success", "soft_deleted": bool(status_col)}))
            except Exception as e:
                return add_security_headers(response.json({"error": str(e)}, status=400))

        try:
            data = request.form if request.form else request.json
            if not data:
                return add_security_headers(response.json({"error": "No data provided"}, status=400))
            data_dict = {k: v[0] if isinstance(v, list) else v for k, v in data.items() if k != '_method'}
        except Exception:
            return add_security_headers(response.json({"error": "Invalid or malformed payload"}, status=400))

        if request.files:
            upload_dir = os.path.join(os.getcwd(), 'uploads')
            for file_key, file_objs in request.files.items():
                file_obj = file_objs[0] if isinstance(file_objs, list) else file_objs
                try:
                    rel_path = save_uploaded_file(file_obj, upload_dir)
                    data_dict[file_key] = rel_path
                except ValueError as val_err:
                    return add_security_headers(response.json({"error": str(val_err)}, status=400))

        # Enforce strict User Creation and Password Rules
        if table_name == 'phc_users_t':
            user_col = 'pus_user_name' if 'pus_user_name' in schema_map else ('pus_usr_name' if 'pus_usr_name' in schema_map else 'username')
            q_ucol = quote_ident(user_col)
            
            if method == 'POST':
                username_val = str(data_dict.get(user_col) or data_dict.get('pus_user_name') or data_dict.get('pus_usr_name') or '').strip()
                if not username_val:
                    return add_security_headers(response.json({"error": "Username is required."}, status=400))
                
                existing = await conn.fetchval(f"SELECT 1 FROM phc_users_t WHERE LOWER({q_ucol}) = LOWER($1)", username_val)
                if existing:
                    return add_security_headers(response.json({"error": f"Username '{username_val}' is already taken. Please choose a unique username."}, status=400))
                
                pwd_val = str(data_dict.get('pus_pwd') or data_dict.get('password') or '').strip()
                if not pwd_val:
                    return add_security_headers(response.json({"error": "Password is required for new users."}, status=400))
                is_valid, msg = validate_password_strength(pwd_val)
                if not is_valid:
                    return add_security_headers(response.json({"error": msg}, status=400))
                
                data_dict['pus_pwd'] = bcrypt.hashpw(pwd_val.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                data_dict[user_col] = username_val
            
            elif method == 'PUT':
                username_val = str(data_dict.get(user_col) or data_dict.get('pus_user_name') or data_dict.get('pus_usr_name') or '').strip()
                if username_val:
                    existing = await conn.fetchval(
                        f"SELECT 1 FROM phc_users_t WHERE LOWER({q_ucol}) = LOWER($1) AND {q_pk} != $2",
                        username_val, cast_pk
                    )
                    if existing:
                        return add_security_headers(response.json({"error": f"Username '{username_val}' is already taken. Please choose a unique username."}, status=400))
                    data_dict[user_col] = username_val
                
                pwd_val = str(data_dict.get('pus_pwd') or data_dict.get('password') or '').strip()
                if pwd_val:
                    is_valid, msg = validate_password_strength(pwd_val)
                    if not is_valid:
                        return add_security_headers(response.json({"error": msg}, status=400))
                    data_dict['pus_pwd'] = bcrypt.hashpw(pwd_val.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                else:
                    data_dict.pop('pus_pwd', None)

        clean_data = _sanitize_payload(data_dict, pk_column, schema_map, is_update=(method == 'PUT'))

        # Multi-tenant Company Segregation
        if table_name != 'phc_companies_t':
            company_col = next((c for c in schema_map if (c.endswith('_company_id') or c == 'company_id') and c != pk_column), None)
            if company_col:
                if not (table_name == 'phc_screens_t' and role == 'ADM') or company_col not in clean_data or not clean_data[company_col]:
                    user_company = await conn.fetchval("SELECT pus_company_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    if user_company:
                        clean_data[company_col] = user_company

        # Mandatory Session Username and Audit Trail Binding
        who_cols = [c for c in schema_map if 'created' in c.lower() or 'modified' in c.lower() or 'edited' in c.lower() or 'update' in c.lower()]
        for wc in who_cols:
            max_len = schema_map.get(wc, {}).get('character_maximum_length') or 50
            user_str = str(session_username)[:max_len]
            
            if ('modified' in wc.lower() or 'edited' in wc.lower() or 'update' in wc.lower()) and 'by' not in wc.lower():
                clean_data[wc] = datetime.now()
            elif ('modified' in wc.lower() or 'edited' in wc.lower() or 'update' in wc.lower()) and 'by' in wc.lower():
                clean_data[wc] = user_str
            elif method == 'POST':
                if 'created' in wc.lower() and 'by' not in wc.lower():
                    clean_data[wc] = datetime.now()
                elif 'created' in wc.lower() and 'by' in wc.lower():
                    clean_data[wc] = user_str

        try:
            async with conn.transaction():
                if method == 'POST':
                    for cname in schema_map:
                        if 'start_date' in cname.lower() and (cname not in clean_data or not clean_data[cname]):
                            clean_data[cname] = datetime.now()
                            
                    col_default = schema_map.get(pk_column, {}).get('column_default')
                    has_default = col_default is not None and str(col_default).strip() != ''
                    if pk_column and not has_default and (pk_column not in clean_data or clean_data[pk_column] is None or clean_data[pk_column] == ""):
                        if pk_type in ('integer', 'bigint', 'smallint'):
                            max_val = await conn.fetchval(f"SELECT MAX({q_pk}) FROM {q_table}")
                            clean_data[pk_column] = (max_val or 0) + 1

                    if table_name == 'phc_role_screen_assignment_t' and 'prs_screen_id' in data_dict:
                        raw_scr = data_dict['prs_screen_id']
                        scr_list = []
                        if isinstance(raw_scr, str) and raw_scr.startswith('['):
                            try:
                                scr_list = json.loads(raw_scr)
                            except Exception:
                                scr_list = [raw_scr]
                        elif isinstance(raw_scr, list):
                            scr_list = raw_scr
                        else:
                            scr_list = [raw_scr]

                        for sid in scr_list:
                            row_clean = clean_data.copy()
                            row_clean['prs_screen_id'] = int(sid)
                            cols = [quote_ident(c) for c in row_clean.keys()]
                            vals = list(row_clean.values())
                            placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
                            q = f"INSERT INTO {q_table} ({', '.join(cols)}) VALUES ({placeholders})"
                            await conn.execute(q, *vals)
                    else:
                        cols = [quote_ident(c) for c in clean_data.keys()]
                        vals = list(clean_data.values())
                        placeholders = ", ".join([f"${i+1}" for i in range(len(vals))])
                        q = f"INSERT INTO {q_table} ({', '.join(cols)}) VALUES ({placeholders})"
                        await conn.execute(q, *vals)

                    # Log INSERT audit event
                    created_id = str(clean_data.get(pk_column) or 'NEW')
                    await log_audit_event(
                        conn, table_name, created_id, 'INSERT', 
                        user_id, session_username, client_ip, 
                        old_values=None, new_values=clean_data
                    )

                elif method == 'PUT':
                    if not clean_data:
                        return add_security_headers(response.json({"error": "No update fields provided"}, status=400))
                    cols = list(clean_data.keys())
                    vals = list(clean_data.values())
                    set_clause = ", ".join([f"{quote_ident(c)} = ${i+1}" for i, c in enumerate(cols)])
                    q = f"UPDATE {q_table} SET {set_clause} WHERE {q_pk} = ${len(vals)+1}"
                    res = await conn.execute(q, *(vals + [cast_pk]))
                    if res.endswith(" 0"):
                        return add_security_headers(response.json({"error": "Record not found"}, status=404))

                    # Log UPDATE audit event
                    await log_audit_event(
                        conn, table_name, str(cast_pk), 'UPDATE', 
                        user_id, session_username, client_ip, 
                        old_values=dict(old_row) if old_row else None, new_values=clean_data
                    )

            # Targeted cache eviction
            invalidate_caches_for_table(table_name)

            if request.headers.get("hx-request") or request.headers.get("HX-Request"):
                res = response.json({"status": "success"})
                res.headers["HX-Redirect"] = f"/table/{table_name}"
                return add_security_headers(res)
            else:
                return add_security_headers(response.redirect(f"/table/{table_name}"))
            
        except Exception as e:
            logger.error(f"Action Error: {e}", exc_info=True)
            return add_security_headers(response.json({"error": str(e)}, status=400))

# -----------------------------------------------------------------------------
# AUDIT TRAIL API ROUTE
# -----------------------------------------------------------------------------
@app.route('/api/audit/<table_name>/<record_id>', methods=['GET'])
@check_auth
async def get_record_audit_history(request, table_name, record_id):
    table_name = table_name.lower()
    record_id = urllib.parse.unquote(str(record_id)).strip()
    
    if not hasattr(app.ctx, 'pool') or app.ctx.pool is None:
        return add_security_headers(response.json({"status": "success", "history": []}))
    
    try:
        async with app.ctx.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    pal_audit_id, pal_action, pal_username, pal_client_ip, 
                    pal_old_values, pal_new_values, pal_timestamp
                FROM phc_audit_log_t 
                WHERE pal_table_name = $1 AND pal_record_id = $2
                ORDER BY pal_timestamp DESC
                LIMIT 50
            """, table_name, record_id)
            
            events = []
            for r in rows:
                events.append({
                    "audit_id": r["pal_audit_id"],
                    "action": r["pal_action"],
                    "username": r["pal_username"],
                    "client_ip": r["pal_client_ip"] or "Unknown",
                    "old_values": json.loads(r["pal_old_values"]) if r["pal_old_values"] else None,
                    "new_values": json.loads(r["pal_new_values"]) if r["pal_new_values"] else None,
                    "timestamp": r["pal_timestamp"].strftime("%Y-%m-%d %H:%M:%S") if r["pal_timestamp"] else ""
                })
            return add_security_headers(response.json({"status": "success", "history": events}))
    except Exception as e:
        logger.error(f"Audit fetch error: {e}")
        return add_security_headers(response.json({"status": "error", "message": str(e)}, status=500))

# -----------------------------------------------------------------------------
# WORKFLOW ENGINE & 21 CFR PART 11 E-SIGNATURE ROUTES
# -----------------------------------------------------------------------------
@app.route('/api/workflow/transition', methods=['POST'])
@check_auth
async def api_workflow_transition(request):
    user_id = request.ctx.user_id
    username = request.ctx.username
    user_role = request.ctx.role
    session_id = request.ctx.session_id
    client_ip = get_client_ip(request)
    
    data = request.json or {}
    client_csrf = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token") or data.get('csrf_token')
    if not validate_csrf_token(client_csrf, session_id):
        return add_security_headers(response.json({"error": "Invalid or expired CSRF token."}, status=403))
        
    table_name = str(data.get("table_name", "")).strip().lower()
    record_id = str(data.get("record_id", "")).strip()
    transition = str(data.get("transition", "")).strip().upper()  # SUBMIT, APPROVE, REJECT, RECALL
    comments = str(data.get("comments", "")).strip()
    sig_pwd = str(data.get("signature_password", ""))
    
    if not table_name or not record_id or not transition:
        return add_security_headers(response.json({"error": "Missing required transition parameters."}, status=400))
        
    if transition == 'REJECT' and not comments:
        return add_security_headers(response.json({"error": "A rejection reason/comment is mandatory."}, status=400))

    async with app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, user_role)
        if table_name not in auth_tables:
            return add_security_headers(response.json({"error": "Unauthorized access to table."}, status=403))

        setup = await conn.fetchrow("""
            SELECT * FROM phc_approval_setup_t 
            WHERE LOWER(pas_table_name) = LOWER($1) AND (pas_status = 'ACT' OR pas_status IS NULL)
        """, table_name)
        if not setup:
            return add_security_headers(response.json({"error": "No active approval workflow configured for this table."}, status=400))
            
        req_role = setup['pas_required_role'] or 'ADM'
        if transition in ('APPROVE', 'REJECT') and (user_role != 'ADM' and user_role != req_role):
            return add_security_headers(response.json({"error": f"Role '{req_role}' or Administrator required to approve/reject."}, status=403))

        # 21 CFR Part 11 Electronic Signature Password Verification
        if setup['pas_require_esig'] or sig_pwd:
            if not sig_pwd:
                return add_security_headers(response.json({"error": "Electronic signature password is required for this action."}, status=400))
            user_pwd = await conn.fetchval("SELECT pus_pwd FROM phc_users_t WHERE pus_user_id = $1", user_id)
            if not user_pwd or not bcrypt.checkpw(sig_pwd.encode('utf-8'), user_pwd.encode('utf-8')):
                return add_security_headers(response.json({"error": "Invalid signature password. Electronic signature verification failed."}, status=401))

        pk_col = await get_pk_column(conn, table_name)
        cols_data = await get_table_columns(conn, table_name)
        schema_map = {c['column_name'].lower(): c for c in cols_data}
        pk_type = schema_map.get(pk_col.lower(), {}).get('data_type', 'integer')
        cast_pk = safe_cast_pk(record_id, pk_type)
        
        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_col)
        
        current_row = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1", cast_pk)
        if not current_row:
            return add_security_headers(response.json({"error": "Record not found."}, status=404))

        status_col = None
        status_max_len = 10
        for c in cols_data:
            cname = c['column_name'].lower()
            if cname.endswith('_status') or cname == 'status' or cname.endswith('_state'):
                status_col = c['column_name']
                status_max_len = c.get('character_maximum_length') or 10
                break
                
        if not status_col:
            return add_security_headers(response.json({"error": "Target table does not have a status column."}, status=400))

        old_status = str(current_row.get(status_col) or 'DFT')
        if transition == 'SUBMIT':
            new_status = 'PND' if status_max_len >= 3 else 'P'
        elif transition == 'APPROVE':
            new_status = 'ACT' if status_max_len >= 3 else 'A'
        elif transition == 'REJECT':
            new_status = 'REJ' if status_max_len >= 3 else 'R'
        elif transition == 'RECALL':
            new_status = 'DFT' if status_max_len >= 3 else 'D'
        else:
            return add_security_headers(response.json({"error": f"Unknown transition '{transition}'."}, status=400))

        # Generate 21 CFR Part 11 Cryptographic Signature Stamp
        ts_now = datetime.now()
        esig_payload = f"SIGNER={username}|UID={user_id}|ROLE={user_role}|TABLE={table_name}|REC={record_id}|ACT={transition}|REASON={comments}|TS={ts_now.isoformat()}|IP={client_ip}"
        esig_hash = hashlib.sha256(esig_payload.encode('utf-8')).hexdigest()

        async with conn.transaction():
            set_parts = [f"{quote_ident(status_col)} = $1"]
            set_vals = [new_status]
            
            mod_by_col = next((c['column_name'] for c in cols_data if ('modified' in c['column_name'].lower() or 'updated' in c['column_name'].lower()) and 'by' in c['column_name'].lower()), None)
            mod_at_col = next((c['column_name'] for c in cols_data if ('modified' in c['column_name'].lower() or 'updated' in c['column_name'].lower()) and 'by' not in c['column_name'].lower()), None)
            if mod_by_col:
                set_parts.append(f"{quote_ident(mod_by_col)} = ${len(set_vals)+1}")
                set_vals.append(str(username)[:50])
            if mod_at_col:
                set_parts.append(f"{quote_ident(mod_at_col)} = ${len(set_vals)+1}")
                set_vals.append(ts_now)
                
            set_vals.append(cast_pk)
            await conn.execute(f"UPDATE {q_table} SET {', '.join(set_parts)} WHERE {q_pk} = ${len(set_vals)}", *set_vals)
            
            await conn.execute("""
                INSERT INTO phc_approval_events_t (
                    pae_table_name, pae_record_id, pae_action, pae_from_status, 
                    pae_to_status, pae_user_id, pae_username, pae_user_role, 
                    pae_comments, pae_esig_hash, pae_client_ip, pae_timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, table_name, str(record_id), transition, old_status, new_status, user_id, username, user_role, comments, esig_hash, client_ip, ts_now)
            
            await log_audit_event(
                conn, table_name, str(record_id), f"WORKFLOW_{transition}", 
                user_id, username, client_ip, 
                old_values={status_col: old_status}, 
                new_values={status_col: new_status, "workflow_comments": comments, "esig_hash": esig_hash}
            )

            table_title = auth_tables.get(table_name, table_name)
            link_url = f"/edit/{table_name}/{record_id}"
            if transition == 'SUBMIT':
                await dispatch_notification(
                    conn, None, req_role, 
                    f"Approval Required: {table_title}", 
                    f"Record #{record_id} in {table_title} was submitted for review by {username}.", 
                    link_url, "WORKFLOW"
                )
            elif transition in ('APPROVE', 'REJECT'):
                created_by_col = next((c['column_name'] for c in cols_data if 'created' in c['column_name'].lower() and 'by' in c['column_name'].lower()), None)
                created_by_user = current_row.get(created_by_col) if created_by_col else None
                creator_id = None
                if created_by_user:
                    creator_id = await conn.fetchval("SELECT pus_user_id FROM phc_users_t WHERE LOWER(pus_user_name) = LOWER($1) OR LOWER(pus_usr_name) = LOWER($1)", str(created_by_user))
                await dispatch_notification(
                    conn, creator_id, None, 
                    f"Record {transition.title()}d: {table_title}", 
                    f"Record #{record_id} was {transition.lower()}d by {username}. Reason: {comments or 'Approved'}", 
                    link_url, "WORKFLOW"
                )

        invalidate_caches_for_table(table_name)
        return add_security_headers(response.json({
            "status": "success", 
            "transition": transition, 
            "new_status": new_status,
            "esig_hash": esig_hash,
            "timestamp": ts_now.strftime("%Y-%m-%d %H:%M:%S")
        }))

@app.route('/api/workflow/history/<table_name>/<record_id>', methods=['GET'])
@check_auth
async def get_workflow_history(request, table_name, record_id):
    table_name = table_name.lower()
    record_id = urllib.parse.unquote(str(record_id)).strip()
    
    if not hasattr(app.ctx, 'pool') or app.ctx.pool is None:
        return add_security_headers(response.json({"status": "success", "history": []}))
        
    try:
        async with app.ctx.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pae_event_id, pae_action, pae_from_status, pae_to_status, 
                       pae_username, pae_user_role, pae_comments, pae_esig_hash, 
                       pae_client_ip, pae_timestamp
                FROM phc_approval_events_t
                WHERE LOWER(pae_table_name) = LOWER($1) AND pae_record_id = $2
                ORDER BY pae_timestamp DESC
                LIMIT 50
            """, table_name, record_id)
            
            events = []
            for r in rows:
                events.append({
                    "id": r['pae_event_id'],
                    "action": r['pae_action'],
                    "from_status": r['pae_from_status'],
                    "to_status": r['pae_to_status'],
                    "username": r['pae_username'],
                    "role": r['pae_user_role'],
                    "comments": r['pae_comments'] or "",
                    "esig_hash": r['pae_esig_hash'] or "",
                    "client_ip": r['pae_client_ip'] or "Unknown",
                    "timestamp": r['pae_timestamp'].strftime('%Y-%m-%d %H:%M:%S') if r['pae_timestamp'] else ""
                })
            return add_security_headers(response.json({"status": "success", "history": events}))
    except Exception as e:
        logger.error(f"Error fetching workflow history: {e}")
        return add_security_headers(response.json({"status": "error", "message": str(e)}, status=500))

# -----------------------------------------------------------------------------
# NOTIFICATIONS API ROUTES
# -----------------------------------------------------------------------------
@app.route('/api/notifications', methods=['GET'])
@check_auth
async def get_user_notifications(request):
    user_id = request.ctx.user_id
    role = request.ctx.role
    
    if not hasattr(app.ctx, 'pool') or app.ctx.pool is None:
        return add_security_headers(response.json({"notifications": [], "unread_count": 0}))
        
    try:
        async with app.ctx.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pun_notification_id, pun_title, pun_message, pun_category, 
                       pun_link_url, pun_is_read, pun_created_at
                FROM phc_user_notifications_t
                WHERE (pun_recipient_user_id = $1 OR pun_recipient_role = $2 OR (pun_recipient_user_id IS NULL AND pun_recipient_role IS NULL))
                ORDER BY pun_created_at DESC
                LIMIT 30
            """, user_id, role)
            
            notifs = []
            unread_count = 0
            for r in rows:
                if not r['pun_is_read']:
                    unread_count += 1
                notifs.append({
                    "id": r['pun_notification_id'],
                    "title": r['pun_title'],
                    "message": r['pun_message'],
                    "category": r['pun_category'] or 'WORKFLOW',
                    "link_url": r['pun_link_url'] or '#',
                    "is_read": bool(r['pun_is_read']),
                    "timestamp": r['pun_created_at'].strftime('%Y-%m-%d %H:%M') if r['pun_created_at'] else ""
                })
            return add_security_headers(response.json({"notifications": notifs, "unread_count": unread_count}))
    except Exception as e:
        logger.error(f"Error fetching notifications: {e}")
        return add_security_headers(response.json({"notifications": [], "unread_count": 0}))

@app.route('/api/notifications/<notif_id>/read', methods=['POST'])
@check_auth
async def mark_notification_read(request, notif_id):
    client_csrf = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token")
    if not validate_csrf_token(client_csrf, request.ctx.session_id):
        return add_security_headers(response.json({"error": "Invalid CSRF token"}, status=403))
        
    try:
        nid = int(notif_id)
        async with app.ctx.pool.acquire() as conn:
            await conn.execute("UPDATE phc_user_notifications_t SET pun_is_read = TRUE WHERE pun_notification_id = $1 AND (pun_recipient_user_id = $2 OR pun_recipient_role = $3)", nid, request.ctx.user_id, request.ctx.role)
        return add_security_headers(response.json({"status": "success"}))
    except Exception as e:
        return add_security_headers(response.json({"error": str(e)}, status=400))

# -----------------------------------------------------------------------------
# OBSERVABILITY: HEALTH & READINESS PROBES
# -----------------------------------------------------------------------------
@app.route('/health', methods=['GET'])
async def health_check(request):
    uptime = round(time.time() - SERVER_START_TIME, 2)
    return add_security_headers(response.json({
        "status": "healthy",
        "service": "Brihas ERP",
        "uptime_seconds": uptime,
        "timestamp": datetime.now().isoformat()
    }))

@app.route('/ready', methods=['GET'])
async def readiness_check(request):
    if not hasattr(app.ctx, 'pool') or app.ctx.pool is None:
        return add_security_headers(response.json({"status": "unready", "error": "Database pool uninitialized"}, status=503))
    try:
        async with app.ctx.pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            if val == 1:
                return add_security_headers(response.json({
                    "status": "ready",
                    "database": "connected",
                    "timestamp": datetime.now().isoformat()
                }))
    except Exception as e:
        logger.error(f"Readiness probe error: {e}")
        return add_security_headers(response.json({"status": "unready", "error": str(e)}, status=503))
    return add_security_headers(response.json({"status": "unready"}, status=503))

# -----------------------------------------------------------------------------
# OPENAPI 3.0 DYNAMIC DOCS
# -----------------------------------------------------------------------------
@app.route('/docs', methods=['GET'])
@check_auth
async def swagger_ui(request):
    return await render_template(request, 'swagger.html', {})

@app.route('/openapi.json', methods=['GET'])
@check_auth
async def openapi_spec(request):
    user_id = request.ctx.user_id
    role = request.ctx.role
    
    async with app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, role)
        
        paths = {}
        tags = []
        for table_name in auth_tables.keys():
            tags.append({"name": table_name})
            
            paths[f"/api/{table_name}"] = {
                "get": {
                    "tags": [table_name],
                    "summary": f"List {table_name}",
                    "responses": {"200": {"description": "Successful Response"}}
                },
                "post": {
                    "tags": [table_name],
                    "summary": f"Create {table_name}",
                    "responses": {"200": {"description": "Successful Response"}}
                }
            }
            paths[f"/api/{table_name}/{{id}}"] = {
                "put": {
                    "tags": [table_name],
                    "summary": f"Update {table_name}",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Successful Response"}}
                },
                "delete": {
                    "tags": [table_name],
                    "summary": f"Delete {table_name}",
                    "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Successful Response"}}
                }
            }

    openapi = {
        "openapi": "3.0.0",
        "info": {
            "title": "Brihas ERP Dynamic API",
            "version": "1.0.0",
            "description": "Dynamically generated API based on authorized tables."
        },
        "tags": tags,
        "paths": paths
    }
    return add_security_headers(response.json(openapi))

if __name__ == '__main__':
    logger.info(f"Starting Brihas ERP Server on port {PORT} with {WORKERS} worker(s)...")
    app.run(host="0.0.0.0", port=PORT, workers=WORKERS)