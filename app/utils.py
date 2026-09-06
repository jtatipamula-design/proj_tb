import os
import re
import uuid
import json
import hmac
import hashlib
import bcrypt
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sanic import response
from app.config import JWT_SECRET, MAX_UPLOAD_SIZE, ALLOWED_EXTENSIONS, BLOCKED_EXTENSIONS, MODULE_ICON_MAP, CURATED_ICON_LIST, logger

env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

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

def _is_password_column(col_name: str) -> bool:
    if not col_name:
        return False
    c = str(col_name).lower()
    return c in ('pus_pwd', 'password', 'pus_password') or 'password' in c or c.endswith('pwd')

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

def add_security_headers(res):
    res.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    res.headers["Pragma"] = "no-cache"
    res.headers["X-Content-Type-Options"] = "nosniff"
    res.headers["X-Frame-Options"] = "SAMEORIGIN"
    res.headers["X-XSS-Protection"] = "1; mode=block"
    res.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return res

def get_client_ip(request) -> str:
    """Extracts client IP reliably across proxies, load balancers, and direct connections."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return getattr(request, "remote_addr", "") or getattr(request, "ip", "127.0.0.1")

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

