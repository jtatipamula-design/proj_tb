import json
import bcrypt
import time
from datetime import datetime
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.config import logger, ADMIN_ONLY_TABLES, READ_ONLY_TABLES
from app.utils import quote_ident, add_security_headers, _sanitize_payload, get_client_ip, validate_uploaded_file, log_audit_event, dispatch_notification, validate_csrf_token, safe_cast_pk, validate_password_strength
from app.auth import check_auth, get_authorized_tables
from app.database import get_pk_column, get_table_columns, invalidate_caches_for_table, SCHEMA_CACHE
import urllib.parse
import hashlib

api_bp = Blueprint("api_routes")

_USER_COMPANY_CACHE = {}  # user_id -> (company_id, expires_at); a user's company rarely changes
_USER_COMPANY_TTL = 60

async def get_company_scope(conn, table_name, column_names, pk_column, user_id):
    """Return (company_col, company_id) to tenant-filter this table, or (None, None)."""
    if table_name == 'phc_companies_t':
        return None, None
    col = next((c for c in column_names if (c.endswith('_company_id') or c == 'company_id') and c != pk_column), None)
    if not col:
        return None, None
    cached = _USER_COMPANY_CACHE.get(user_id)
    if cached and cached[1] > time.monotonic():
        company = cached[0]
    else:
        company = await conn.fetchval("SELECT pus_company_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
        _USER_COMPANY_CACHE[user_id] = (company, time.monotonic() + _USER_COMPANY_TTL)
    return (col, company) if company is not None else (None, None)

def company_clause(scope, param_idx):
    """SQL fragment ' AND <col> = $n' plus its params for a scope from get_company_scope."""
    col, val = scope
    if not col:
        return "", []
    return f" AND {quote_ident(col)} = ${param_idx}", [val]

def write_block_reason(table_name, role, module_name):
    """Why this role may not write this table through the CRUD API, or None if allowed."""
    if table_name in READ_ONLY_TABLES:
        return "This table is an append-only record and cannot be edited."
    if role != 'ADM' and (table_name in ADMIN_ONLY_TABLES or (module_name or '').lower() == 'erpadmin'):
        return "Administrator role required to modify this table."
    return None

def insert_pk_missing(schema_map, pk_column, clean_data):
    """True when an insert gives no PK value and the database cannot generate one."""
    if clean_data.get(pk_column) not in (None, ""):
        return False
    info = schema_map.get(pk_column, {})
    return not str(info.get('column_default') or '').strip() and info.get('is_identity') != 'YES'

async def record_visible(conn, table_name, record_id, user_id, role=None):
    """True if the record exists and belongs to the user's company (or the table has no company column).
    A record that no longer exists (hard-deleted) stays visible to administrators so its history can be read."""
    pk_col = await get_pk_column(conn, table_name)
    if not pk_col:
        return False
    cols = await get_table_columns(conn, table_name)
    scope = await get_company_scope(conn, table_name, [c['column_name'] for c in cols], pk_col, user_id)
    if not scope[0]:
        return True
    pk_type = next((c.get('data_type', 'integer') for c in cols if c['column_name'] == pk_col), 'integer')
    cast_pk = safe_cast_pk(record_id, pk_type)
    if cast_pk is None:
        return False
    sc_sql, sc_vals = company_clause(scope, 2)
    q_table, q_pk = quote_ident(table_name), quote_ident(pk_col)
    if await conn.fetchval(f"SELECT 1 FROM {q_table} WHERE {q_pk} = $1{sc_sql}", cast_pk, *sc_vals):
        return True
    return role == 'ADM' and not await conn.fetchval(f"SELECT 1 FROM {q_table} WHERE {q_pk} = $1", cast_pk)

@api_bp.route('/api/<table_name>', methods=['POST'], name="api_create")
@check_auth
async def api_create_record(request, table_name):
    return await process_api_action(request, table_name, None)

@api_bp.route('/api/<table_name>/<pk_val>', methods=['PUT', 'DELETE', 'POST'], name="api_update_delete")
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
        _m = request.form.getlist('_method') if hasattr(request.form, 'getlist') else request.form.get('_method')
        if isinstance(_m, str):
            method = _m.upper()
        else:
            method = _m[0].upper() if _m else ''
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

    async with request.app.ctx.pool.acquire() as conn:
        auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            return response.json({"error": "Unauthorized"}, status=403)
        block_reason = write_block_reason(table_name, role, table_modules.get(table_name))
        if block_reason:
            return add_security_headers(response.json({"error": block_reason}, status=403))

        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            return add_security_headers(response.json({"error": "This table has no primary key and cannot be edited"}, status=400))
        columns_info = await get_table_columns(conn, table_name)
        schema_map = SCHEMA_CACHE["schema_maps"].get(table_name, {c['column_name']: c for c in columns_info})
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer')

        cast_pk = safe_cast_pk(pk_val, pk_type)
        if method in ('PUT', 'DELETE') and cast_pk is None:
            return add_security_headers(response.json({"error": "Invalid primary key format"}, status=400))

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)
        scope = await get_company_scope(conn, table_name, list(schema_map), pk_column, user_id)

        async def lock_old_row():
            """Current record state for the audit 'before' values, locked until the transaction ends."""
            sc_sql, sc_vals = company_clause(scope, 2)
            return await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1{sc_sql} FOR UPDATE", cast_pk, *sc_vals)

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
                    old_row = await lock_old_row()
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
                        sc_sql, sc_vals = company_clause(scope, len(set_vals) + 1)
                        q = f"UPDATE {q_table} SET {', '.join(set_parts)} WHERE {q_pk} = ${len(set_vals)}{sc_sql}"
                        res = await conn.execute(q, *set_vals, *sc_vals)
                    else:
                        sc_sql, sc_vals = company_clause(scope, 2)
                        res = await conn.execute(f"DELETE FROM {q_table} WHERE {q_pk} = $1{sc_sql}", cast_pk, *sc_vals)

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
            except Exception:
                logger.error(f"Delete error on {table_name}", exc_info=True)
                return add_security_headers(response.json({"error": "Could not delete the record."}, status=400))

        try:
            data = request.form if request.form else request.json
            if not data:
                return add_security_headers(response.json({"error": "No data provided"}, status=400))
            data_dict = {k: v[0] if isinstance(v, list) else v for k, v in data.items() if k != '_method'}
        except Exception:
            return add_security_headers(response.json({"error": "Invalid or malformed payload"}, status=400))

        if request.files:
            for file_key, file_objs in request.files.items():
                file_obj = file_objs[0] if isinstance(file_objs, list) else file_objs
                if not file_obj or not getattr(file_obj, 'name', '') or not getattr(file_obj, 'body', None):
                    continue

                col_info = schema_map.get(file_key, {})
                col_dt = col_info.get('data_type', '').lower()

                if col_dt not in ('bytea', 'blob', 'oid', 'binary', 'varbinary'):
                    return add_security_headers(response.json({"error": "Files can only be uploaded to file columns."}, status=400))
                try:
                    validate_uploaded_file(file_obj)
                except ValueError as val_err:
                    return add_security_headers(response.json({"error": str(val_err)}, status=400))
                data_dict[file_key] = bytes(file_obj.body)

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
        company_col, user_company = scope
        if company_col and not (table_name == 'phc_screens_t' and role == 'ADM' and clean_data.get(company_col)):
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

                    if insert_pk_missing(schema_map, pk_column, clean_data):
                        logger.error(f"Insert refused: {table_name}.{pk_column} has no default/identity and no value was given")
                        return add_security_headers(response.json({
                            "error": f"'{pk_column}' needs a value: this table does not generate IDs automatically. "
                                     "Ask an administrator to add an identity/sequence default for it in a migration."
                        }, status=400))

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
                        q = f"INSERT INTO {q_table} ({', '.join(cols)}) VALUES ({placeholders}) RETURNING {q_pk}"
                        new_pk = await conn.fetchval(q, *vals)
                        clean_data[pk_column] = new_pk

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
                    old_row = await lock_old_row()
                    cols = list(clean_data.keys())
                    vals = list(clean_data.values())
                    set_clause = ", ".join([f"{quote_ident(c)} = ${i+1}" for i, c in enumerate(cols)])
                    sc_sql, sc_vals = company_clause(scope, len(vals) + 2)
                    q = f"UPDATE {q_table} SET {set_clause} WHERE {q_pk} = ${len(vals)+1}{sc_sql}"
                    res = await conn.execute(q, *(vals + [cast_pk] + sc_vals))
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
            return add_security_headers(response.json({"error": "Could not save the record. Please check the values and try again."}, status=400))

# -----------------------------------------------------------------------------
# AUDIT TRAIL API ROUTE
# -----------------------------------------------------------------------------
@api_bp.route('/api/audit/<table_name>/<record_id>', methods=['GET'])
@check_auth
async def get_record_audit_history(request, table_name, record_id):
    table_name = table_name.lower()
    record_id = urllib.parse.unquote(str(record_id)).strip()
    
    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({"status": "success", "history": []}))
    
    try:
        async with request.app.ctx.pool.acquire() as conn:
            auth_tables, _ = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
            if table_name not in auth_tables:
                return add_security_headers(response.json({"error": "Unauthorized"}, status=403))
            if not await record_visible(conn, table_name, record_id, request.ctx.user_id, request.ctx.role):
                return add_security_headers(response.json({"error": "Record not found"}, status=404))
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
        logger.error(f"Audit fetch error: {e}", exc_info=True)
        return add_security_headers(response.json({"status": "error", "message": "Could not load audit history."}, status=500))

# -----------------------------------------------------------------------------
# WORKFLOW ENGINE & 21 CFR PART 11 E-SIGNATURE ROUTES
# -----------------------------------------------------------------------------
WORKFLOW_TARGETS = {'SUBMIT': ('PND', 'P'), 'APPROVE': ('ACT', 'A'), 'REJECT': ('REJ', 'R'), 'RECALL': ('DFT', 'D')}
PENDING_STATUSES = ('PND', 'PENDING', 'SUBMITTED', 'IN_REVIEW', 'P')


def transition_error(transition, old_status, submitter_id, user_id):
    """Why this workflow move is refused as (message, http_status), or None if allowed.
    Only a pending record can be approved, rejected or recalled; a pending record cannot be
    submitted again; whoever submitted a record cannot approve it (segregation of duties)."""
    if transition not in WORKFLOW_TARGETS:
        return f"Unknown transition '{transition}'.", 400
    is_pending = str(old_status).upper() in PENDING_STATUSES
    if transition != 'SUBMIT' and not is_pending:
        return f"Only a pending record can be {transition.lower()}ed. Current status: {old_status}.", 400
    if transition == 'SUBMIT' and is_pending:
        return "This record is already pending approval.", 400
    if transition == 'APPROVE' and submitter_id is not None and submitter_id == user_id:
        return "You submitted this record, so another approver must approve it.", 403
    return None


@api_bp.route('/api/workflow/transition', methods=['POST'])
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

    async with request.app.ctx.pool.acquire() as conn:
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
        if not pk_col:
            return add_security_headers(response.json({"error": "This table has no primary key and cannot be edited"}, status=400))
        cols_data = await get_table_columns(conn, table_name)
        schema_map = {c['column_name'].lower(): c for c in cols_data}
        pk_type = schema_map.get(pk_col.lower(), {}).get('data_type', 'integer')
        cast_pk = safe_cast_pk(record_id, pk_type)

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_col)
        scope = await get_company_scope(conn, table_name, [c['column_name'] for c in cols_data], pk_col, user_id)
        sc_sql, sc_vals = company_clause(scope, 2)

        current_row = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1{sc_sql}", cast_pk, *sc_vals)
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
        submitter_id = None
        if transition == 'APPROVE':
            submitter_id = await conn.fetchval("""
                SELECT pae_user_id FROM phc_approval_events_t
                WHERE LOWER(pae_table_name) = LOWER($1) AND pae_record_id = $2 AND pae_action = 'SUBMIT'
                ORDER BY pae_timestamp DESC LIMIT 1
            """, table_name, str(record_id))
        refusal = transition_error(transition, old_status, submitter_id, user_id)
        if refusal:
            return add_security_headers(response.json({"error": refusal[0]}, status=refusal[1]))
        new_status = WORKFLOW_TARGETS[transition][0 if status_max_len >= 3 else 1]

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
            sc_sql, sc_vals = company_clause(scope, len(set_vals) + 1)
            await conn.execute(f"UPDATE {q_table} SET {', '.join(set_parts)} WHERE {q_pk} = ${len(set_vals)}{sc_sql}", *set_vals, *sc_vals)
            
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
                    creator_id = await conn.fetchval("SELECT pus_user_id FROM phc_users_t WHERE LOWER(pus_user_name) = LOWER($1)", str(created_by_user))
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

@api_bp.route('/api/workflow/history/<table_name>/<record_id>', methods=['GET'])
@check_auth
async def get_workflow_history(request, table_name, record_id):
    table_name = table_name.lower()
    record_id = urllib.parse.unquote(str(record_id)).strip()
    
    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({"status": "success", "history": []}))
        
    try:
        async with request.app.ctx.pool.acquire() as conn:
            auth_tables, _ = await get_authorized_tables(conn, request.ctx.user_id, request.ctx.role)
            if table_name not in auth_tables:
                return add_security_headers(response.json({"error": "Unauthorized"}, status=403))
            if not await record_visible(conn, table_name, record_id, request.ctx.user_id, request.ctx.role):
                return add_security_headers(response.json({"error": "Record not found"}, status=404))
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
        logger.error(f"Error fetching workflow history: {e}", exc_info=True)
        return add_security_headers(response.json({"status": "error", "message": "Could not load workflow history."}, status=500))

# -----------------------------------------------------------------------------
# NOTIFICATIONS API ROUTES
# -----------------------------------------------------------------------------
@api_bp.route('/api/notifications', methods=['GET'])
@check_auth
async def get_user_notifications(request):
    user_id = request.ctx.user_id
    role = request.ctx.role
    
    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({"notifications": [], "unread_count": 0}))
        
    try:
        async with request.app.ctx.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pun_notification_id, pun_title, pun_message, pun_category,
                       pun_link_url, (r.pnr_notification_id IS NOT NULL) AS is_read, pun_created_at
                FROM phc_user_notifications_t
                LEFT JOIN phc_notification_reads_t r
                       ON r.pnr_notification_id = pun_notification_id AND r.pnr_user_id = $1
                WHERE (pun_recipient_user_id = $1 OR pun_recipient_role = $2 OR (pun_recipient_user_id IS NULL AND pun_recipient_role IS NULL))
                ORDER BY pun_created_at DESC
                LIMIT 30
            """, user_id, role)
            
            notifs = []
            unread_count = 0
            for r in rows:
                if not r['is_read']:
                    unread_count += 1
                notifs.append({
                    "id": r['pun_notification_id'],
                    "title": r['pun_title'],
                    "message": r['pun_message'],
                    "category": r['pun_category'] or 'WORKFLOW',
                    "link_url": r['pun_link_url'] or '#',
                    "is_read": r['is_read'],
                    "timestamp": r['pun_created_at'].strftime('%Y-%m-%d %H:%M') if r['pun_created_at'] else ""
                })
            return add_security_headers(response.json({"notifications": notifs, "unread_count": unread_count}))
    except Exception as e:
        logger.error(f"Error fetching notifications: {e}")
        return add_security_headers(response.json({"notifications": [], "unread_count": 0}))

@api_bp.route('/api/notifications/<notif_id>/read', methods=['POST'])
@check_auth
async def mark_notification_read(request, notif_id):
    client_csrf = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token")
    if not validate_csrf_token(client_csrf, request.ctx.session_id):
        return add_security_headers(response.json({"error": "Invalid CSRF token"}, status=403))
        
    try:
        nid = int(notif_id)
        async with request.app.ctx.pool.acquire() as conn:
            # Read state is per user: one approver opening a role notification must not clear it for the others.
            await conn.execute("""
                INSERT INTO phc_notification_reads_t (pnr_user_id, pnr_notification_id)
                SELECT $2, pun_notification_id FROM phc_user_notifications_t
                WHERE pun_notification_id = $1
                  AND (pun_recipient_user_id = $2 OR pun_recipient_role = $3 OR (pun_recipient_user_id IS NULL AND pun_recipient_role IS NULL))
                ON CONFLICT DO NOTHING
            """, nid, request.ctx.user_id, request.ctx.role)
        return add_security_headers(response.json({"status": "success"}))
    except ValueError:
        return add_security_headers(response.json({"error": "Invalid notification id"}, status=400))
    except Exception:
        logger.error("Error marking notification read", exc_info=True)
        return add_security_headers(response.json({"error": "Could not update notification."}, status=400))

# -----------------------------------------------------------------------------
# FILE SERVING & DOWNLOAD ROUTE
# -----------------------------------------------------------------------------
@api_bp.route('/api/file/<table_name>/<pk_val>/<col_name>', methods=['GET'])
@check_auth
async def download_file_column(request, table_name, pk_val, col_name):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    col_name = col_name.lower()
    
    async with request.app.ctx.pool.acquire() as conn:
        auth_tables, _ = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        pk_col = await get_pk_column(conn, table_name)
        if not pk_col:
            raise NotFound("This table has no primary key")
        cols_data = await get_table_columns(conn, table_name)
        schema_map = {c['column_name'].lower(): c for c in cols_data}
        if col_name not in schema_map:
            raise NotFound(f"Column '{col_name}' not found")

        pk_type = schema_map.get(pk_col.lower(), {}).get('data_type', 'integer')
        cast_pk = safe_cast_pk(pk_val, pk_type)
        q_table = quote_ident(table_name)
        q_col = quote_ident(col_name)
        q_pk = quote_ident(pk_col)
        scope = await get_company_scope(conn, table_name, [c['column_name'] for c in cols_data], pk_col, user_id)
        sc_sql, sc_vals = company_clause(scope, 2)

        val = await conn.fetchval(f"SELECT {q_col} FROM {q_table} WHERE {q_pk} = $1{sc_sql}", cast_pk, *sc_vals)
        if val is None:
            raise NotFound("No file found for this record")
        
        if isinstance(val, (bytes, bytearray, memoryview)):
            b_data = bytes(val)
            mime = "application/octet-stream"
            if b_data.startswith(b'\x89PNG\r\n\x1a\n'):
                mime = "image/png"
            elif b_data.startswith(b'\xff\xd8\xff'):
                mime = "image/jpeg"
            elif b_data.startswith(b'%PDF'):
                mime = "application/pdf"
            elif b_data.startswith(b'GIF87a') or b_data.startswith(b'GIF89a'):
                mime = "image/gif"
            elif b_data.startswith(b'RIFF') and b_data[8:12] == b'WEBP':
                mime = "image/webp"
            
            res = response.raw(b_data, content_type=mime)
            res.headers["Content-Disposition"] = f'inline; filename="{col_name}_{pk_val}"'
            return add_security_headers(res)
        else:
            return response.text(str(val))
