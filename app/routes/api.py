import json
import uuid
import bcrypt
import time
from datetime import datetime
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.config import logger
from app.utils import quote_ident, add_security_headers, _sanitize_for_audit, _sanitize_payload, _is_password_column, get_client_ip, save_uploaded_file, generate_csrf_token, log_audit_event, dispatch_notification, get_approval_workflow_info, validate_csrf_token, safe_cast_pk, validate_password_strength
from app.auth import check_auth, get_authorized_tables
from app.database import get_pk_column, get_table_columns, invalidate_caches_for_table, get_fk_map, SCHEMA_CACHE
import os
import urllib.parse
import hashlib

api_bp = Blueprint("api_routes")

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
@api_bp.route('/api/audit/<table_name>/<record_id>', methods=['GET'])
@check_auth
async def get_record_audit_history(request, table_name, record_id):
    table_name = table_name.lower()
    record_id = urllib.parse.unquote(str(record_id)).strip()
    
    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({"status": "success", "history": []}))
    
    try:
        async with request.app.ctx.pool.acquire() as conn:
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

@api_bp.route('/api/workflow/history/<table_name>/<record_id>', methods=['GET'])
@check_auth
async def get_workflow_history(request, table_name, record_id):
    table_name = table_name.lower()
    record_id = urllib.parse.unquote(str(record_id)).strip()
    
    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({"status": "success", "history": []}))
        
    try:
        async with request.app.ctx.pool.acquire() as conn:
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

@api_bp.route('/api/notifications/<notif_id>/read', methods=['POST'])
@check_auth
async def mark_notification_read(request, notif_id):
    client_csrf = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token")
    if not validate_csrf_token(client_csrf, request.ctx.session_id):
        return add_security_headers(response.json({"error": "Invalid CSRF token"}, status=403))
        
    try:
        nid = int(notif_id)
        async with request.app.ctx.pool.acquire() as conn:
            await conn.execute("UPDATE phc_user_notifications_t SET pun_is_read = TRUE WHERE pun_notification_id = $1 AND (pun_recipient_user_id = $2 OR pun_recipient_role = $3)", nid, request.ctx.user_id, request.ctx.role)
        return add_security_headers(response.json({"status": "success"}))
    except Exception as e:
        return add_security_headers(response.json({"error": str(e)}, status=400))

# -----------------------------------------------------------------------------
# OBSERVABILITY: HEALTH & READINESS PROBES
