import os
import uuid
import time
import bcrypt
import jwt
from datetime import datetime, timedelta, timezone
from sanic import Blueprint, response

from app.config import JWT_SECRET, logger
from app.utils import (
    get_client_ip, add_security_headers, generate_csrf_token, render_template, quote_ident,
    validate_csrf_token, validate_password_strength, log_audit_event,
)
from app.auth import check_login_rate_limit, record_login_attempt, USER_AUTH_CACHE, check_auth
from app.database import get_table_columns

auth_bp = Blueprint("auth_routes")


def _as_utc(dt):
    """Treat naive datetimes (older rows) as UTC."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def next_failed_count(failed_attempts, locked_until, now):
    """Failure count after one more wrong password; an expired lock resets the count."""
    if locked_until and _as_utc(locked_until) <= now:
        failed_attempts = 0
    return (failed_attempts or 0) + 1


def password_change_error(current_ok, new, confirm, same_as_current):
    """Why a self-service password change is refused, or None if it may proceed."""
    if not current_ok:
        return "Current password is incorrect."
    if new != confirm:
        return "New password and confirmation do not match."
    ok, msg = validate_password_strength(new)
    if not ok:
        return msg
    if len(new.encode('utf-8')) > 72:  # bcrypt ignores (or rejects) anything past 72 bytes
        return "Password must be at most 72 bytes long."
    if same_as_current:
        return "New password must be different from the current password."
    return None


def _set_session_cookies(request, res, user_id, username, session_id):
    """Issues the JWT + CSRF cookies for session_id and drops the user's cached auth entry."""
    token = jwt.encode({
        "user_id": user_id,
        "username": username,
        "session_id": session_id,
        "exp": int(time.time() + 86400)
    }, JWT_SECRET, algorithm="HS256")
    USER_AUTH_CACHE.pop(user_id, None)
    is_secure = request.scheme == 'https' or os.environ.get("ENV") == "production" or bool(os.environ.get("RENDER"))
    res.add_cookie("auth_token", token, httponly=True, samesite="Lax", path="/", secure=is_secure)
    res.add_cookie("csrf_token", generate_csrf_token(session_id), httponly=False, samesite="Lax", path="/", secure=is_secure)
    return add_security_headers(res)

@auth_bp.route('/login', methods=['GET', 'POST'])
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

    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({
            "status": "error", 
            "message": "Database is not connected. Please ensure DATABASE_URL is set in your .env file."
        }, status=503))

    async with request.app.ctx.pool.acquire() as conn:
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
                locked_until = _as_utc(locked_until)
                now_dt = datetime.now(timezone.utc)
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
                
                res = response.json({"status": "success", "message": "Login successful"})
                return _set_session_cookies(request, res, user_id_val, user_name_val, session_id)
            else:
                # Increment failed attempts in PostgreSQL
                now_utc = datetime.now(timezone.utc)
                failed_count = next_failed_count(user.get('pus_failed_attempts'), user.get('pus_locked_until'), now_utc)
                user_id_val = user.get('pus_user_id') or user.get('id')
                if failed_count >= 5:
                    lock_time = now_utc + timedelta(minutes=15)
                    await conn.execute("UPDATE phc_users_t SET pus_failed_attempts = $1, pus_locked_until = $2 WHERE pus_user_id = $3", failed_count, lock_time, user_id_val)
                    return add_security_headers(response.json({
                        "status": "error", 
                        "message": "Account has been temporarily locked for 15 minutes due to 5 consecutive failed login attempts."
                    }, status=403))
                else:
                    await conn.execute("UPDATE phc_users_t SET pus_failed_attempts = $1, pus_locked_until = NULL WHERE pus_user_id = $2", failed_count, user_id_val)
        
        record_login_attempt(client_ip, success=False)
        return add_security_headers(response.json({"status": "error", "message": "Invalid credentials"}, status=401))

@auth_bp.route('/account/password', methods=['GET', 'POST'])
@check_auth
async def change_password(request):
    if request.method == 'GET':
        return render_template('change_password.html', request=request)

    def fail(msg, status=400):
        return add_security_headers(response.json({"error": msg}, status=status))

    data = request.json if isinstance(request.json, dict) else {}
    client_csrf = request.headers.get("X-CSRF-Token") or data.get('csrf_token')
    if not validate_csrf_token(client_csrf, request.ctx.session_id):
        return fail("Invalid or expired CSRF token.", 403)

    # Same limiter as /login, so a hijacked session cannot be used to guess the password.
    client_ip = get_client_ip(request)
    allowed, wait_sec = check_login_rate_limit(client_ip)
    if not allowed:
        return fail(f"Too many failed attempts. Please try again in {wait_sec // 60 + 1} minute(s).", 429)

    current = str(data.get("current_password", ""))
    new = str(data.get("new_password", ""))
    confirm = str(data.get("confirm_password", ""))
    user_id = request.ctx.user_id
    username = request.ctx.username or ""

    async with request.app.ctx.pool.acquire() as conn:
        stored_pwd = await conn.fetchval("SELECT pus_pwd FROM phc_users_t WHERE pus_user_id = $1", user_id) or ""
        current_ok = False
        if current and stored_pwd:
            try:
                current_ok = bcrypt.checkpw(current.encode('utf-8'), stored_pwd.encode('utf-8'))
            except (ValueError, TypeError):
                pass
        if not current_ok:
            record_login_attempt(client_ip, success=False)

        err = password_change_error(current_ok, new, confirm, new == current)
        if err:
            return fail(err)

        new_hash = bcrypt.hashpw(new.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        session_id = str(uuid.uuid4())  # new session id signs out every other device
        async with conn.transaction():
            await conn.execute("""
                UPDATE phc_users_t
                SET pus_pwd = $1, pus_session_id = $2, pus_modified = CURRENT_DATE, pus_modified_by = $3
                WHERE pus_user_id = $4
            """, new_hash, session_id, username[:20], user_id)
            await log_audit_event(conn, 'phc_users_t', user_id, 'PASSWORD_CHANGE', user_id, username, client_ip)

    res = response.json({"status": "success", "message": "Password changed"})
    return _set_session_cookies(request, res, user_id, username, session_id)

@auth_bp.route('/logout', methods=['GET'])
@check_auth
async def logout(request):
    user_id = getattr(request.ctx, 'user_id', None)
    if user_id:
        USER_AUTH_CACHE.pop(user_id, None)
        if hasattr(request.app.ctx, 'pool') and request.app.ctx.pool:
            try:
                async with request.app.ctx.pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute("UPDATE phc_users_t SET pus_session_id = NULL WHERE pus_user_id = $1", user_id)
            except Exception as e:
                logger.warning(f"Logout session clear error: {e}")
    res = response.redirect("/login")
    res.delete_cookie("auth_token", path="/")
    res.delete_cookie("csrf_token", path="/")
    return add_security_headers(res)
