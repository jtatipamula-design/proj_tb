import os
import uuid
import time
import bcrypt
import jwt
from datetime import datetime, timedelta
from sanic import Blueprint, response

from app.config import JWT_SECRET, logger
from app.utils import get_client_ip, add_security_headers, generate_csrf_token, render_template, quote_ident
from app.auth import check_login_rate_limit, record_login_attempt, USER_AUTH_CACHE, check_auth
from app.database import get_table_columns

auth_bp = Blueprint("auth_routes")

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
