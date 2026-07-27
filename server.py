# ... existing code ...
import time

# --- ENTERPRISE PERFORMANCE CACHE ---
# Stores {user_id: {"session_id": str, "role": str, "expires": float}}
USER_AUTH_CACHE = {}
CACHE_TTL = 30  # Number of seconds before it checks the DB again

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
            
            # 1. CHECK THE BLAZING FAST RAM CACHE FIRST
            if cached_data and cached_data["expires"] > now:
                db_session_id = cached_data["session_id"]
                db_role = cached_data["role"]
            
            # 2. IF CACHE IS EMPTY OR EXPIRED, PING THE DB (Once per 30s)
            else:
                async with app.ctx.pool.acquire() as conn:
                    user_data = await conn.fetchrow("SELECT pus_session_id, pus_user_type FROM phc_users_t WHERE pus_user_id = $1", user_id)
                    
                    if not user_data:
                        res = response.redirect("/login")
                        res.delete_cookie("auth_token")
                        return res
                    
                    db_session_id = str(user_data['pus_session_id'])
                    db_role = user_data.get('pus_user_type', 'STD')
                    
                    # Save to RAM for the next 30 seconds
                    USER_AUTH_CACHE[user_id] = {
                        "session_id": db_session_id,
                        "role": db_role,
                        "expires": now + CACHE_TTL
                    }

            # 3. ENFORCE SESSION & ROLE
            if db_session_id != str(session_uuid):
                # Kicked out because they logged in elsewhere or session is invalid
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
# ... existing code ...
