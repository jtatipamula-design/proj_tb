import time
import os
from datetime import datetime
from sanic import Blueprint, response
from app.utils import add_security_headers, render_template
from app.auth import check_auth, get_authorized_tables
from app.config import PORT, WORKERS, logger

system_bp = Blueprint("system_routes")
SERVER_START_TIME = time.time()
# -----------------------------------------------------------------------------
@system_bp.route('/health', methods=['GET'])
async def health_check(request):
    uptime = round(time.time() - SERVER_START_TIME, 2)
    return add_security_headers(response.json({
        "status": "healthy",
        "service": "Brihas ERP",
        "uptime_seconds": uptime,
        "timestamp": datetime.now().isoformat()
    }))

@system_bp.route('/ready', methods=['GET'])
async def readiness_check(request):
    if not hasattr(request.app.ctx, 'pool') or request.app.ctx.pool is None:
        return add_security_headers(response.json({"status": "unready", "error": "Database pool uninitialized"}, status=503))
    try:
        async with request.app.ctx.pool.acquire() as conn:
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
@system_bp.route('/docs', methods=['GET'])
@check_auth
async def swagger_ui(request):
    return render_template('swagger.html', request=request)

@system_bp.route('/openapi.json', methods=['GET'])
@check_auth
async def openapi_spec(request):
    user_id = request.ctx.user_id
    role = request.ctx.role
    
    async with request.app.ctx.pool.acquire() as conn:
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
