import os
import re

with open("server.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

def fix_decorators(code, bp_name):
    return re.sub(r'@app\.(route|get|post|put|delete|patch|options)', fr'@{bp_name}.\1', code)

# --- Extract and fix _sanitize_payload for app/utils.py ---
sanitize_lines = lines[1079:1161]
sanitize_code = "".join(sanitize_lines)

with open("app/utils.py", "r", encoding="utf-8") as f:
    utils_code = f.read()

# Replace the stub _sanitize_payload with the full one
utils_code = re.sub(r'def _sanitize_payload\(.*?\):.*', sanitize_code, utils_code, flags=re.DOTALL)
with open("app/utils.py", "w", encoding="utf-8") as f:
    f.write(utils_code)


# --- Create app/routes/ui.py ---
ui_code = "".join(lines[1162:1748])
ui_code = fix_decorators(ui_code, "ui_bp")
ui_header = """import time
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.config import logger
from app.utils import render_template, quote_ident, add_security_headers
from app.auth import check_auth, get_authorized_tables
from app.database import get_pk_column, get_table_columns, get_dropdown_options, SCHEMA_CACHE

ui_bp = Blueprint("ui_routes")

"""
with open("app/routes/ui.py", "w", encoding="utf-8") as f:
    f.write(ui_header + ui_code.replace("app.ctx.pool", "request.app.ctx.pool"))


# --- Create app/routes/api.py ---
api_code = "".join(lines[1748:2343])
api_code = fix_decorators(api_code, "api_bp")
api_header = """import json
import uuid
import bcrypt
import time
from datetime import datetime
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.config import logger
from app.utils import quote_ident, add_security_headers, _sanitize_for_audit, _sanitize_payload, _is_password_column, get_client_ip, save_uploaded_file, generate_csrf_token
from app.auth import check_auth, get_authorized_tables
from app.database import get_pk_column, get_table_columns, invalidate_caches_for_table, get_fk_map

api_bp = Blueprint("api_routes")

"""
with open("app/routes/api.py", "w", encoding="utf-8") as f:
    f.write(api_header + api_code.replace("app.ctx.pool", "request.app.ctx.pool"))


# --- Create app/routes/system.py ---
system_code = "".join(lines[2343:])
system_code = fix_decorators(system_code, "system_bp")
system_header = """import time
import os
from sanic import Blueprint, response
from app.utils import add_security_headers, render_template

system_bp = Blueprint("system_routes")
SERVER_START_TIME = time.time()
"""
with open("app/routes/system.py", "w", encoding="utf-8") as f:
    f.write(system_header + system_code.replace("app.ctx.pool", "request.app.ctx.pool"))

print("Extraction complete!")
