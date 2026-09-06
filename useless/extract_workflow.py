import re

with open("server.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

workflow_funcs = "".join(lines[352:455])

with open("app/utils.py", "a", encoding="utf-8") as f:
    f.write("\n" + workflow_funcs)

with open("app/routes/api.py", "r", encoding="utf-8") as f:
    api_code = f.read()

import_pattern = r"from app.utils import quote_ident, add_security_headers, _sanitize_for_audit, _sanitize_payload, _is_password_column, get_client_ip, save_uploaded_file, generate_csrf_token"
replacement = import_pattern + ", log_audit_event, dispatch_notification, get_approval_workflow_info"

api_code = api_code.replace(import_pattern, replacement)

with open("app/routes/api.py", "w", encoding="utf-8") as f:
    f.write(api_code)

print("Workflow functions extracted and imports updated.")
