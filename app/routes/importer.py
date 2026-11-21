import csv
import io
from datetime import datetime
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.config import logger, MAX_UPLOAD_SIZE
from app.utils import render_template, quote_ident, add_security_headers, _sanitize_payload, get_client_ip, log_audit_event, validate_csrf_token, get_approval_setup
from app.auth import check_auth, get_authorized_tables
from app.database import get_pk_column, get_table_columns, invalidate_caches_for_table, SCHEMA_CACHE
from app.routes.api import get_company_scope, write_block_reason, insert_pk_missing, apply_workflow_status_rule

import_bp = Blueprint("import_routes")

MAX_IMPORT_ROWS = 5000
BINARY_TYPES = ('bytea', 'blob', 'oid', 'binary', 'varbinary')


class CsvImportError(ValueError):
    """A rejected import; row is the 1-based data row number when one row is at fault."""
    def __init__(self, message, row=None):
        super().__init__(message)
        self.row = row


def export_label(col):
    """The header export_table_csv writes for a column."""
    return col.split('_', 1)[-1].replace('_', ' ').title()


def map_headers(headers, columns):
    """Map CSV headers (raw column names or export labels, case-insensitive) to columns.
    Returns (mapped, unknown): mapped[i] is the column for headers[i], or None when unknown."""
    lookup = {export_label(c).lower(): c for c in reversed(list(columns))}  # first column wins a shared label
    lookup.update({c.lower(): c for c in columns})  # a raw name always beats a label
    mapped = [lookup.get(h.strip().lower()) for h in headers]
    return mapped, [h for h, m in zip(headers, mapped) if m is None]


def _unescape_cell(val):
    """Undo the ' that _csv_cell puts in front of formula-like text on export."""
    return val[1:] if len(val) > 1 and val[0] == "'" and val[1] in '=+-@' else val


def parse_csv(body, columns, max_rows=MAX_IMPORT_ROWS):
    """Parse uploaded CSV bytes into [(row_number, {column: text})]; blank lines are skipped
    but still counted, so row_number matches the line the user sees under the header."""
    try:
        text = bytes(body).decode('utf-8-sig')
    except UnicodeDecodeError:
        raise CsvImportError("The file is not valid UTF-8. Save it as 'CSV UTF-8' and try again.")
    try:
        records = list(csv.reader(io.StringIO(text, newline='')))
    except csv.Error as e:
        raise CsvImportError(f"The file is not a valid CSV: {e}")
    if not records or not any(h.strip() for h in records[0]):
        raise CsvImportError("The file is empty: a header row is required.")

    mapped, unknown = map_headers(records[0], columns)
    if unknown:
        raise CsvImportError("Unknown column(s): " + ", ".join(repr(h) for h in unknown))
    if len(set(mapped)) != len(mapped):
        raise CsvImportError("Two headers refer to the same column.")

    rows = []
    for n, rec in enumerate(records[1:], start=1):
        if not any(cell.strip() for cell in rec):
            continue
        if len(rec) > len(mapped):
            raise CsvImportError("Row has more cells than the header.", row=n)
        rows.append((n, {col: _unescape_cell(cell) for col, cell in zip(mapped, rec)}))
        if len(rows) > max_rows:
            raise CsvImportError(f"Too many rows: at most {max_rows} can be imported at once.")
    if not rows:
        raise CsvImportError("The file has a header but no data rows.")
    return rows


def build_insert_row(raw, table_name, schema_map, pk_column, scope, role, username, now):
    """One CSV row -> the values to insert, under the same rules as a single create in process_api_action."""
    # The export writes binary cells as a "[Binary Object ...]" placeholder; never store that text.
    raw = {k: v for k, v in raw.items() if schema_map.get(k, {}).get('data_type', '').lower() not in BINARY_TYPES}
    clean = _sanitize_payload(raw, pk_column, schema_map, is_update=False)

    company_col, user_company = scope
    if company_col and not (table_name == 'phc_screens_t' and role == 'ADM' and clean.get(company_col)):
        clean[company_col] = user_company

    for wc in schema_map:
        low = wc.lower()
        if not any(w in low for w in ('created', 'modified', 'edited', 'update')):
            continue
        max_len = schema_map[wc].get('character_maximum_length') or 50
        clean[wc] = str(username)[:max_len] if 'by' in low else now

    for cname in schema_map:
        if 'start_date' in cname.lower() and not clean.get(cname):
            clean[cname] = now
    return clean


def _err(message, status=400, row=None):
    body = {"error": message}
    if row is not None:
        body["row"] = row
    return add_security_headers(response.json(body, status=status))


@import_bp.route('/import/<table_name>', methods=['GET'], name="import_form")
@check_auth
async def import_form(request, table_name):
    table_name = table_name.lower()
    if table_name not in request.ctx.all_tables:
        raise NotFound("Table not found or unauthorized")
    return render_template('import_view.html', request=request, table_name=table_name,
                           table_title=request.ctx.all_tables.get(table_name, table_name),
                           max_rows=MAX_IMPORT_ROWS)


@import_bp.route('/import/<table_name>', methods=['POST'], name="import_upload")
@check_auth
async def import_upload(request, table_name):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    client_ip = get_client_ip(request)
    session_username = getattr(request.ctx, 'username', None) or 'System'

    client_csrf = request.headers.get("X-CSRF-Token") or (request.form.get('csrf_token') if request.form else None)
    if not validate_csrf_token(client_csrf, getattr(request.ctx, 'session_id', None)):
        logger.warning(f"CSRF rejection: user_id={user_id}, table={table_name}, import")
        return _err("Invalid or expired security token (CSRF). Please refresh the page and try again.", 403)

    if table_name == 'phc_users_t':
        return _err("Users cannot be bulk imported: passwords must be set through the user form.")

    file_obj = request.files.get('file') if request.files else None
    if not file_obj or not file_obj.body:
        return _err("Choose a CSV file to import.")
    if len(file_obj.body) > MAX_UPLOAD_SIZE:
        return _err(f"File exceeds maximum allowed size of {MAX_UPLOAD_SIZE // (1024*1024)}MB.")

    async with request.app.ctx.pool.acquire() as conn:
        auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            return _err("Unauthorized", 403)
        block_reason = write_block_reason(table_name, role, table_modules.get(table_name))
        if block_reason:
            return _err(block_reason, 403)

        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            return _err("This table has no primary key and cannot be edited")
        columns_info = await get_table_columns(conn, table_name)
        schema_map = SCHEMA_CACHE["schema_maps"].get(table_name, {c['column_name']: c for c in columns_info})

        try:
            rows = parse_csv(file_obj.body, list(schema_map))
        except CsvImportError as e:
            return _err(str(e), row=e.row)

        scope = await get_company_scope(conn, table_name, list(schema_map), pk_column, user_id)
        q_table, q_pk = quote_ident(table_name), quote_ident(pk_column)
        has_workflow = await get_approval_setup(conn, table_name, user_id) is not None
        row_no = None
        try:
            async with conn.transaction():
                for row_no, raw in rows:
                    clean = build_insert_row(raw, table_name, schema_map, pk_column, scope, role, session_username, datetime.now())
                    if has_workflow:
                        apply_workflow_status_rule(clean, schema_map, is_insert=True)
                    if insert_pk_missing(schema_map, pk_column, clean):
                        raise CsvImportError(f"'{pk_column}' needs a value: this table does not generate IDs automatically.", row=row_no)
                    cols = ", ".join(quote_ident(c) for c in clean)
                    placeholders = ", ".join(f"${i+1}" for i in range(len(clean)))
                    clean[pk_column] = await conn.fetchval(
                        f"INSERT INTO {q_table} ({cols}) VALUES ({placeholders}) RETURNING {q_pk}", *clean.values())
                    await log_audit_event(conn, table_name, str(clean.get(pk_column) or 'NEW'), 'INSERT',
                                          user_id, session_username, client_ip, old_values=None, new_values=clean)
        except CsvImportError as e:
            return _err(str(e), row=e.row)
        except Exception as e:
            logger.error(f"Import error on {table_name} row {row_no}: {e}", exc_info=True)
            return _err("Could not save this row, so nothing was imported. Please check its values and try again.", row=row_no)

    invalidate_caches_for_table(table_name)
    return add_security_headers(response.json({"status": "success", "inserted": len(rows)}))
