"""Read-only JSON API: list and single-record GET for any authorized table."""
import urllib.parse
from sanic import Blueprint, response
from app.utils import quote_ident, add_security_headers, safe_cast_pk, _is_password_column
from app.auth import check_auth
from app.database import get_pk_column, get_table_columns
from app.routes.api import get_company_scope, company_clause
from app.query import parse_list_args, build_list_query

read_api_bp = Blueprint("read_api_routes")

BINARY_TYPES = ('bytea', 'blob', 'oid', 'binary', 'varbinary')
MAX_PER_PAGE = 200


def select_list(columns_data):
    """SELECT column list: password columns are left out, binary columns come back as NULL."""
    parts = []
    for c in columns_data:
        name = c['column_name']
        if _is_password_column(name):
            continue
        q = quote_ident(name)
        parts.append(f"NULL AS {q}" if (c.get('data_type') or '').lower() in BINARY_TYPES else q)
    return ", ".join(parts)


def json_value(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        return None
    if hasattr(v, 'isoformat'):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): json_value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [json_value(x) for x in v]
    return str(v)  # Decimal, UUID, timedelta, ...


def json_row(row):
    return {k: json_value(v) for k, v in dict(row).items() if not _is_password_column(k)}


def _int_arg(args, name, default, maximum=None):
    try:
        n = int(args.get(name, default))
    except (ValueError, TypeError):
        return default
    n = max(1, n)
    return min(n, maximum) if maximum else n


def _error(message, status):
    return add_security_headers(response.json({"error": message}, status=status))


async def _table_context(request, conn, table_name):
    """(pk_column, columns_data, scope) for an authorized table, or an error response."""
    auth_tables = request.ctx.all_tables
    if table_name not in auth_tables:
        return _error("Unauthorized", 403)
    pk_column = await get_pk_column(conn, table_name)
    columns_data = await get_table_columns(conn, table_name)
    if not pk_column or not columns_data:
        return _error("Table not found", 404)
    scope = await get_company_scope(conn, table_name, [c['column_name'] for c in columns_data], pk_column, request.ctx.user_id)
    return pk_column, columns_data, scope


@read_api_bp.route('/api/<table_name>', methods=['GET'], name="api_list")
@check_auth
async def api_list_records(request, table_name):
    table_name = table_name.lower()
    page = _int_arg(request.args, "page", 1)
    per_page = _int_arg(request.args, "per_page", 50, MAX_PER_PAGE)

    async with request.app.ctx.pool.acquire() as conn:
        ctx = await _table_context(request, conn, table_name)
        if not isinstance(ctx, tuple):
            return ctx
        pk_column, columns_data, scope = ctx
        where_sql, params, order_sql = build_list_query(parse_list_args(request.args), columns_data, pk_column, scope, table_name)
        q_table = quote_ident(table_name)
        total = await conn.fetchval(f"SELECT COUNT(*) FROM {q_table}{where_sql}", *params) or 0
        rows = await conn.fetch(
            f"SELECT {select_list(columns_data)} FROM {q_table}{where_sql}{order_sql} LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
            *params, per_page, (page - 1) * per_page)

    return add_security_headers(response.json(
        {"data": [json_row(r) for r in rows], "page": page, "per_page": per_page, "total": total}))


@read_api_bp.route('/api/<table_name>/<pk_val>', methods=['GET'], name="api_get")
@check_auth
async def api_get_record(request, table_name, pk_val):
    table_name = table_name.lower()

    async with request.app.ctx.pool.acquire() as conn:
        ctx = await _table_context(request, conn, table_name)
        if not isinstance(ctx, tuple):
            return ctx
        pk_column, columns_data, scope = ctx
        pk_type = next((c.get('data_type', 'integer') for c in columns_data if c['column_name'] == pk_column), 'integer')
        cast_pk = safe_cast_pk(urllib.parse.unquote(pk_val), pk_type)
        if cast_pk is None:
            return _error("Record not found", 404)
        sc_sql, sc_vals = company_clause(scope, 2)
        row = await conn.fetchrow(
            f"SELECT {select_list(columns_data)} FROM {quote_ident(table_name)} WHERE {quote_ident(pk_column)} = $1{sc_sql}",
            cast_pk, *sc_vals)

    if not row:
        return _error("Record not found", 404)
    return add_security_headers(response.json(json_row(row)))
