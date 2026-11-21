"""Pure builders for the list query (search, filters, sort, company scope) shared by the
table page, CSV export and the JSON read API. No database access here."""
import json
from app.utils import quote_ident, _is_password_column

NUMERIC_TYPES = ('integer', 'bigint', 'smallint', 'numeric')
UNSEARCHABLE_TYPES = ('bytea', 'json', 'jsonb', 'geometry', 'point', 'polygon')
_LIKE_PATTERNS = {'contains': '%{}%', 'not_contains': '%{}%', 'starts_with': '{}%', 'ends_with': '%{}'}
_COMPARE_SQL = {'equals': '=', 'not_equals': '!=', 'greater_than': '>', 'less_than': '<'}


def _json_list(raw):
    try:
        parsed = json.loads(raw) if raw else []
    except Exception:
        return []
    return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []


def parse_list_args(args):
    """Read q / type_filter / sort_rules (or sort_col+sort_dir) / filters from a request.args-like mapping."""
    sort_col = (args.get("sort_col") or "").strip()
    sort_dir = (args.get("sort_dir") or "desc").strip().lower()
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'desc'

    raw_sort_rules = args.get("sort_rules") or ""
    if raw_sort_rules:
        sort_rules = [{"col": str(s["col"]), "dir": s["dir"]} for s in _json_list(raw_sort_rules)
                      if s.get("col") and s.get("dir") in ("asc", "desc")]
    else:
        sort_rules = [{"col": sort_col, "dir": sort_dir}] if sort_col else []

    filters = [{"col": str(f["col"]).strip(), "op": str(f["op"]).strip().lower(), "val": str(f.get("val", "")).strip()}
               for f in _json_list(args.get("filters") or "") if f.get("col") and f.get("op")]

    return {
        "q": (args.get("q") or "").strip(),
        "type_filter": (args.get("type_filter") or "").strip(),
        "sort_rules": sort_rules,
        "filters": filters,
        "sort_col": sort_col,
        "sort_dir": sort_dir,
    }


def build_list_query(list_args, columns_data, pk_column, company_scope, table_name):
    """Return (where_sql, params, order_sql) for a parse_list_args() result.
    where_sql is '' or ' WHERE ...', order_sql is '' or ' ORDER BY ...'; params are numbered from $1.
    Column names reach SQL only if they exist in columns_data, and only through quote_ident.
    Password columns are never searched, filtered or sorted on: a prefix filter would reveal the hash."""
    columns_data = [c for c in columns_data if not _is_password_column(c['column_name'])]
    schema_cols = {c['column_name']: c for c in columns_data}
    params, where = [], []

    def bind(value):
        params.append(value)
        return f"${len(params)}"

    company_col, company_val = company_scope
    if company_col:
        where.append(f"{quote_ident(company_col)} = {bind(company_val)}")

    if table_name == 'phc_lookup_values_t' and list_args["type_filter"]:
        where.append(f"{quote_ident('plv_lookup_type_code')} = {bind(list_args['type_filter'])}")

    if list_args["q"]:
        searchable = [c['column_name'] for c in columns_data if c['data_type'] not in UNSEARCHABLE_TYPES]
        if searchable:
            p = bind(f"%{list_args['q']}%")
            where.append("(" + " OR ".join(f"CAST({quote_ident(col)} AS TEXT) ILIKE {p}" for col in searchable) + ")")

    for f in list_args["filters"]:
        col_name, op, val = f['col'], f['op'], f['val']
        if col_name not in schema_cols:
            continue
        target_type = (schema_cols[col_name].get('data_type') or '').lower()
        q_col = quote_ident(col_name)

        if op == 'is_empty':
            where.append(f"({q_col} IS NULL OR CAST({q_col} AS TEXT) = '')")
        elif op == 'is_not_empty':
            where.append(f"({q_col} IS NOT NULL AND CAST({q_col} AS TEXT) != '')")
        elif not val:
            continue
        elif op in _LIKE_PATTERNS:
            p = bind(_LIKE_PATTERNS[op].format(val))
            if op == 'not_contains':
                where.append(f"(CAST({q_col} AS TEXT) NOT ILIKE {p} OR {q_col} IS NULL)")
            else:
                where.append(f"CAST({q_col} AS TEXT) ILIKE {p}")
        elif op in _COMPARE_SQL:
            sym = _COMPARE_SQL[op]
            is_eq = op in ('equals', 'not_equals')
            if target_type in NUMERIC_TYPES:
                try:
                    num_val = float(val) if '.' in val else int(val)
                except (ValueError, TypeError):
                    continue
                clause = f"{q_col} {sym} {bind(num_val)}"
            elif is_eq and target_type == 'boolean':
                clause = f"{q_col} {sym} {bind(val.lower() in ('true', '1', 'yes', 't', 'act'))}"
            elif is_eq:
                clause = f"LOWER(CAST({q_col} AS TEXT)) {sym} LOWER({bind(val)})"
            elif 'date' in target_type or 'timestamp' in target_type:
                # ::text first so asyncpg binds the value as a string, not a datetime object
                clause = f"{q_col} {sym} {bind(val)}::text::timestamp"
            else:
                continue
            where.append(f"({clause} OR {q_col} IS NULL)" if op == 'not_equals' else clause)

    order = [f"{quote_ident(s['col'])} {'ASC' if s['dir'] == 'asc' else 'DESC'}"
             for s in list_args["sort_rules"] if s['col'] in schema_cols]
    if pk_column and (not order or pk_column not in [s['col'] for s in list_args["sort_rules"]]):
        order.append(f"{quote_ident(pk_column)} DESC")

    where_sql = " WHERE " + " AND ".join(where) if where else ""
    order_sql = " ORDER BY " + ", ".join(order) if order else ""
    return where_sql, params, order_sql
