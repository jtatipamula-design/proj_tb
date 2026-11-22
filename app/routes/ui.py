import time
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.utils import render_template, quote_ident, add_security_headers, resolve_lookup_type, safe_cast_pk, get_approval_workflow_info, get_approval_setup, workflow_status_column, _is_password_column, audit_column_kind, find_audit_column
from app.auth import check_auth
from app.database import get_pk_column, get_table_columns, get_dropdown_options, SCHEMA_CACHE, clear_schema_cache, get_all_lookups, resolve_fk_details, get_fk_display_dict
from app.routes.api import get_company_scope, company_clause
from app.query import parse_list_args, build_list_query
import urllib.parse
from datetime import datetime

ui_bp = Blueprint("ui_routes")

@ui_bp.route('/')
@check_auth
async def dashboard(request):
    modules_tree = getattr(request.ctx, 'modules_tree', {}) or {}
    all_tables = getattr(request.ctx, 'all_tables', {}) or {}
    
    stats = {
        "total_modules": len(modules_tree),
        "total_screens": len(all_tables),
        "total_users": 0,
        "active_sessions": 0,
        "validation_records": 0,
        "total_roles": 0,
        "db_latency_ms": None
    }
    
    if hasattr(request.app.ctx, 'pool') and request.app.ctx.pool:
        t0 = time.perf_counter()
        try:
            async with request.app.ctx.pool.acquire() as conn:
                try:
                    u_row = await conn.fetchrow("""
                        SELECT 
                            COUNT(*) as total_users,
                            COUNT(CASE WHEN pus_session_id IS NOT NULL THEN 1 END) as active_sessions
                        FROM phc_users_t
                    """)
                    if u_row:
                        stats["total_users"] = u_row["total_users"] or 0
                        stats["active_sessions"] = u_row["active_sessions"] or 0
                except Exception:
                    pass

                try:
                    stats["total_roles"] = await conn.fetchval("SELECT COUNT(*) FROM phc_roles_t") or 0
                except Exception:
                    pass

                try:
                    p_count = await conn.fetchval("SELECT COUNT(*) FROM pcv_production_products_t") or 0
                    v_count = await conn.fetchval("SELECT COUNT(*) FROM pcv_validation_executions_t") or 0
                    stats["validation_records"] = p_count + v_count
                except Exception:
                    pass

                t1 = time.perf_counter()
                stats["db_latency_ms"] = round((t1 - t0) * 1000, 1)
        except Exception:
            pass

    return render_template('dashboard.html', request=request, stats=stats)

@ui_bp.route('/table/<table_name>')
@check_auth
async def show_table(request, table_name):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()
    
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except (ValueError, TypeError):
        page = 1

    per_page = 50
    offset = (page - 1) * per_page
    list_args = parse_list_args(request.args)
    search_query, type_filter = list_args["q"], list_args["type_filter"]
    active_sort_rules, active_filter_rules = list_args["sort_rules"], list_args["filters"]
    sort_col, sort_dir = list_args["sort_col"], list_args["sort_dir"]

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with request.app.ctx.pool.acquire() as conn:

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            raise NotFound("Table configuration error: No Primary Key")

        columns_data = await get_table_columns(conn, table_name)

        if not columns_data:
            raise NotFound("Table does not exist")

        columns = []
        date_columns = []
        audit_by_columns = []
        audit_date_columns = []
        company_col_def = None

        for c in columns_data:
            cname = c['column_name']
            cname_low = cname.lower()
            if cname in (pk_column, 'psn_screen_id') or _is_password_column(cname): continue
            
            is_company_col = 'company_id' in cname_low
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
                    company_col_def = {"raw": cname, "column_name": cname, "label": clean_label, "data_type": c.get('data_type', 'varchar')}
                continue
            
            # 1. Audit columns (Created/Modified By, then Created/Modified Date) go last
            kind = audit_column_kind(cname)
            if kind:
                label = {"created_by": "Created By", "modified_by": "Modified By",
                         "created_at": "Created Date", "modified_at": "Modified Date"}[kind]
                (audit_by_columns if kind.endswith('_by') else audit_date_columns).append(
                    {"raw": cname, "column_name": cname, "label": label, "data_type": c.get('data_type', 'varchar')})
                continue

            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            col_def = {"raw": cname, "column_name": cname, "label": clean_label, "data_type": c.get('data_type', 'varchar')}
            
            # 3. Regular Date columns vs standard business columns
            if 'date' in c['data_type'] or 'timestamp' in c['data_type']:
                date_columns.append(col_def)
            else:
                columns.append(col_def)
                
        columns.extend(date_columns)
        columns.extend(audit_by_columns)
        columns.extend(audit_date_columns)
        if company_col_def:
            columns.append(company_col_def)

        lookup_categories = []
        if table_name == 'phc_lookup_values_t':
            try:
                lookup_categories = await conn.fetch(
                    "SELECT plt_lookup_type_code as code, COALESCE(plt_lookup_type, plt_lookup_type_code) as name FROM phc_lookup_types WHERE plt_status = 'ACT' ORDER BY name"
                )
            except Exception:
                try:
                    lookup_categories = await conn.fetch(
                        "SELECT plt_lookup_type_code as code, COALESCE(plt_lookup_type_name, plt_lookup_type, plt_lookup_type_code) as name FROM phc_lookup_types_t WHERE plt_status = 'ACT' ORDER BY name"
                    )
                except Exception:
                    lookup_categories = []

        q_table = quote_ident(table_name)
        scope = await get_company_scope(conn, table_name, [c['column_name'] for c in columns_data], pk_column, user_id)
        where_sql, params, order_sql = build_list_query(list_args, columns_data, pk_column, scope, table_name)
        count_query = f"SELECT COUNT(*) FROM {q_table}{where_sql}"
        base_query = f"SELECT * FROM {q_table}{where_sql}{order_sql} LIMIT ${len(params)+1} OFFSET ${len(params)+2}"

        try:
            total_count = await conn.fetchval(count_query, *params)
            total_count = total_count or 0
            raw_rows = await conn.fetch(base_query, *(params + [per_page, offset]))
        except Exception:
            clear_schema_cache()
            total_count = await conn.fetchval(count_query, *params)
            total_count = total_count or 0
            raw_rows = await conn.fetch(base_query, *(params + [per_page, offset]))

        resolved_rows = [dict(r) for r in raw_rows]
        
        # In-Memory Cached Lookup Resolution
        lookup_map = await get_all_lookups(conn)

        # Resolve FKs and lookups
        
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
            # 1. Foreign Key Resolution
            f_table, f_pk = await resolve_fk_details(conn, table_name, cname)
            if f_table and f_pk:
                ids = set(r[cname] for r in resolved_rows if r[cname] is not None)
                if ids:
                    try:
                        fk_dict = await get_fk_display_dict(conn, f_table, f_pk, specific_ids=ids)
                        for r in resolved_rows:
                            val = r[cname]
                            if val in fk_dict:
                                r[cname] = f"{fk_dict[val]} (ID: {val})"
                    except Exception:
                        pass
            
            # 2. Dynamic Lookups (In-Memory Resolution with Prefix Stripping)
            canonical_lookup = resolve_lookup_type(cname)
            upper_cname = cname.upper()
            col_lookup = lookup_map.get(canonical_lookup) or lookup_map.get(upper_cname)
            if col_lookup:
                for r in resolved_rows:
                    val_str = str(r[cname]) if r[cname] is not None else None
                    if val_str and val_str in col_lookup:
                        r[cname] = col_lookup[val_str]

            # 3. Format Database Binary Object Columns (Bytea/Blob)
            if c.get('data_type', '').lower() in ('bytea', 'blob', 'oid', 'binary', 'varbinary'):
                for r in resolved_rows:
                    val = r.get(cname)
                    if val is not None:
                        if isinstance(val, (bytes, bytearray, memoryview)):
                            r[cname] = f"[Binary Object ({len(val)} bytes)]"
                        elif not isinstance(val, str):
                            r[cname] = "[Binary Object]"
                        
        rows = resolved_rows

    total_pages = max(1, (total_count + per_page - 1) // per_page)
    start_row = offset + 1 if total_count > 0 else 0
    end_row = min(offset + per_page, total_count)

    return render_template(
        'table_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        rows=rows,
        pk_column=pk_column,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        start_row=start_row,
        end_row=end_row,
        search_query=search_query,
        lookup_categories=lookup_categories,
        type_filter=type_filter,
        active_sort_rules=active_sort_rules,
        active_filter_rules=active_filter_rules,
        sort_col=sort_col,
        sort_dir=sort_dir
    )

@ui_bp.route('/new/<table_name>', methods=['GET'])
@check_auth
async def show_add_form(request, table_name):
    return await render_form(request, table_name, is_update=False)

@ui_bp.route('/edit/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_edit_form(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

@ui_bp.route('/form/<table_name>/<pk_val>', methods=['GET'])
@check_auth
async def show_form_view_alias(request, table_name, pk_val):
    return await render_form(request, table_name, is_update=True, pk_val=pk_val)

def form_field_value(column_name, is_file, raw_val, is_update):
    """Value pre-filled in a form input. A stored password hash is never sent: saving the form
    would otherwise store the hash itself as the new password (an empty box keeps the old one)."""
    if _is_password_column(column_name):
        return ''
    if is_file and is_update:
        return "[Stored Binary Object]" if raw_val not in (None, '') else ''
    return raw_val

async def render_form(request, table_name, is_update=False, pk_val=None):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with request.app.ctx.pool.acquire() as conn:

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)
        if not pk_column:
            raise NotFound("This table has no primary key and cannot be edited")
        columns_data = await get_table_columns(conn, table_name)
        schema_map = SCHEMA_CACHE["schema_maps"].get(table_name, {c['column_name']: c for c in columns_data})
        pk_type = schema_map.get(pk_column, {}).get('data_type', 'integer')

        q_table = quote_ident(table_name)
        q_pk = quote_ident(pk_column)

        row_data = {}
        if is_update:
            pk_val = urllib.parse.unquote(pk_val) if pk_val else pk_val
            cast_pk = safe_cast_pk(pk_val, pk_type)
            if cast_pk is None:
                raise NotFound(f"Invalid primary key format. pk_val='{pk_val}', pk_type='{pk_type}'")
            scope = await get_company_scope(conn, table_name, list(schema_map), pk_column, user_id)
            sc_sql, sc_vals = company_clause(scope, 2)
            row_data = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1{sc_sql}", cast_pk, *sc_vals)
            if not row_data:
                raise NotFound("Record not found")

        lookup_map = await get_all_lookups(conn)
        user_company = await conn.fetchval("SELECT pus_company_id FROM phc_users_t WHERE pus_user_id = $1", user_id)
        # With an approval rule the status is set only by the workflow panel, so it is not a form field.
        wf_status_col = workflow_status_column(schema_map) if await get_approval_setup(conn, table_name, user_id) else None

        columns = []
        company_form_def = None
        for c in columns_data:
            cname = c['column_name']
            if audit_column_kind(cname) or cname == wf_status_col:
                continue
                
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if not (table_name == 'phc_screens_t' and role == 'ADM'):
                    continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            raw_val = row_data.get(cname, '') if is_update else ''
            is_file = c['data_type'].lower() in ('bytea', 'blob', 'oid', 'binary', 'varbinary')
            val = form_field_value(cname, is_file, raw_val, is_update)

            options = await get_dropdown_options(conn, table_name, cname, preloaded_lookups=lookup_map, company_id=user_company)

            json_options = None
            if table_name == 'phc_role_screen_assignment_t' and cname == 'prs_screen_id' and not is_update:
                json_options = await get_dropdown_options(conn, table_name, cname, preloaded_lookups=lookup_map, company_id=user_company)
                options = [] 
                
            col_def = {
                "column_name": cname,
                "label": clean_label,
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO' and 'default' not in cname.lower(),
                "is_pk": cname == pk_column,
                "value": val,
                "options": options,
                "json_options": json_options,
                "is_file": is_file
            }
            if is_company_col:
                company_form_def = col_def
            else:
                columns.append(col_def)

        if company_form_def:
            columns.append(company_form_def)

        audit_info = {}
        workflow_info = None
        if is_update and row_data:
            created_by_col, created_at_col, modified_by_col, modified_at_col = (
                find_audit_column(row_data.keys(), k) for k in ('created_by', 'created_at', 'modified_by', 'modified_at'))

            audit_info = {
                "created_by": row_data.get(created_by_col) if created_by_col else None,
                "created_at": row_data.get(created_at_col) if created_at_col else None,
                "modified_by": row_data.get(modified_by_col) if modified_by_col else None,
                "modified_at": row_data.get(modified_at_col) if modified_at_col else None,
            }

            workflow_info = await get_approval_workflow_info(conn, table_name, pk_val, dict(row_data), user_id, role)

    return render_template(
        'form_view.html',
        request=request,
        table_name=table_name,
        table_title=table_title,
        columns=columns,
        is_update=is_update,
        pk_val=pk_val,
        audit_info=audit_info,
        workflow_info=workflow_info
    )

def _csv_cell(val):
    """Render one CSV cell; prefix formula-like text with ' to block spreadsheet formula injection."""
    if val is None:
        return ''
    if isinstance(val, (bytes, bytearray, memoryview)):
        return f"[Binary Object ({len(val)} bytes)]"
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')
    s = str(val)
    return "'" + s if isinstance(val, str) and s[:1] in ('=', '+', '-', '@') else s

@ui_bp.route('/export/<table_name>')
@check_auth
async def export_table_csv(request, table_name):
    import csv
    import io
    
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    async with request.app.ctx.pool.acquire() as conn:
        auth_tables, table_modules = request.ctx.all_tables, request.ctx.table_modules
        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")

        pk_column = await get_pk_column(conn, table_name)
        columns_data = await get_table_columns(conn, table_name)

        export_cols = []
        company_csv_col = None
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column or _is_password_column(cname): continue
            
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    company_csv_col = cname
                continue
            export_cols.append(cname)
        if company_csv_col:
            export_cols.append(company_csv_col)

        if not export_cols:
            return response.text("No exportable columns found.", status=400)

        col_list = ", ".join(quote_ident(c) for c in export_cols)
        q_table = quote_ident(table_name)
        scope = await get_company_scope(conn, table_name, [c['column_name'] for c in columns_data], pk_column, user_id)
        where_clause, query_params, order_clause = build_list_query(parse_list_args(request.args), columns_data, pk_column, scope, table_name)
        query = f"SELECT {col_list} FROM {q_table}{where_clause}{order_clause}"

    table_title = auth_tables.get(table_name, table_name)
    safe_title = table_title.replace(' ', '_').replace('"', '')
    filename = f"{safe_title}_Export.csv"

    headers = dict(add_security_headers(response.empty()).headers)
    headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    res = await request.respond(content_type="text/csv", headers=headers)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([col.split('_', 1)[-1].replace('_', ' ').title() for col in export_cols])

    async with request.app.ctx.pool.acquire() as conn:
        async with conn.transaction():
            n = 0
            async for row in conn.cursor(query, *query_params):
                writer.writerow([_csv_cell(row[col]) for col in export_cols])
                n += 1
                if n % 500 == 0:
                    await res.send(output.getvalue())
                    output.seek(0)
                    output.truncate(0)
    await res.send(output.getvalue())
    await res.eof()
