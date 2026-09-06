import time
from sanic import Blueprint, response
from sanic.exceptions import NotFound
from app.config import logger
from app.utils import render_template, quote_ident, add_security_headers, resolve_lookup_type, safe_cast_pk, get_approval_workflow_info
from app.auth import check_auth, get_authorized_tables
from app.database import get_pk_column, get_table_columns, get_dropdown_options, SCHEMA_CACHE, clear_schema_cache, get_all_lookups, get_fk_map, resolve_fk_details, get_fk_display_dict
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
        "total_users": 1,
        "active_sessions": 1,
        "validation_records": 0,
        "total_roles": 0,
        "uptime_percent": 99.9,
        "db_latency_ms": 3.8
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
                        stats["total_users"] = u_row["total_users"] or 1
                        stats["active_sessions"] = u_row["active_sessions"] or 1
                except Exception:
                    pass

                try:
                    stats["total_roles"] = await conn.fetchval("SELECT COUNT(*) FROM phc_roles_t") or 0
                except Exception:
                    pass

                try:
                    p_count = await conn.fetchval("SELECT COUNT(*) FROM pcv_products_t") or 0
                    v_count = await conn.fetchval("SELECT COUNT(*) FROM pcv_validation_executions_t") or 0
                    stats["validation_records"] = p_count + v_count
                except Exception:
                    pass

                t1 = time.perf_counter()
                stats["db_latency_ms"] = max(1.2, round((t1 - t0) * 1000, 1))
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
    search_query = request.args.get("q", "").strip()
    type_filter = request.args.get("type_filter", "").strip()

    # 1. Parse server-side sorting parameters
    raw_sort_rules = request.args.get("sort_rules", "")
    sort_col = request.args.get("sort_col", "").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip().lower()
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'desc'

    active_sort_rules = []
    if raw_sort_rules:
        try:
            import json
            parsed_sort = json.loads(raw_sort_rules)
            if isinstance(parsed_sort, list):
                for s in parsed_sort:
                    if isinstance(s, dict) and s.get("col") and s.get("dir") in ("asc", "desc"):
                        active_sort_rules.append({"col": s["col"], "dir": s["dir"]})
        except Exception:
            pass
    elif sort_col:
        active_sort_rules = [{"col": sort_col, "dir": sort_dir}]

    # 2. Parse server-side structured filter parameters
    raw_filters = request.args.get("filters", "")
    active_filter_rules = []
    if raw_filters:
        try:
            import json
            parsed_filters = json.loads(raw_filters)
            if isinstance(parsed_filters, list):
                for f in parsed_filters:
                    if isinstance(f, dict) and f.get("col") and f.get("op"):
                        active_filter_rules.append({
                            "col": str(f["col"]).strip(),
                            "op": str(f["op"]).strip().lower(),
                            "val": str(f.get("val", "")).strip()
                        })
        except Exception:
            pass

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with request.app.ctx.pool.acquire() as conn:
        if auth_tables is None or table_modules is None:
            auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)

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
            if cname in (pk_column, 'psn_screen_id'): continue
            
            is_company_col = 'company_id' in cname_low
            if is_company_col:
                if role == 'ADM' and table_modules.get(table_name, '').lower() == 'erpadmin':
                    clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
                    company_col_def = {"raw": cname, "column_name": cname, "label": clean_label, "data_type": c.get('data_type', 'varchar')}
                continue
            
            # 1. Audit "By" columns (Created By / Modified By)
            if 'created' in cname_low and 'by' in cname_low:
                audit_by_columns.append({"raw": cname, "column_name": cname, "label": "Created By", "data_type": c.get('data_type', 'varchar')})
                continue
            elif ('modified' in cname_low or 'edited' in cname_low or 'updated' in cname_low) and 'by' in cname_low:
                audit_by_columns.append({"raw": cname, "column_name": cname, "label": "Modified By", "data_type": c.get('data_type', 'varchar')})
                continue
            
            # 2. Audit "Date" columns (Created Date / Modified Date)
            elif 'created' in cname_low and ('date' in c['data_type'] or 'timestamp' in c['data_type'] or 'date' in cname_low):
                audit_date_columns.append({"raw": cname, "column_name": cname, "label": "Created Date", "data_type": c.get('data_type', 'varchar')})
                continue
            elif ('modified' in cname_low or 'edited' in cname_low or 'updated' in cname_low) and ('date' in c['data_type'] or 'timestamp' in c['data_type'] or 'date' in cname_low):
                audit_date_columns.append({"raw": cname, "column_name": cname, "label": "Modified Date", "data_type": c.get('data_type', 'varchar')})
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
        q_pk = quote_ident(pk_column)
        schema_cols = {c['column_name']: c for c in columns_data}
        
        base_query = f"SELECT * FROM {q_table}"
        count_query = f"SELECT COUNT(*) FROM {q_table}"
        params = []
        where_clauses = []

        if table_name == 'phc_lookup_values_t' and type_filter:
            params.append(type_filter)
            where_clauses.append(f"{quote_ident('plv_lookup_type_code')} = ${len(params)}")

        # 3. Server-side global search across all text-castable columns
        if search_query:
            params.append(f"%{search_query}%")
            param_idx = len(params)
            searchable_cols = [
                c['column_name'] for c in columns_data 
                if c['data_type'] not in ('bytea', 'json', 'jsonb', 'geometry', 'point', 'polygon')
            ]
            if searchable_cols:
                search_clauses = [f"CAST({quote_ident(col)} AS TEXT) ILIKE ${param_idx}" for col in searchable_cols]
                where_clauses.append("(" + " OR ".join(search_clauses) + ")")

        # 4. Server-side structured 10-operator filter execution
        for f in active_filter_rules:
            col_name = f['col']
            op = f['op']
            val = f['val']
            if col_name not in schema_cols:
                continue
            
            c_info = schema_cols[col_name]
            target_type = c_info.get('data_type', '').lower()
            q_col = quote_ident(col_name)

            if op == 'is_empty':
                where_clauses.append(f"({q_col} IS NULL OR CAST({q_col} AS TEXT) = '')")
            elif op == 'is_not_empty':
                where_clauses.append(f"({q_col} IS NOT NULL AND CAST({q_col} AS TEXT) != '')")
            elif op == 'contains' and val:
                params.append(f"%{val}%")
                where_clauses.append(f"CAST({q_col} AS TEXT) ILIKE ${len(params)}")
            elif op == 'not_contains' and val:
                params.append(f"%{val}%")
                where_clauses.append(f"(CAST({q_col} AS TEXT) NOT ILIKE ${len(params)} OR {q_col} IS NULL)")
            elif op == 'starts_with' and val:
                params.append(f"{val}%")
                where_clauses.append(f"CAST({q_col} AS TEXT) ILIKE ${len(params)}")
            elif op == 'ends_with' and val:
                params.append(f"%{val}")
                where_clauses.append(f"CAST({q_col} AS TEXT) ILIKE ${len(params)}")
            elif op == 'equals' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"{q_col} = ${len(params)}")
                    except (ValueError, TypeError):
                        pass
                elif target_type == 'boolean':
                    b_val = val.lower() in ('true', '1', 'yes', 't', 'act')
                    params.append(b_val)
                    where_clauses.append(f"{q_col} = ${len(params)}")
                else:
                    params.append(val)
                    where_clauses.append(f"LOWER(CAST({q_col} AS TEXT)) = LOWER(${len(params)})")
            elif op == 'not_equals' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"({q_col} != ${len(params)} OR {q_col} IS NULL)")
                    except (ValueError, TypeError):
                        pass
                elif target_type == 'boolean':
                    b_val = val.lower() in ('true', '1', 'yes', 't', 'act')
                    params.append(b_val)
                    where_clauses.append(f"({q_col} != ${len(params)} OR {q_col} IS NULL)")
                else:
                    params.append(val)
                    where_clauses.append(f"(LOWER(CAST({q_col} AS TEXT)) != LOWER(${len(params)}) OR {q_col} IS NULL)")
            elif op == 'greater_than' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"{q_col} > ${len(params)}")
                    except (ValueError, TypeError):
                        pass
                elif 'date' in target_type or 'timestamp' in target_type:
                    params.append(val)
                    where_clauses.append(f"{q_col} > ${len(params)}::timestamp")
            elif op == 'less_than' and val:
                if target_type in ('integer', 'bigint', 'smallint', 'numeric'):
                    try:
                        num_val = float(val) if '.' in val else int(val)
                        params.append(num_val)
                        where_clauses.append(f"{q_col} < ${len(params)}")
                    except (ValueError, TypeError):
                        pass
                elif 'date' in target_type or 'timestamp' in target_type:
                    params.append(val)
                    where_clauses.append(f"{q_col} < ${len(params)}::timestamp")

        if where_clauses:
            where_str = " WHERE " + " AND ".join(where_clauses)
            base_query += where_str
            count_query += where_str

        # 5. Dynamic server-side ORDER BY generation
        order_clauses = []
        for s in active_sort_rules:
            c_name = s['col']
            d_str = 'ASC' if s['dir'] == 'asc' else 'DESC'
            if c_name in schema_cols:
                order_clauses.append(f"{quote_ident(c_name)} {d_str}")
        
        if not order_clauses or pk_column not in [s['col'] for s in active_sort_rules]:
            order_clauses.append(f"{q_pk} DESC")
        
        base_query += f" ORDER BY {', '.join(order_clauses)} LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
        
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
        fk_map = await get_fk_map(conn, table_name)
        
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
                    except Exception as e:
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

async def render_form(request, table_name, is_update=False, pk_val=None):
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    auth_tables = getattr(request.ctx, 'all_tables', None)
    table_modules = getattr(request.ctx, 'table_modules', None)

    async with request.app.ctx.pool.acquire() as conn:
        if auth_tables is None or table_modules is None:
            auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)

        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")
        
        table_title = auth_tables[table_name]
        pk_column = await get_pk_column(conn, table_name)
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
            row_data = await conn.fetchrow(f"SELECT * FROM {q_table} WHERE {q_pk} = $1", cast_pk)
            if not row_data:
                raise NotFound(f"Record not found. Table: {q_table}, PK: {q_pk}, Value: '{cast_pk}', Type: {type(cast_pk).__name__}")

        lookup_map = await get_all_lookups(conn)

        columns = []
        company_form_def = None
        for c in columns_data:
            cname = c['column_name']
            if 'created' in cname.lower() or 'modified' in cname.lower() or 'edited' in cname.lower():
                continue
                
            is_company_col = 'company_id' in cname.lower()
            if is_company_col:
                if not (table_name == 'phc_screens_t' and role == 'ADM'):
                    continue
            
            clean_label = cname.split('_', 1)[-1].replace('_', ' ').title()
            
            val = row_data.get(cname, '') if is_update else ''
            options = await get_dropdown_options(conn, table_name, cname, preloaded_lookups=lookup_map)

            json_options = None
            if table_name == 'phc_role_screen_assignment_t' and cname == 'prs_screen_id' and not is_update:
                json_options = await get_dropdown_options(conn, table_name, cname, preloaded_lookups=lookup_map)
                options = [] 
                
            col_def = {
                "column_name": cname,
                "label": clean_label,
                "data_type": c['data_type'],
                "required": c['is_nullable'] == 'NO' and 'default' not in cname.lower(),
                "is_pk": cname == pk_column,
                "value": val,
                "options": options,
                "json_options": json_options
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
            created_by_col = next((c for c in row_data.keys() if 'created' in c.lower() and 'by' in c.lower()), None)
            created_at_col = next((c for c in row_data.keys() if 'created' in c.lower() and 'by' not in c.lower()), None)
            modified_by_col = next((c for c in row_data.keys() if ('modified' in c.lower() or 'edited' in c.lower() or 'updated' in c.lower()) and 'by' in c.lower()), None)
            modified_at_col = next((c for c in row_data.keys() if ('modified' in c.lower() or 'edited' in c.lower() or 'updated' in c.lower()) and 'by' not in c.lower()), None)
            
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

@ui_bp.route('/export/<table_name>')
@check_auth
async def export_table_csv(request, table_name):
    import csv
    import io
    
    user_id = request.ctx.user_id
    role = request.ctx.role
    table_name = table_name.lower()

    async with request.app.ctx.pool.acquire() as conn:
        auth_tables, table_modules = await get_authorized_tables(conn, user_id, role)
        if table_name not in auth_tables:
            raise NotFound("Table not found or unauthorized")

        pk_column = await get_pk_column(conn, table_name)
        columns_data = await get_table_columns(conn, table_name)

        export_cols = []
        company_csv_col = None
        for c in columns_data:
            cname = c['column_name']
            if cname == pk_column: continue
            
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
        order_clause = f" ORDER BY {quote_ident(pk_column)} DESC" if pk_column else ""
        query = f"SELECT {col_list} FROM {q_table}{order_clause}"

    table_title = auth_tables.get(table_name, table_name)
    filename = f"{table_title.replace(' ', '_')}_Export.csv"

    async def streaming_fn(res):
        header = [col.split('_', 1)[-1].replace('_', ' ').title() for col in export_cols]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        await res.write(output.getvalue())
        output.seek(0)
        output.truncate(0)

        async with request.app.ctx.pool.acquire() as conn:
            async with conn.transaction():
                async for row in conn.cursor(query):
                    csv_row = []
                    for col in export_cols:
                        val = row[col]
                        if val is None:
                            csv_row.append('')
                        elif isinstance(val, datetime):
                            csv_row.append(val.strftime('%Y-%m-%d'))
                        else:
                            csv_row.append(str(val))
                    writer.writerow(csv_row)
                    await res.write(output.getvalue())
                    output.seek(0)
                    output.truncate(0)

    res = response.stream(streaming_fn, content_type="text/csv")
    res.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return add_security_headers(res)

