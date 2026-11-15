import os
os.environ.setdefault("JWT_SECRET", "test-secret")

import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from app.query import parse_list_args, build_list_query

COLS = [
    {"column_name": "pm_id", "data_type": "integer"},
    {"column_name": "pm_name", "data_type": "character varying"},
    {"column_name": "pm_qty", "data_type": "numeric"},
    {"column_name": "pm_active", "data_type": "boolean"},
    {"column_name": "pm_made_date", "data_type": "timestamp without time zone"},
    {"column_name": "pm_photo", "data_type": "bytea"},
    {"column_name": "pm_company_id", "data_type": "integer"},
]
NO_SCOPE = (None, None)


def build(args=None, scope=NO_SCOPE, table="pm_t", pk="pm_id", **filt):
    args = dict(args or {})
    if filt:
        args["filters"] = json.dumps([filt])
    return build_list_query(parse_list_args(args), COLS, pk, scope, table)


def test_no_args_gives_pk_desc_and_no_where():
    assert build() == ("", [], ' ORDER BY "pm_id" DESC')


@pytest.mark.parametrize("op,val,clause,params", [
    ("is_empty", "", '("pm_name" IS NULL OR CAST("pm_name" AS TEXT) = \'\')', []),
    ("is_not_empty", "", '("pm_name" IS NOT NULL AND CAST("pm_name" AS TEXT) != \'\')', []),
    ("contains", "ab", 'CAST("pm_name" AS TEXT) ILIKE $1', ["%ab%"]),
    ("not_contains", "ab", '(CAST("pm_name" AS TEXT) NOT ILIKE $1 OR "pm_name" IS NULL)', ["%ab%"]),
    ("starts_with", "ab", 'CAST("pm_name" AS TEXT) ILIKE $1', ["ab%"]),
    ("ends_with", "ab", 'CAST("pm_name" AS TEXT) ILIKE $1', ["%ab"]),
    ("equals", "Ab", 'LOWER(CAST("pm_name" AS TEXT)) = LOWER($1)', ["Ab"]),
    ("not_equals", "Ab", '(LOWER(CAST("pm_name" AS TEXT)) != LOWER($1) OR "pm_name" IS NULL)', ["Ab"]),
])
def test_text_operators(op, val, clause, params):
    assert build(col="pm_name", op=op, val=val)[:2] == (" WHERE " + clause, params)


@pytest.mark.parametrize("op,val,clause,params", [
    ("equals", "5", '"pm_qty" = $1', [5]),
    ("equals", "5.5", '"pm_qty" = $1', [5.5]),
    ("not_equals", "5", '("pm_qty" != $1 OR "pm_qty" IS NULL)', [5]),
    ("greater_than", "5", '"pm_qty" > $1', [5]),
    ("less_than", "5", '"pm_qty" < $1', [5]),
])
def test_numeric_operators(op, val, clause, params):
    assert build(col="pm_qty", op=op, val=val)[:2] == (" WHERE " + clause, params)


@pytest.mark.parametrize("op", ["equals", "not_equals", "greater_than", "less_than"])
def test_non_numeric_value_on_numeric_column_is_dropped(op):
    assert build(col="pm_qty", op=op, val="abc")[:2] == ("", [])


def test_boolean_and_date_typing():
    assert build(col="pm_active", op="equals", val="yes")[:2] == (' WHERE "pm_active" = $1', [True])
    assert build(col="pm_active", op="not_equals", val="no")[:2] == (' WHERE ("pm_active" != $1 OR "pm_active" IS NULL)', [False])
    assert build(col="pm_made_date", op="greater_than", val="2024-01-01")[:2] == (' WHERE "pm_made_date" > $1::text::timestamp', ["2024-01-01"])
    assert build(col="pm_made_date", op="less_than", val="2024-01-01")[:2] == (' WHERE "pm_made_date" < $1::text::timestamp', ["2024-01-01"])
    # greater/less than means nothing for plain text columns
    assert build(col="pm_name", op="greater_than", val="a")[:2] == ("", [])


def test_empty_value_and_unknown_operator_are_ignored():
    assert build(col="pm_name", op="contains", val="")[:2] == ("", [])
    assert build(col="pm_name", op="regex", val="x")[:2] == ("", [])


def test_unknown_or_injected_column_is_ignored():
    evil = 'pm_name"; DROP TABLE pm_t; --'
    where, params, order = build(
        {"sort_rules": json.dumps([{"col": evil, "dir": "asc"}])}, col=evil, op="contains", val="x")
    assert (where, params, order) == ("", [], ' ORDER BY "pm_id" DESC')


def test_column_names_only_go_through_quote_ident(monkeypatch):
    import app.query as query
    monkeypatch.setattr(query, "quote_ident", lambda name: f"<{name}>")
    where, _, order = query.build_list_query(
        parse_list_args({"q": "x", "sort_col": "pm_name", "filters": json.dumps([{"col": "pm_qty", "op": "equals", "val": "1"}])}),
        COLS, "pm_id", ("pm_company_id", 7), "pm_t")
    for c in COLS:
        assert c["column_name"] not in (where + order).replace(f"<{c['column_name']}>", "")


def test_param_numbering_with_company_scope_search_and_filters():
    where, params, _ = build(
        {"q": "foo", "filters": json.dumps([
            {"col": "pm_name", "op": "contains", "val": "a"},
            {"col": "pm_qty", "op": "greater_than", "val": "3"}])},
        scope=("pm_company_id", 7))
    assert params == [7, "%foo%", "%a%", 3]
    assert where.startswith(' WHERE "pm_company_id" = $1 AND (CAST("pm_id" AS TEXT) ILIKE $2 OR ')
    assert where.endswith(' AND CAST("pm_name" AS TEXT) ILIKE $3 AND "pm_qty" > $4')
    assert "pm_photo" not in where  # bytea is not searchable


def test_type_filter_only_applies_to_lookup_values_table():
    assert build({"type_filter": "UOM"})[:2] == ("", [])
    assert build({"type_filter": "UOM"}, table="phc_lookup_values_t", scope=("pm_company_id", 7))[:2] == (
        ' WHERE "pm_company_id" = $1 AND "plv_lookup_type_code" = $2', [7, "UOM"])


def test_sorting():
    assert build({"sort_col": "pm_name", "sort_dir": "asc"})[2] == ' ORDER BY "pm_name" ASC, "pm_id" DESC'
    assert build({"sort_col": "pm_name", "sort_dir": "sideways"})[2] == ' ORDER BY "pm_name" DESC, "pm_id" DESC'
    rules = json.dumps([{"col": "pm_qty", "dir": "desc"}, {"col": "pm_id", "dir": "asc"}, {"col": "nope", "dir": "asc"}])
    assert build({"sort_rules": rules, "sort_col": "pm_name"})[2] == ' ORDER BY "pm_qty" DESC, "pm_id" ASC'
    assert build(pk=None)[2] == ""


def test_malformed_json_args_are_ignored():
    parsed = parse_list_args({"filters": "{not json", "sort_rules": '"a string"', "q": "  hi  "})
    assert (parsed["filters"], parsed["sort_rules"], parsed["q"]) == ([], [], "hi")
    assert parse_list_args({"filters": '[1, {"col": "a"}, {"col": " a ", "op": " EQUALS ", "val": 5}]'})["filters"] == [
        {"col": "a", "op": "equals", "val": "5"}]


# --- read API: serialisation and routing (no database) ---

def test_read_api_serialisation_hides_passwords_and_binary():
    from app.routes.read_api import json_row, select_list
    row = {"pus_user_id": 1, "pus_pwd": "hash", "pus_photo": b"\x00\x01", "pus_rate": Decimal("1.50"),
           "pus_created_date": datetime(2024, 1, 2, 3, 4, 5), "pus_dob": date(2000, 1, 2), "pus_name": None}
    out = json_row(row)
    assert out == {"pus_user_id": 1, "pus_photo": None, "pus_rate": "1.50",
                   "pus_created_date": "2024-01-02T03:04:05", "pus_dob": "2000-01-02", "pus_name": None}
    json.dumps(out)
    cols = [{"column_name": "pus_user_id", "data_type": "integer"}, {"column_name": "pus_pwd", "data_type": "text"},
            {"column_name": "pus_photo", "data_type": "bytea"}]
    assert select_list(cols) == '"pus_user_id", NULL AS "pus_photo"'


def test_read_routes_do_not_shadow_existing_api_routes():
    from sanic import Sanic
    from app.routes.api import api_bp
    from app.routes.read_api import read_api_bp
    Sanic.test_mode = True
    app = Sanic("read_api_route_check")
    app.blueprint(api_bp)
    app.blueprint(read_api_bp)
    app.router.finalize()

    def handler(path, method):
        route, _, _ = app.router.get(path, method, None)
        return route.name.rsplit(".", 1)[-1]

    assert handler("/api/some_table", "GET") == "api_list"
    assert handler("/api/some_table/5", "GET") == "api_get"
    assert handler("/api/some_table", "POST") == "api_create"
    for m in ("PUT", "DELETE", "POST"):
        assert handler("/api/some_table/5", m) == "api_update_delete"
    assert handler("/api/notifications", "GET") == "get_user_notifications"
    assert handler("/api/notifications/3/read", "POST") == "mark_notification_read"
    assert handler("/api/audit/some_table/5", "GET") == "get_record_audit_history"
    assert handler("/api/workflow/history/some_table/5", "GET") == "get_workflow_history"
    assert handler("/api/workflow/transition", "POST") == "api_workflow_transition"
    assert handler("/api/file/some_table/5/col", "GET") == "download_file_column"
