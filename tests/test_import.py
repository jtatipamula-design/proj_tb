import os

os.environ.setdefault("JWT_SECRET", "test-secret")

from datetime import datetime

import pytest

from app.routes.importer import CsvImportError, build_insert_row, export_label, map_headers, parse_csv

COLS = ["pit_item_id", "pit_item_name", "pit_unit_price", "pit_company_id"]

SCHEMA = {
    "pit_item_id": {"data_type": "integer", "column_default": "nextval('s')"},
    "pit_item_name": {"data_type": "character varying", "character_maximum_length": 50},
    "pit_unit_price": {"data_type": "numeric"},
    "pit_photo": {"data_type": "bytea"},
    "pit_company_id": {"data_type": "integer"},
    "pit_start_date": {"data_type": "date"},
    "pit_created": {"data_type": "timestamp without time zone"},
    "pit_created_by": {"data_type": "character varying", "character_maximum_length": 5},
    "pit_modified": {"data_type": "timestamp without time zone"},
    "pit_modified_by": {"data_type": "character varying", "character_maximum_length": 50},
}
NOW = datetime(2026, 1, 2, 3, 4, 5)


def build(raw, table="phc_items_t", scope=("pit_company_id", 7), role="STD"):
    return build_insert_row(raw, table, SCHEMA, "pit_item_id", scope, role, "alice_long", NOW)


def test_export_label_matches_export():
    assert export_label("pit_item_name") == "Item Name"


def test_map_headers_raw_names_and_labels():
    mapped, unknown = map_headers(["pit_item_name", " Unit Price ", "ITEM ID"], COLS)
    assert mapped == ["pit_item_name", "pit_unit_price", "pit_item_id"]
    assert unknown == []


def test_map_headers_reports_unknown():
    mapped, unknown = map_headers(["Item Name", "Colour", ""], COLS)
    assert mapped == ["pit_item_name", None, None]
    assert unknown == ["Colour", ""]


def test_map_headers_raw_name_beats_label():
    # 'x_name' has label 'Name', which is also a raw column
    assert map_headers(["name"], ["x_name", "name"])[0] == ["name"]


def test_parse_basic_and_row_numbers_skip_blank_lines():
    rows = parse_csv(b"Item Name,Unit Price\r\nBolt,1.50\r\n\r\n,\r\nNut,\r\n", COLS)
    assert rows == [
        (1, {"pit_item_name": "Bolt", "pit_unit_price": "1.50"}),
        (4, {"pit_item_name": "Nut", "pit_unit_price": ""}),
    ]


def test_parse_handles_bom():
    rows = parse_csv("﻿Item Name\nBolt\n".encode("utf-8"), COLS)
    assert rows == [(1, {"pit_item_name": "Bolt"})]


def test_parse_strips_export_formula_prefix_only():
    rows = parse_csv(b"Item Name\n'=SUM(A1)\n'-5\n'quoted'\n'\n=raw\n", COLS)
    assert [r["pit_item_name"] for _, r in rows] == ["=SUM(A1)", "-5", "'quoted'", "'", "=raw"]


def test_parse_quoted_newline_and_comma():
    rows = parse_csv(b'Item Name,Unit Price\n"a,\nb",2\n', COLS)
    assert rows == [(1, {"pit_item_name": "a,\nb", "pit_unit_price": "2"})]


@pytest.mark.parametrize("body", [b"", b"\n", b"\xef\xbb\xbf", b",,\n"])
def test_parse_empty_file(body):
    with pytest.raises(CsvImportError, match="empty"):
        parse_csv(body, COLS)


def test_parse_header_only():
    with pytest.raises(CsvImportError, match="no data rows"):
        parse_csv(b"Item Name\n", COLS)


def test_parse_unknown_headers_listed():
    with pytest.raises(CsvImportError, match="Unknown column.*'Colour'.*'Size'"):
        parse_csv(b"Item Name,Colour,Size\nBolt,red,3\n", COLS)


def test_parse_duplicate_header():
    with pytest.raises(CsvImportError, match="same column"):
        parse_csv(b"Item Name,pit_item_name\na,b\n", COLS)


def test_parse_row_cap():
    body = b"Item Name\n" + b"x\n" * 3
    assert len(parse_csv(body, COLS, max_rows=3)) == 3
    with pytest.raises(CsvImportError, match="Too many rows"):
        parse_csv(body, COLS, max_rows=2)


def test_parse_too_many_cells_reports_row():
    with pytest.raises(CsvImportError) as exc:
        parse_csv(b"Item Name\nok\nbad,extra\n", COLS)
    assert exc.value.row == 2


def test_parse_not_utf8():
    with pytest.raises(CsvImportError, match="UTF-8"):
        parse_csv(b"Item Name\n\xff\xfe\n", COLS)


def test_build_row_forces_company_and_fills_who_columns():
    out = build({"pit_item_name": "Bolt", "pit_unit_price": "1.5", "pit_company_id": "99",
                 "pit_created_by": "mallory", "pit_photo": "[Binary Object (3 bytes)]"})
    assert out["pit_company_id"] == 7
    assert out["pit_created_by"] == "alice"  # truncated to the column length, CSV value ignored
    assert out["pit_modified_by"] == "alice_long"
    assert out["pit_created"] == NOW and out["pit_modified"] == NOW
    assert out["pit_start_date"] == NOW
    assert "pit_photo" not in out
    assert "pit_item_id" not in out


def test_build_row_keeps_given_start_date_and_no_scope():
    out = build({"pit_item_name": "Bolt", "pit_start_date": "2024-05-06", "pit_company_id": "3"}, scope=(None, None))
    assert out["pit_start_date"].isoformat() == "2024-05-06"
    assert out["pit_company_id"] == 3


def test_build_row_admin_screens_company_exception():
    assert build({"pit_company_id": "3"}, table="phc_screens_t", role="ADM")["pit_company_id"] == 3
    assert build({"pit_company_id": ""}, table="phc_screens_t", role="ADM")["pit_company_id"] == 7
    assert build({"pit_company_id": "3"}, table="phc_screens_t", role="STD")["pit_company_id"] == 7
