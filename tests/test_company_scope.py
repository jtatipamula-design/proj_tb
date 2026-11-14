import os
os.environ.setdefault("JWT_SECRET", "test-secret")

import asyncio
from app.database import SCHEMA_CACHE, get_fk_display_dict
from app.routes import api


class FakeConn:
    """Records SQL and answers the few queries these helpers make."""
    def __init__(self, company=7, row_exists=True, cols=("pmat_material_id", "pmat_material_name", "pmat_company_id", "pmat_status")):
        self.company, self.row_exists, self.cols, self.sql = company, row_exists, cols, []

    async def fetchval(self, q, *args):
        self.sql.append((q, args))
        if "pus_company_id" in q:
            return self.company
        return 1 if self.row_exists else None

    async def fetch(self, q, *args):
        self.sql.append((q, args))
        if "information_schema.columns" in q:
            return [{"column_name": c} for c in self.cols]
        return []


def _patch_schema(monkeypatch, cols):
    async def pk(conn, t):
        return cols[0]
    async def tcols(conn, t):
        return [{"column_name": c, "data_type": "integer"} for c in cols]
    monkeypatch.setattr(api, "get_pk_column", pk)
    monkeypatch.setattr(api, "get_table_columns", tcols)


def test_record_visible_filters_by_company(monkeypatch):
    cols = ["pmat_material_id", "pmat_company_id"]
    _patch_schema(monkeypatch, cols)
    conn = FakeConn(company=7, row_exists=False)
    assert asyncio.run(api.record_visible(conn, "phc_material_master_t", "5", 1)) is False
    q, args = conn.sql[-1]
    assert '"pmat_company_id" = $2' in q and args == (5, 7)


def test_record_visible_without_company_column(monkeypatch):
    _patch_schema(monkeypatch, ["pxx_id", "pxx_name"])
    assert asyncio.run(api.record_visible(FakeConn(), "phc_misc_t", "5", 1)) is True


def test_record_visible_rejects_bad_id(monkeypatch):
    _patch_schema(monkeypatch, ["pmat_material_id", "pmat_company_id"])
    assert asyncio.run(api.record_visible(FakeConn(), "phc_material_master_t", "abc", 1)) is False


def test_dropdown_options_filtered_by_company():
    SCHEMA_CACHE["display_cols"].pop("phc_material_master_t", None)
    conn = FakeConn()
    asyncio.run(get_fk_display_dict(conn, "phc_material_master_t", "pmat_material_id", company_id=7))
    q, args = conn.sql[-1]
    assert '"pmat_company_id" = $1' in q and "'ACT'" in q and args == (7,)


def test_company_table_itself_is_not_filtered():
    SCHEMA_CACHE["display_cols"].pop("phc_companies_t", None)
    conn = FakeConn(cols=("pco_company_id", "pco_company_name"))
    asyncio.run(get_fk_display_dict(conn, "phc_companies_t", "pco_company_id", company_id=7))
    q, args = conn.sql[-1]
    assert "WHERE" not in q and args == ()
