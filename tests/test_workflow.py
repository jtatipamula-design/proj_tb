import os
os.environ.setdefault("JWT_SECRET", "test-secret")

import asyncio
import pytest
from app.routes import api
from app.routes.api import transition_error


@pytest.mark.parametrize("transition", ["APPROVE", "REJECT", "RECALL"])
def test_only_pending_records_can_be_decided(transition):
    assert transition_error(transition, "PND", None, 1) is None
    assert transition_error(transition, "p", None, 1) is None
    for status in ("DFT", "ACT", "REJ"):
        msg, code = transition_error(transition, status, None, 1)
        assert code == 400 and "pending" in msg


def test_submit_not_from_pending():
    assert transition_error("SUBMIT", "DFT", None, 1) is None
    assert transition_error("SUBMIT", "REJ", None, 1) is None
    assert transition_error("SUBMIT", "PND", None, 1) == ("This record is already pending approval.", 400)


def test_submitter_cannot_approve_own_record():
    msg, code = transition_error("APPROVE", "PND", 5, 5)
    assert code == 403
    assert transition_error("APPROVE", "PND", 5, 6) is None
    assert transition_error("APPROVE", "PND", None, 5) is None


def test_submitter_may_recall_own_record():
    assert transition_error("RECALL", "PND", 5, 5) is None


def test_unknown_transition_refused():
    msg, code = transition_error("DELETE", "PND", None, 1)
    assert code == 400 and "Unknown" in msg


class _Conn:
    def __init__(self, exists_in_company, exists_at_all):
        self.answers = [1 if exists_in_company else None, 1 if exists_at_all else None]

    async def fetchval(self, q, *args):
        if "pus_company_id" in q:
            return 7
        return self.answers.pop(0)


def _patch(monkeypatch):
    async def pk(conn, t):
        return "pmat_material_id"
    async def cols(conn, t):
        return [{"column_name": "pmat_material_id", "data_type": "integer"},
                {"column_name": "pmat_company_id", "data_type": "integer"}]
    monkeypatch.setattr(api, "get_pk_column", pk)
    monkeypatch.setattr(api, "get_table_columns", cols)
    api._USER_COMPANY_CACHE.clear()


def test_deleted_record_history_visible_to_admin_only(monkeypatch):
    _patch(monkeypatch)
    rv = lambda role: asyncio.run(api.record_visible(_Conn(False, False), "t", "5", 1, role))
    assert rv("ADM") is True
    assert rv("USR") is False


def test_other_company_record_hidden_even_from_admin(monkeypatch):
    _patch(monkeypatch)
    assert asyncio.run(api.record_visible(_Conn(False, True), "t", "5", 1, "ADM")) is False
