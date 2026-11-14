import os
os.environ.setdefault("JWT_SECRET", "test-secret")

from app.routes.api import write_block_reason, insert_pk_missing


def test_non_admin_blocked_from_admin_tables():
    for t in ("phc_users_t", "phc_companies_t", "phc_approval_setup_t"):
        assert write_block_reason(t, "STD", "General")


def test_non_admin_blocked_from_any_erpadmin_module_table():
    assert write_block_reason("phc_some_new_admin_t", "STD", "ERPAdmin")


def test_admin_allowed_on_admin_tables():
    assert write_block_reason("phc_companies_t", "ADM", "ERPAdmin") is None


def test_normal_table_allowed_for_everyone():
    assert write_block_reason("pcv_equipments_t", "STD", "Cleaning Validation") is None


def test_append_only_tables_blocked_even_for_admin():
    for t in ("phc_audit_log_t", "phc_approval_events_t"):
        assert write_block_reason(t, "ADM", "ERPAdmin")


def test_insert_needs_pk_when_no_default_or_identity():
    schema = {"pxx_id": {"column_default": None, "is_identity": "NO"}}
    assert insert_pk_missing(schema, "pxx_id", {}) is True
    assert insert_pk_missing(schema, "pxx_id", {"pxx_id": ""}) is True
    assert insert_pk_missing(schema, "pxx_id", {"pxx_id": 5}) is False


def test_insert_ok_with_sequence_or_identity():
    assert insert_pk_missing({"a": {"column_default": "nextval('s'::regclass)", "is_identity": "NO"}}, "a", {}) is False
    assert insert_pk_missing({"a": {"column_default": None, "is_identity": "YES"}}, "a", {}) is False
