import os
os.environ.setdefault("JWT_SECRET", "test-secret")

import bcrypt
from app.routes.ui import form_field_value
from app.routes.api import apply_workflow_status_rule
from app.query import build_list_query, parse_list_args
from app.utils import workflow_status_column


# 1. The edit form never carries the stored hash (saving it would make the hash the new password).
def test_form_never_prefills_password_hash():
    stored = bcrypt.hashpw(b"Secret123", bcrypt.gensalt()).decode()
    assert form_field_value("pus_pwd", False, stored, True) == ""
    assert form_field_value("pus_name", False, "Asha", True) == "Asha"
    assert form_field_value("pdoc_file", True, b"%PDF", True) == "[Stored Binary Object]"
    assert form_field_value("pdoc_file", True, None, True) == ""


# 3. With an approval rule, saves cannot set the status; new records start as draft.
SCHEMA = {
    "pcc_name": {"column_name": "pcc_name", "character_maximum_length": 100},
    "pcc_status": {"column_name": "pcc_status", "character_maximum_length": 3},
}

def test_edit_cannot_set_workflow_status():
    data = {"pcc_name": "X", "pcc_status": "ACT"}
    apply_workflow_status_rule(data, SCHEMA, is_insert=False)
    assert data == {"pcc_name": "X"}

def test_new_record_starts_as_draft():
    data = {"pcc_name": "X", "pcc_status": "ACT"}
    apply_workflow_status_rule(data, SCHEMA, is_insert=True)
    assert data["pcc_status"] == "DFT"
    one_char = {"s_status": {"column_name": "s_status", "character_maximum_length": 1}}
    data = {}
    apply_workflow_status_rule(data, one_char, is_insert=True)
    assert data == {"s_status": "D"}

def test_table_without_status_column_is_untouched():
    data = {"pcc_name": "X"}
    apply_workflow_status_rule(data, {"pcc_name": SCHEMA["pcc_name"]}, is_insert=True)
    assert data == {"pcc_name": "X"}

def test_workflow_status_column():
    assert workflow_status_column(["pcc_id", "pcc_status"]) == "pcc_status"
    assert workflow_status_column(["doc_state"]) == "doc_state"
    assert workflow_status_column(["pcc_id", "pcc_name"]) is None


# 4. Password columns are never searched, filtered or sorted on (a prefix filter reveals the hash).
USER_COLS = [
    {"column_name": "pus_user_id", "data_type": "bigint"},
    {"column_name": "pus_user_name", "data_type": "character varying"},
    {"column_name": "pus_pwd", "data_type": "character varying"},
]

def test_password_column_not_filterable_or_searchable():
    args = parse_list_args({
        "q": "$2b$",
        "filters": '[{"col": "pus_pwd", "op": "starts_with", "val": "$2b$12$a"}]',
        "sort_rules": '[{"col": "pus_pwd", "dir": "asc"}]',
    })
    where_sql, params, order_sql = build_list_query(args, USER_COLS, "pus_user_id", (None, None), "phc_users_t")
    assert "pus_pwd" not in where_sql and "pus_pwd" not in order_sql
    assert "$2b$12$a" not in params
    assert "pus_user_name" in where_sql  # normal search still works


# 7. Without the right password, login never says whether the account exists, is locked or inactive.
from app.routes.auth import login_outcome

def test_login_outcome_hides_account_state_without_password():
    for inactive in (False, True):
        for locked in (False, True):
            assert login_outcome(False, inactive, locked) == "invalid"
    assert login_outcome(True, False, True) == "locked"
    assert login_outcome(True, True, False) == "inactive"
    assert login_outcome(True, False, False) == "ok"


# 8. One rule for audit columns; numeric *_by columns get the user id, not the username.
from datetime import datetime
from app.utils import audit_column_kind, audit_stamp

def test_audit_column_kind():
    assert audit_column_kind("pmat_created_by") == "created_by"
    assert audit_column_kind("pcc_created") == "created_at"
    assert audit_column_kind("pum_last_updated_by") == "modified_by"
    assert audit_column_kind("pum_last_updated_date") == "modified_at"
    assert audit_column_kind("pmd_edited_on") == "modified_at"
    assert audit_column_kind("pcc_name") is None

def test_audit_stamp_numeric_by_columns_get_user_id():
    now = datetime(2026, 11, 21, 10, 0)
    schema = {
        "pum_name": {"data_type": "character varying"},
        "pum_created_by": {"data_type": "bigint"},
        "pum_created": {"data_type": "timestamp without time zone"},
        "pum_last_updated_by": {"data_type": "character varying", "character_maximum_length": 5},
        "pum_last_updated_date": {"data_type": "timestamp without time zone"},
    }
    assert audit_stamp(schema, 42, "administrator", now, is_insert=True) == {
        "pum_created_by": 42, "pum_created": now, "pum_last_updated_by": "admin", "pum_last_updated_date": now}
    assert audit_stamp(schema, 42, "administrator", now, is_insert=False) == {
        "pum_last_updated_by": "admin", "pum_last_updated_date": now}
