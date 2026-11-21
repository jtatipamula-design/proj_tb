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
