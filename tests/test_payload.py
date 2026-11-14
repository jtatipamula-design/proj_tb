import os

os.environ.setdefault("JWT_SECRET", "test-secret")

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import bcrypt

from app.utils import (
    _sanitize_payload,
    generate_csrf_token,
    get_client_ip,
    validate_csrf_token,
)

SCHEMA = {
    "id": {"data_type": "integer"},
    "name": {"data_type": "character varying", "character_maximum_length": 50},
    "note": {"data_type": "text"},
    "price": {"data_type": "numeric"},
    "qty": {"data_type": "integer"},
    "active": {"data_type": "boolean"},
    "due": {"data_type": "date"},
    "password": {"data_type": "character varying", "character_maximum_length": 100},
}


def clean(data, is_update=False):
    return _sanitize_payload(data, "id", SCHEMA, is_update=is_update)


def test_empty_string_update_becomes_none_insert_skipped():
    assert clean({"name": ""}, is_update=True) == {"name": None}
    assert clean({"name": ""}) == {}


def test_password_is_hashed():
    out = clean({"password": "Secret123"})["password"]
    assert out.startswith("$2b$")
    assert bcrypt.checkpw(b"Secret123", out.encode())


def test_already_hashed_password_kept():
    h = bcrypt.hashpw(b"x", bcrypt.gensalt()).decode()
    assert clean({"password": h})["password"] == h


def test_numeric_is_exact_decimal():
    out = clean({"price": "19.99"})["price"]
    assert isinstance(out, Decimal) and out == Decimal("19.99")


def test_integer_parsing():
    assert clean({"qty": "42"})["qty"] == 42
    assert clean({"qty": "12345678901234567890"})["qty"] == 12345678901234567890
    assert clean({"qty": "abc"})["qty"] is None


def test_boolean_on_is_true():
    assert clean({"active": "on"})["active"] is True


def test_text_column_date_like_string_stays_str():
    assert clean({"note": "2024-01-01"})["note"] == "2024-01-01"


def test_date_column_parsed():
    assert clean({"due": "2024-01-01"})["due"] == date(2024, 1, 1)


def test_pk_skipped_on_update_and_audit_fields_dropped():
    out = clean({"id": "7", "name": "a", "created_at": "x", "modified_by": "y"}, is_update=True)
    assert out == {"name": "a"}


def test_csrf_tokens():
    tok = generate_csrf_token("sess1")
    assert validate_csrf_token(tok, "sess1") is True
    assert validate_csrf_token("wrong", "sess1") is False
    assert validate_csrf_token("", "sess1") is False
    assert validate_csrf_token(tok, "") is False
    assert validate_csrf_token(tok, "sess2") is False


def test_client_ip_ignores_spoofed_forwarded_header():
    req = SimpleNamespace(
        remote_addr="10.0.0.5",
        ip="192.168.1.1",
        headers={"x-forwarded-for": "6.6.6.6", "x-real-ip": "6.6.6.6"},
    )
    assert get_client_ip(req) == "10.0.0.5"
    req.remote_addr = ""
    assert get_client_ip(req) == "192.168.1.1"
