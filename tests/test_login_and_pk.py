import os
import asyncio
from datetime import datetime, timedelta, timezone

os.environ.setdefault("JWT_SECRET", "test-secret")

from app.database import get_pk_column, preload_all_pks, SCHEMA_CACHE  # noqa: E402
from app.routes.auth import next_failed_count  # noqa: E402


class FakeConn:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, query, *args):
        return self.rows


def test_pk_single_column():
    SCHEMA_CACHE["pks"].pop("t_single", None)
    conn = FakeConn([{"attname": "id"}])
    assert asyncio.run(get_pk_column(conn, "t_single")) == "id"
    assert SCHEMA_CACHE["pks"]["t_single"] == "id"


def test_pk_composite_returns_none():
    SCHEMA_CACHE["pks"].pop("t_comp", None)
    conn = FakeConn([{"attname": "role_id"}, {"attname": "screen_id"}])
    assert asyncio.run(get_pk_column(conn, "t_comp")) is None
    assert "t_comp" not in SCHEMA_CACHE["pks"]


def test_preload_skips_composite():
    SCHEMA_CACHE["pks"].pop("p_single", None)
    SCHEMA_CACHE["pks"].pop("p_comp", None)
    SCHEMA_CACHE["pks_loaded"] = False
    conn = FakeConn([
        {"table_name": "p_single", "column_name": "id"},
        {"table_name": "p_comp", "column_name": "role_id"},
        {"table_name": "p_comp", "column_name": "screen_id"},
    ])
    asyncio.run(preload_all_pks(conn))
    assert SCHEMA_CACHE["pks"]["p_single"] == "id"
    assert "p_comp" not in SCHEMA_CACHE["pks"]
    SCHEMA_CACHE["pks_loaded"] = False


NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_expired_lock_resets():
    assert next_failed_count(5, NOW - timedelta(minutes=1), NOW) == 1


def test_no_lock_increments():
    assert next_failed_count(2, None, NOW) == 3
    assert next_failed_count(None, None, NOW) == 1


def test_active_lock_increments():
    assert next_failed_count(5, NOW + timedelta(minutes=5), NOW) == 6


def test_naive_locked_until_is_utc():
    # 12:30 naive = 12:30 UTC -> still active (would be expired if read as IST)
    assert next_failed_count(5, datetime(2026, 1, 1, 12, 30), NOW) == 6
    assert next_failed_count(5, datetime(2026, 1, 1, 11, 30), NOW) == 1
