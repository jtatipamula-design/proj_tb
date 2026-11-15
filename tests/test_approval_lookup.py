import os
os.environ.setdefault("JWT_SECRET", "test-secret")

import asyncio
from app.utils import get_approval_workflow_info


class _Savepoint:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.savepoints += 1

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.rolled_back += exc_type is not None
        return False


class _BrokenSchemaConn:
    """Approval setup query fails, like a phc_approval_setup_t without pas_table_name."""
    def __init__(self):
        self.savepoints = 0
        self.rolled_back = 0

    def transaction(self):
        return _Savepoint(self)

    async def fetchrow(self, *args):
        raise RuntimeError("column s.pas_type_id does not exist")


def test_failed_setup_lookup_is_isolated_in_a_savepoint():
    # Without the savepoint the error aborts the caller's save transaction and every edit/delete fails.
    conn = _BrokenSchemaConn()
    result = asyncio.run(get_approval_workflow_info(conn, "phc_cost_center_t", "2", {"pcc_status": "ACT"}, 1, "ADM"))
    assert result is None
    assert conn.savepoints == 1 and conn.rolled_back == 1
