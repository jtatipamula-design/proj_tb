import os
os.environ.setdefault("JWT_SECRET", "test-secret")

from datetime import date, datetime

from asyncpg import exceptions as pgexc

from app.routes.api import row_version_conflict, db_error_response


def test_no_version_sent_means_no_check():
    assert row_version_conflict(None, datetime(2026, 1, 1, 10, 0)) is False


def test_same_version_passes():
    ts = datetime(2026, 1, 1, 10, 0, 5, 123456)
    assert row_version_conflict(ts.isoformat(), ts) is False
    assert row_version_conflict(date(2026, 1, 1).isoformat(), date(2026, 1, 1)) is False


def test_changed_row_conflicts():
    assert row_version_conflict(datetime(2026, 1, 1, 10, 0).isoformat(), datetime(2026, 1, 1, 10, 1)) is True
    assert row_version_conflict("2026-01-01", date(2026, 1, 2)) is True


def test_never_modified_row():
    assert row_version_conflict("", None) is False
    assert row_version_conflict("", datetime(2026, 1, 1)) is True  # modified since the form was opened


def test_unique_violation():
    e = pgexc.UniqueViolationError("duplicate key value violates unique constraint")
    e.constraint_name = "pxx_code_key"
    assert db_error_response(e, False) == ("A record with the same pxx_code_key already exists.", 409)


def test_foreign_key_violation_depends_on_action():
    e = pgexc.ForeignKeyViolationError("violates foreign key constraint")
    assert db_error_response(e, True) == ("This record is referenced by other records and cannot be deleted.", 409)
    assert db_error_response(e, False) == ("A referenced record does not exist.", 409)


def test_not_null_names_the_column():
    e = pgexc.NotNullViolationError("null value in column")
    e.column_name = "pxx_name"
    assert db_error_response(e, False) == ("'pxx_name' is required.", 400)


def test_value_errors_are_400_with_plain_text():
    for cls in (pgexc.StringDataRightTruncationError, pgexc.NumericValueOutOfRangeError, pgexc.CheckViolationError):
        msg, status = db_error_response(cls("SELECT secret FROM t"), False)
        assert status == 400 and "secret" not in msg and "Could not save" not in msg


def test_unknown_errors_stay_generic():
    msg, status = db_error_response(RuntimeError("UPDATE t SET x = 1 -- raw sql"), False)
    assert status == 400 and "raw sql" not in msg and msg.startswith("Could not save")
    assert db_error_response(RuntimeError("boom"), True) == ("Could not delete the record.", 400)
