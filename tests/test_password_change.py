import os

os.environ.setdefault("JWT_SECRET", "test-secret")

from sanic import response  # noqa: E402

from app.routes.auth import password_change_error  # noqa: E402
from app.utils import add_security_headers, render_template  # noqa: E402

GOOD = "Str0ngPass"


def test_valid_change_is_allowed():
    assert password_change_error(True, GOOD, GOOD, False) is None


def test_wrong_current_password_wins_over_everything():
    assert "Current password" in password_change_error(False, GOOD, GOOD, False)
    assert "Current password" in password_change_error(False, "x", "y", True)


def test_confirmation_must_match():
    assert "do not match" in password_change_error(True, GOOD, GOOD + "x", False)


def test_strength_is_enforced():
    assert "8 characters" in password_change_error(True, "Sh0rt", "Sh0rt", False)
    assert "uppercase" in password_change_error(True, "alllower1", "alllower1", False)
    assert "digit" in password_change_error(True, "NoDigitsHere", "NoDigitsHere", False)


def test_over_72_bytes_rejected():
    long_pwd = "Aa1" + "x" * 70
    assert "72 bytes" in password_change_error(True, long_pwd, long_pwd, False)


def test_new_must_differ_from_current():
    assert "different" in password_change_error(True, GOOD, GOOD, True)


def test_security_headers_present():
    h = add_security_headers(response.json({})).headers
    assert h["Strict-Transport-Security"] == "max-age=31536000; includeSubDomains"
    csp = h["Content-Security-Policy"]
    for directive in ("default-src 'self'", "frame-ancestors 'self'", "base-uri 'self'",
                      "form-action 'self'", "object-src 'none'", "img-src 'self' data: blob:",
                      "https://unpkg.com", "https://fonts.googleapis.com", "https://fonts.gstatic.com"):
        assert directive in csp
    assert "unsafe-eval" not in csp


def test_change_password_page_renders_with_menu_link():
    html = render_template("change_password.html").body.decode()
    assert 'id="pwdForm"' in html
    assert 'href="/account/password"' in html
