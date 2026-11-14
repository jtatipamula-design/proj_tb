import os
os.environ.setdefault("JWT_SECRET", "test-secret")

import asyncio
import json
import time
from types import SimpleNamespace

import jwt

from app import auth


def test_build_modules_tree_empty_returns_tuple():
    result = asyncio.run(auth.build_modules_tree(None, {}, {}, {"x": []}))
    assert result == ({}, {"x": []})


def _request(token=None, path="/api/data"):
    cookies = {"auth_token": token} if token else {}
    # app.ctx has no pool: any request that reached the DB branch would get 503, not 401
    return SimpleNamespace(cookies=cookies, path=path, ctx=SimpleNamespace(),
                           app=SimpleNamespace(ctx=SimpleNamespace()))


async def _handler(request):
    raise AssertionError("handler must not run")

_protected = auth.check_auth(_handler)


def _assert_401(token):
    res = asyncio.run(_protected(_request(token)))
    assert res.status == 401
    assert json.loads(res.body) == {"error": "Unauthorized"}


def test_check_auth_missing_cookie():
    _assert_401(None)


def test_check_auth_wrong_secret():
    _assert_401(jwt.encode({"user_id": 1, "session_id": "s"}, "other-secret", algorithm="HS256"))


def test_check_auth_expired_token():
    token = jwt.encode({"user_id": 1, "session_id": "s", "exp": int(time.time()) - 60},
                       auth.JWT_SECRET, algorithm="HS256")
    _assert_401(token)


def test_rate_limiter_blocks_and_clears():
    auth.LOGIN_ATTEMPTS.clear()
    ip = "10.0.0.1"
    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        assert auth.check_login_rate_limit(ip)[0]
        auth.record_login_attempt(ip)
    allowed, wait = auth.check_login_rate_limit(ip)
    assert not allowed and wait > 0
    auth.record_login_attempt(ip, success=True)
    assert auth.check_login_rate_limit(ip) == (True, 0)


def test_rate_limiter_sweeps_stale_ips():
    auth.LOGIN_ATTEMPTS.clear()
    stale = time.time() - auth.RATE_LIMIT_WINDOW - 1
    for i in range(1500):
        auth.LOGIN_ATTEMPTS[f"1.1.{i // 256}.{i % 256}"] = [stale]
    auth.LOGIN_ATTEMPTS["fresh"] = [time.time()]
    auth.record_login_attempt("2.2.2.2")
    assert set(auth.LOGIN_ATTEMPTS) == {"fresh", "2.2.2.2"}
