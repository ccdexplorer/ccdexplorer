"""The api account session: its cookie, its logout, and what may change state.

The cookie carried only HttpOnly and lived on the static per-user token, so a
logout that merely deleted it left the token valid for anyone who had copied
it, and a password reset did not touch it either. The pages that change an
account (plan, keys) trusted any request that carried the cookie.
"""

import datetime as dt
from types import SimpleNamespace

import httpx2 as httpx
import pytest
from fastapi import HTTPException, Response

from conftest import build_test_app
from ccdexplorer.ccdexplorer_api.app import security
from ccdexplorer.ccdexplorer_api.app.routers.auth import auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password

# --- the cookie -----------------------------------------------------------


def _cookie(monkeypatch=None, https=True):
    if monkeypatch is not None:
        monkeypatch.setattr(security, "API_URL", "https://x" if https else "http://localhost")
    response = Response()
    security.set_session_cookie(response, "tok")
    return response.headers["set-cookie"].lower()


def test_the_cookie_is_httponly(monkeypatch):
    assert "httponly" in _cookie(monkeypatch)


def test_the_cookie_is_not_sent_cross_site(monkeypatch):
    assert "samesite=lax" in _cookie(monkeypatch)


def test_the_cookie_is_https_only_where_the_api_is_served_over_https(monkeypatch):
    assert "secure" in _cookie(monkeypatch, https=True)


def test_a_local_http_api_can_still_log_in(monkeypatch):
    """Secure on http://localhost would mean no cookie ever comes back."""
    assert "secure" not in _cookie(monkeypatch, https=False)


# --- logout ---------------------------------------------------------------


def _route(app, path, method):
    for r in app.routes:
        if getattr(r, "path", None) == path and method in getattr(r, "methods", set()):
            return r
    return None


@pytest.fixture(scope="module")
def app():
    return build_test_app()


def test_logout_is_a_post(app):
    assert _route(app, "/auth/logout", "POST") is not None


async def test_a_plain_get_does_not_log_anyone_out(app):
    """A link, an image tag or a redirect from another site is a GET. What it
    reaches has no database to write to and no cookie to delete."""
    route = _route(app, "/auth/logout", "GET")
    assert route is not None and route.endpoint is auth.logout_get
    response = await auth.logout_get()
    assert response.status_code == 303
    assert "set-cookie" not in response.headers


class _Collection:
    def __init__(self):
        self.writes = []

    async def bulk_write(self, ops):
        self.writes.append(ops)


def _user():
    from ccdexplorer.ccdexplorer_api.app.models import User

    return User(
        scope="http://x",
        token="old-token",
        alias_id=0,
        alias_account_id="3alias",
        api_account_id="acct",
        email="a@example.org",
        password=hash_password("pw"),
        plan_end_date=dt.datetime.now().astimezone(dt.UTC),
    )


async def test_logging_out_retires_the_token(monkeypatch):
    users = _Collection()
    monkeypatch.setattr(auth, "get_user_details", lambda request: _user())
    db = SimpleNamespace(utilities_db={"api_users": users})
    request = SimpleNamespace(app=SimpleNamespace(user=object()))

    response = await auth.logout(request, Response(), db)

    stored = users.writes[0][0]._doc
    assert stored["token"] != "old-token"
    assert response.status_code in (302, 303)


async def test_logging_out_with_no_session_is_harmless(monkeypatch):
    monkeypatch.setattr(auth, "get_user_details", lambda request: None)
    db = SimpleNamespace(utilities_db={"api_users": _Collection()})
    request = SimpleNamespace(app=SimpleNamespace(user=None))
    response = await auth.logout(request, Response(), db)
    assert response.status_code in (302, 303)


# --- a reset retires the token --------------------------------------------


async def test_a_password_reset_retires_the_token(monkeypatch):
    user = _user()
    user.reset_password_token = "rt"
    users = _Collection()

    async def find(*a, **k):
        return user

    monkeypatch.setattr(auth, "get_user_by_reset_password_token", find)
    db = SimpleNamespace(utilities_db={"api_users": users})

    request = SimpleNamespace(
        app=SimpleNamespace(tooter=SimpleNamespace(email_api=lambda **k: None)),
    )
    await auth.set_new_password_after_forgot(
        request, Response(), db, password="new-password-1", reset_password_token="rt"
    )

    assert users.writes[0][0]._doc["token"] != "old-token"


# --- what may change state ------------------------------------------------

STATE_CHANGING = [
    ("/plans/set", "POST"),
    ("/plans/reset", "POST"),
    ("/account/new-key", "POST"),
    ("/account/key/{key}", "DELETE"),
]


@pytest.mark.parametrize("path,method", STATE_CHANGING)
def test_the_route_wants_an_htmx_request(app, path, method):
    route = _route(app, path, method)
    assert route is not None
    assert security.require_htmx in [d.dependency for d in route.dependencies]


async def test_a_request_not_made_by_the_page_is_refused():
    request = SimpleNamespace(headers={})
    with pytest.raises(HTTPException) as exc:
        await security.require_htmx(request)
    assert exc.value.status_code == 403


async def test_a_request_made_by_the_page_is_let_through():
    await security.require_htmx(SimpleNamespace(headers={"hx-request": "true"}))


# --- the unused way to write an address -----------------------------------


def test_the_unverified_email_write_is_gone(app):
    gone = [
        r
        for r in app.routes
        if "save/email-address" in getattr(r, "path", "") and "site_user" in getattr(r, "path", "")
    ]
    assert gone == []


def test_the_navbar_logs_out_with_a_form():
    page = open("projects/ccdexplorer_api/templates/base/navbar.html").read()
    assert 'action="/auth/logout"' in page
    assert 'href="/auth/logout"' not in page
