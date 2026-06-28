"""Tests for the hardened ``access-token`` session cookie (finding #4).

The cookie must be HttpOnly + SameSite=Lax always, and Secure only when the site
is served over https (so local http dev still works).
"""

from types import SimpleNamespace

from starlette.responses import Response

from ccdexplorer.ccdexplorer_site.app.utils import (
    clear_access_token_cookie,
    set_access_token_cookie,
)


def _request(site_url: str):
    return SimpleNamespace(app=SimpleNamespace(env={"SITE_URL": site_url}))


def test_cookie_is_httponly_samesite_and_secure_on_https():
    response = Response()
    set_access_token_cookie(_request("https://ccdexplorer.io"), response, "tok-123")
    header = response.headers["set-cookie"].lower()

    assert "access-token=tok-123" in header
    assert "httponly" in header
    assert "samesite=lax" in header
    assert "secure" in header
    assert "path=/" in header


def test_cookie_not_secure_on_local_http():
    response = Response()
    set_access_token_cookie(_request("http://127.0.0.1:8000"), response, "tok")
    header = response.headers["set-cookie"].lower()

    assert "httponly" in header
    assert "samesite=lax" in header
    # No Secure flag on http, otherwise the browser would drop the cookie in dev.
    assert "secure" not in header


def test_clear_cookie_expires_it():
    response = Response()
    clear_access_token_cookie(_request("https://ccdexplorer.io"), response)
    header = response.headers["set-cookie"].lower()

    assert "access-token=" in header
    # delete_cookie sets an empty value with an expiry in the past / max-age 0.
    assert ("max-age=0" in header) or ("expires=" in header)
