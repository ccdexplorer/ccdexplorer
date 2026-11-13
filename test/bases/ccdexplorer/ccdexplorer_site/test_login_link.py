"""The site's side of a login link, and of connecting Telegram.

/token/<x> used to look <x> up as an account token and log the browser in.
The token never changed, so the url was a password that sat in chat history,
browser history and proxy logs. It now holds a code the api redeems once.

The settings page printed the same token in its "Connect Telegram" link.
That link now goes through the site, which asks the api for a code.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

TEMPLATES = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site" / "templates"
ACCOUNT_TOKEN = "3f0c1d6e-0000-4000-8000-000000000001"
CODE = "kq3Zc0d3-one_time-kq3Zc0d3-one_time-kq3Zc0d3"
API = "https://api.example"


def _usersv2():
    """Resolved per call: a full-suite run re-imports the site app package,
    and a module captured at collection time is one no monkeypatch reaches."""
    from ccdexplorer.ccdexplorer_site.app.routers import usersv2

    return usersv2


class Api:
    """Stands in for post_url_from_api, answering by path."""

    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    async def __call__(self, url, httpx_client, json_post_content=None, headers=None):
        self.calls.append((url, json_post_content))
        for suffix, (ok, value) in self.answers.items():
            if url.endswith(suffix):
                return SimpleNamespace(ok=ok, return_value=value)
        return SimpleNamespace(ok=False, return_value=None)


class Templates:
    def __init__(self):
        self.rendered = []

    def TemplateResponse(self, request, name, context, **kwargs):
        self.rendered.append((name, context, kwargs))
        return SimpleNamespace(template=name, context=context, **kwargs)


def _request(user=None, site_url="https://ccdexplorer.io"):
    env = {"SITE_URL": site_url}
    return SimpleNamespace(
        app=SimpleNamespace(api_url=API, httpx_client=None, env=env, templates=Templates()),
        headers={"user-agent": "pytest"},
        state=SimpleNamespace(user=user, session_id="s1" if user else None),
        cookies={},
    )


@pytest.fixture
def api(monkeypatch):
    def install(answers):
        fake = Api(answers)
        monkeypatch.setattr(_usersv2(), "post_url_from_api", fake)
        return fake

    return install


GOOD = {
    "/site-auth/login-codes/redeem": (True, {"token": ACCOUNT_TOKEN}),
    f"/site-auth/{ACCOUNT_TOKEN}/sessions": (
        True,
        {"session_id": "s1", "session_token": "session-token"},
    ),
}


# --- /token/<code> ---------------------------------------------------------


async def test_a_good_code_logs_the_browser_in(api):
    api(GOOD)

    response = await _usersv2().slash_token(_request(), CODE)

    assert response.status_code == 303
    assert response.headers["location"] == "/settings/user/overview"
    assert "access-token=session-token" in response.headers["set-cookie"]


async def test_the_code_goes_to_the_api_in_the_body_not_the_url(api):
    """A url ends up in access logs and in traces; a body does not."""
    fake = api(GOOD)

    await _usersv2().slash_token(_request(), CODE)

    url, body = fake.calls[0]
    assert url == f"{API}/site-auth/login-codes/redeem"
    assert body == {"code": CODE}
    assert CODE not in url


async def test_a_refused_code_logs_nobody_in(api):
    fake = api({"/site-auth/login-codes/redeem": (False, {"detail": "nope"})})
    request = _request()

    response = await _usersv2().slash_token(request, CODE)

    assert len(fake.calls) == 1  # no session was asked for
    assert not hasattr(response, "headers") or "set-cookie" not in response.headers


async def test_a_refused_code_says_how_to_get_another(api):
    api({"/site-auth/login-codes/redeem": (False, None)})
    request = _request()

    await _usersv2().slash_token(request, CODE)

    name, context, _ = request.app.templates.rendered[0]
    assert name == "auth/login.html"
    assert "/login" in context["error"]


async def test_the_account_token_is_only_ever_offered_as_a_code(api):
    """An old link holds the account token. It is handed to the api as a
    code like anything else, and is not looked up as an account."""
    fake = api({"/site-auth/login-codes/redeem": (False, None)})

    await _usersv2().slash_token(_request(), ACCOUNT_TOKEN)

    assert [url for url, _ in fake.calls] == [f"{API}/site-auth/login-codes/redeem"]


# --- connecting Telegram ------------------------------------------------------


LINK = {f"/site-auth/{ACCOUNT_TOKEN}/telegram-link-code": (True, {"code": CODE})}


async def test_connecting_telegram_sends_the_browser_to_the_bot_with_a_code(api):
    api(LINK)
    user = SimpleNamespace(token=ACCOUNT_TOKEN)

    response = await _usersv2().connect_telegram(_request(user))

    assert response.status_code == 303
    assert response.headers["location"] == (
        f"https://telegram.me/concordium_explorer_bot?start={CODE}"
    )


async def test_the_redirect_does_not_hold_the_account_token(api):
    api(LINK)

    response = await _usersv2().connect_telegram(_request(SimpleNamespace(token=ACCOUNT_TOKEN)))

    assert ACCOUNT_TOKEN not in response.headers["location"]


async def test_anywhere_but_production_it_is_the_dev_bot(api):
    api(LINK)
    request = _request(SimpleNamespace(token=ACCOUNT_TOKEN), site_url="http://127.0.0.1:8000")

    response = await _usersv2().connect_telegram(request)

    assert response.headers["location"].startswith("https://telegram.me/ce_dev_bot?start=")


async def test_connecting_telegram_needs_a_login(api):
    fake = api(LINK)

    response = await _usersv2().connect_telegram(_request(user=None))

    assert response.headers["location"] == "/auth/login"
    assert fake.calls == []


async def test_no_code_means_back_to_settings_not_to_telegram(api):
    api({f"/site-auth/{ACCOUNT_TOKEN}/telegram-link-code": (False, None)})

    response = await _usersv2().connect_telegram(_request(SimpleNamespace(token=ACCOUNT_TOKEN)))

    assert response.headers["location"] == "/settings/user/overview"


# --- the page ---------------------------------------------------------------


def test_no_template_prints_the_account_token():
    offenders = [
        str(path.relative_to(TEMPLATES))
        for path in TEMPLATES.rglob("*.html")
        if "user.token" in path.read_text()
    ]
    assert offenders == []


def test_the_settings_page_links_to_the_site_not_straight_to_telegram():
    page = (TEMPLATES / "userv2" / "login_connections.html").read_text()
    assert 'href="/settings/user/connect-telegram"' in page
    assert "?start=" not in page
