"""Changing the address on an account is asked of the password.

The settings form sent the new address and nothing else, so anyone holding the
session could point the account's reset links at their own mailbox. An account
that has a password now sends it along; the api refuses the change without it.
"""

import jinja2
from types import SimpleNamespace

from ccdexplorer.ccdexplorer_site.app.routers import usersv2
from ccdexplorer.site_user import SiteUser

TEMPLATE_DIR = "projects/ccdexplorer_site/templates"


def _render(user):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
    return env.get_template("userv2/user_email_address.html").render({"user": user})


def test_an_account_with_a_password_is_asked_for_it():
    page = _render(SimpleNamespace(email_address="a@example.org", password="hash"))
    assert 'name="current_password"' in page
    assert 'type="password"' in page


def test_an_account_without_one_is_not():
    page = _render(SimpleNamespace(email_address="a@example.org", password=None))
    assert "current_password" not in page


class _Spy:
    def __init__(self, ok=True, value=None):
        self.calls = []
        self.result = SimpleNamespace(ok=ok, return_value=value or {"needs_verification": True})

    async def __call__(self, url, client, json_post_content=None):
        self.calls.append((url, json_post_content))
        return self.result


def _request():
    return SimpleNamespace(app=SimpleNamespace(api_url="http://api", httpx_client=None))


async def _save(monkeypatch, form, spy):
    user = SiteUser(token="tok", email_address="old@example.org")

    async def fake_user(request):
        return user

    monkeypatch.setattr(usersv2, "get_user_detailsv2", fake_user)
    monkeypatch.setattr(usersv2, "post_url_from_api", spy)
    return await usersv2.save_email_address_response(_request(), form)


async def test_the_password_is_sent_to_the_api(monkeypatch):
    spy = _Spy()
    await _save(monkeypatch, {"email_address": "new@example.org", "current_password": "pw"}, spy)
    url, body = spy.calls[0]
    assert url.endswith("/site-auth/tok/set-email")
    assert body == {"email": "new@example.org", "current_password": "pw"}


async def test_an_empty_password_is_sent_as_none_not_as_a_string(monkeypatch):
    spy = _Spy()
    await _save(monkeypatch, {"email_address": "new@example.org", "current_password": ""}, spy)
    assert spy.calls[0][1]["current_password"] is None


async def test_what_the_reader_typed_is_not_echoed_as_markup(monkeypatch):
    spy = _Spy()
    out = await _save(monkeypatch, {"email_address": "<img src=x onerror=alert(1)>"}, spy)
    assert "<img" not in out


async def test_the_apis_refusal_is_shown_and_escaped(monkeypatch):
    spy = _Spy(ok=False, value={"detail": "Current password is incorrect. <b>"})
    out = await _save(monkeypatch, {"email_address": "new@example.org"}, spy)
    assert "Current password is incorrect." in out
    assert "&lt;b&gt;" in out


# --- other text that reaches a page ---------------------------------------


def test_a_contract_name_from_the_chain_is_text_in_the_picker():
    out = usersv2.contract_search_result_html("<1,0>", "<img src=x onerror=alert(1)>")
    assert "<img" not in out
    assert "&lt;img" in out
    assert "&lt;1,0&gt;" in out  # the id's own angle brackets are text too


async def test_the_resend_error_is_text_not_markup(monkeypatch):
    from ccdexplorer.ccdexplorer_site.app.routers import auth

    async def fake_user(request):
        return SimpleNamespace(token="tok")

    async def fake_post(url, client, payload):
        return SimpleNamespace(ok=False, return_value={"detail": "<script>x</script>"})

    monkeypatch.setattr(auth, "get_user_detailsv2", fake_user)
    monkeypatch.setattr(auth, "post_url_from_api", fake_post)
    request = SimpleNamespace(app=SimpleNamespace(api_url="http://api", httpx_client=None))
    out = await auth.resend_verification(request)
    assert "<script>" not in out
