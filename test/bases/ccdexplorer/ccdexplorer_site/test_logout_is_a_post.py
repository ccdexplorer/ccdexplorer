"""Signing out is a POST.

It was a GET, so any page could end a visitor's session with an image tag.
"""

import inspect
from types import SimpleNamespace

import jinja2

from ccdexplorer.ccdexplorer_site.app.routers import usersv2

TEMPLATE_DIR = "projects/ccdexplorer_site/templates"


def _routes(method):
    return [
        r
        for r in usersv2.router.routes
        if getattr(r, "path", "") == "/settings/user/logout" and method in r.methods
    ]


def test_the_logout_that_acts_is_a_post():
    (route,) = _routes("POST")
    assert route.endpoint is usersv2.logout


async def test_a_get_only_redirects():
    (route,) = _routes("GET")
    assert route.endpoint is usersv2.logout_get
    assert not inspect.signature(route.endpoint).parameters  # no request, no session
    response = await route.endpoint()
    assert response.status_code == 303
    assert "set-cookie" not in response.headers


def test_the_page_offers_a_form_not_a_link():
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
    page = env.get_template("userv2/login_connections.html").render(
        {
            "user": SimpleNamespace(
                username="u",
                first_name="F",
                chat_id=1,
                password=None,
                email_address=None,
                email_verified=False,
                telegram_chat_id=1,
            )
        }
    )
    assert 'method="post" action="/settings/user/logout"' in page
    assert 'href="/settings/user/logout"' not in page
