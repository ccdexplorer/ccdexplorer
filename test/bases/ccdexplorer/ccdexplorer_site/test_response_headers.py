"""Every response carries the headers that cost nothing to send.

The site sent none: no content-type sniffing guard, no referrer policy and
nothing to stop another origin framing it. These three are the ones no page
on the site can be broken by.

What is deliberately not here is a script-src policy. The templates hold over
a hundred inline scripts and a couple of dozen inline handlers, so a policy
strict enough to mean anything would break the site and one loose enough not
to would be decoration. That needs nonces on every one of them first.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"


@pytest.fixture(scope="module")
def response():
    app = create_app(
        AppSettings(
            static_dir=PROJECT / "static",
            templates_dir=PROJECT / "templates",
            node_modules_dir=PROJECT / "node_modules",
            addresses_dir=PROJECT / "addresses",
        )
    )
    return TestClient(app, follow_redirects=False).get("/mainnet/no-such-page-at-all")


def test_the_browser_is_told_not_to_guess_content_types(response):
    assert response.headers["x-content-type-options"] == "nosniff"


def test_the_referrer_stops_at_the_origin_across_sites(response):
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"


@pytest.mark.parametrize(
    "directive", ["frame-ancestors 'self'", "object-src 'none'", "base-uri 'self'"]
)
def test_the_policy_carries_the_directives_nothing_depends_on(response, directive):
    policy = [d.strip() for d in response.headers["content-security-policy"].split(";")]

    assert directive in policy


def test_the_policy_does_not_pretend_to_restrict_scripts(response):
    """A script-src that has to allow inline script protects nothing, and a
    default-src would restrict scripts by fallback and break every page."""
    policy = response.headers["content-security-policy"]

    assert "script-src" not in policy
    assert "default-src" not in policy


def test_static_files_carry_them_too():
    app = create_app(
        AppSettings(
            static_dir=PROJECT / "static",
            templates_dir=PROJECT / "templates",
            node_modules_dir=PROJECT / "node_modules",
            addresses_dir=PROJECT / "addresses",
        )
    )
    static = next(p for p in (PROJECT / "static").rglob("*.css"))
    served = TestClient(app).get(f"/static/{static.relative_to(PROJECT / 'static')}")

    assert served.status_code == 200
    assert served.headers["x-content-type-options"] == "nosniff"
