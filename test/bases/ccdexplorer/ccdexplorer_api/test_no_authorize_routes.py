"""The api serves no authorization flow.

Four routes were registered for an MCP mount that is itself commented out.
With nothing mounted they did nothing for anyone who was meant to use them,
and they were still served to everyone else. They come back with the mount,
written for it.
"""

import pytest

from conftest import build_test_app

GONE = [
    "/authorize",
    "/oauth/token",
    "/oauth/register",
    "/.well-known/oauth-protected-resource",
]


@pytest.fixture(scope="module")
def paths():
    app = build_test_app()
    return {getattr(route, "path", None) for route in app.routes}


@pytest.mark.parametrize("path", GONE)
def test_the_route_is_not_registered(paths, path):
    assert path not in paths


def test_the_routes_that_share_the_factory_are_still_there(paths):
    """The removal sits between these in the factory; taking too much would
    take the health check a deploy waits on."""
    assert {"/health", "/version", "/metrics"} <= paths


def test_nothing_is_left_that_puts_a_key_in_a_code():
    from ccdexplorer.ccdexplorer_api.app import factory

    for name in ("_make_auth_code", "_decode_auth_code", "_verify_pkce_s256"):
        assert not hasattr(factory, name), name
