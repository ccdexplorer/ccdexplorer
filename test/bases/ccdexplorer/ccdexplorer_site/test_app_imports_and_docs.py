"""The site app must import, and must not serve API docs.

The import test exists because it was not there: adding an auth-router import
of factory._client_ip created a cycle -- factory imports the routers at module
level, so the router's import ran against a half-built factory and raised
ImportError. Nothing in the suite imported factory, so the whole suite passed
while the site could not have started.
"""

import pytest


def test_the_factory_module_imports():
    from ccdexplorer.ccdexplorer_site.app import factory

    assert factory.create_app is not None


def test_the_auth_router_imports():
    from ccdexplorer.ccdexplorer_site.app.routers import auth

    assert auth.router is not None


def test_importing_factory_first_still_works():
    """The direction that actually broke: factory before the routers."""
    import importlib
    import sys

    for name in list(sys.modules):
        if name.startswith("ccdexplorer.ccdexplorer_site.app"):
            del sys.modules[name]

    factory = importlib.import_module("ccdexplorer.ccdexplorer_site.app.factory")
    assert factory.create_app is not None


@pytest.mark.parametrize("flag", ["docs_url=None", "redoc_url=None", "openapi_url=None"])
def test_the_docs_routes_are_disabled(flag):
    """The documented API is a separate service; these are at docs.ccdexplorer.io."""
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    assert flag in inspect.getsource(factory.create_app)


def test_client_ip_prefers_the_proxy_header():
    from types import SimpleNamespace

    from ccdexplorer.ccdexplorer_site.app.utils import client_ip

    request = SimpleNamespace(
        headers={"x-forwarded-for": "203.0.113.9, 10.0.0.1"},
        client=SimpleNamespace(host="10.0.0.1"),
    )
    assert client_ip(request) == "203.0.113.9"


def test_client_ip_falls_back_to_the_peer():
    from types import SimpleNamespace

    from ccdexplorer.ccdexplorer_site.app.utils import client_ip

    request = SimpleNamespace(headers={}, client=SimpleNamespace(host="198.51.100.4"))
    assert client_ip(request) == "198.51.100.4"
