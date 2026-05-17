from types import SimpleNamespace

import pytest
from ratelimit.auths import EmptyInformation

from ccdexplorer.ccdexplorer_api.app.factory import BearerTokenApiKeyMiddleware
from ccdexplorer.ccdexplorer_api.app.ratelimiting import (
    AUTH_FUNCTION,
    api_key_from_headers,
)


def test_api_key_from_headers_prefers_existing_ccdexplorer_key():
    api_key = api_key_from_headers(
        {
            "x-ccdexplorer-key": "rest-key",
            "authorization": "Bearer bearer-key",
        }
    )

    assert api_key == "rest-key"


def test_api_key_from_headers_accepts_bearer_token():
    assert api_key_from_headers({"authorization": "Bearer test-key"}) == "test-key"


def test_api_key_from_headers_rejects_non_bearer_authorization():
    assert api_key_from_headers({"authorization": "Basic test-key"}) is None


@pytest.mark.asyncio
async def test_auth_function_accepts_bearer_token():
    async def fake_get_api_keys(*args, **kwargs):
        return {
            "test-key": {
                "api_account_id": "test-account",
                "api_group": "test-group",
            }
        }

    app = SimpleNamespace(
        state=SimpleNamespace(get_api_keys_fn=fake_get_api_keys),
        motormongo=None,
    )
    scope = {
        "app": app,
        "headers": [(b"authorization", b"Bearer test-key")],
    }

    assert await AUTH_FUNCTION(scope) == ("test-account", "test-group")
    assert scope["api_auth"] == ("test-account", "test-group")


@pytest.mark.asyncio
async def test_auth_function_rejects_unknown_bearer_token():
    async def fake_get_api_keys(*args, **kwargs):
        return {}

    app = SimpleNamespace(
        state=SimpleNamespace(get_api_keys_fn=fake_get_api_keys),
        motormongo=None,
    )
    scope = {
        "app": app,
        "headers": [(b"authorization", b"Bearer test-key")],
    }

    with pytest.raises(EmptyInformation):
        await AUTH_FUNCTION(scope)


@pytest.mark.asyncio
async def test_bearer_token_api_key_middleware_adds_existing_header():
    captured_scope = {}

    async def inner_app(scope, receive, send):
        captured_scope.update(scope)

    middleware = BearerTokenApiKeyMiddleware(inner_app)
    scope = {
        "type": "http",
        "headers": [(b"authorization", b"Bearer test-key")],
    }

    await middleware(scope, None, None)

    assert (b"x-ccdexplorer-key", b"test-key") in captured_scope["headers"]


@pytest.mark.asyncio
async def test_bearer_token_api_key_middleware_keeps_existing_header():
    captured_scope = {}

    async def inner_app(scope, receive, send):
        captured_scope.update(scope)

    middleware = BearerTokenApiKeyMiddleware(inner_app)
    scope = {
        "type": "http",
        "headers": [
            (b"authorization", b"Bearer bearer-key"),
            (b"x-ccdexplorer-key", b"rest-key"),
        ],
    }

    await middleware(scope, None, None)

    headers = captured_scope["headers"]
    assert headers.count((b"x-ccdexplorer-key", b"rest-key")) == 1
    assert (b"x-ccdexplorer-key", b"bearer-key") not in headers
