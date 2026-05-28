import datetime as dt

import pytest
from fastmcp import Client

from ccdexplorer.ccdexplorer_mcp import core
from ccdexplorer.ccdexplorer_mcp.core import (
    ApiKeyAuthMiddleware,
    ApiKeyValidator,
    Settings,
    _auth_headers,
    _filter_openapi_spec,
    api_key_from_headers,
    load_settings,
)


def test_auth_headers_use_api_key():
    settings = Settings(
        api_base_url="https://api.ccdexplorer.io",
        api_key="api-key",
        api_key_scope="https://api.ccdexplorer.io",
        mongo_uri="mongodb://example",
        api_key_cache_ttl=5,
        request_timeout=20,
    )

    assert _auth_headers(settings) == {"x-ccdexplorer-key": "api-key"}


def test_api_key_from_headers_prefers_ccdexplorer_key():
    api_key = api_key_from_headers(
        {
            b"x-ccdexplorer-key": b"api-key",
            b"authorization": b"Bearer bearer-key",
        }
    )

    assert api_key == "api-key"


def test_api_key_from_headers_accepts_bearer_token():
    assert api_key_from_headers({b"authorization": b"Bearer api-key"}) == "api-key"


def test_api_key_from_headers_rejects_non_bearer_authorization():
    assert api_key_from_headers({b"authorization": b"Basic api-key"}) is None


def test_load_settings_uses_api_key_scope(monkeypatch):
    monkeypatch.setenv("API_CODEX_KEY", "api-key")
    monkeypatch.setenv("MONGO_URI", "mongodb://example")
    monkeypatch.setenv("CCDEXPLORER_API_BASE_URL", "https://api.example.com/")
    monkeypatch.setenv("CCDEXPLORER_API_KEY_SCOPE", "https://api.example.com")
    monkeypatch.setenv("CCDEXPLORER_MCP_API_KEY_CACHE_TTL", "3")
    monkeypatch.setenv("CCDEXPLORER_MCP_REQUEST_TIMEOUT", "5")

    settings = load_settings()

    assert settings.api_base_url == "https://api.example.com"
    assert settings.api_key == "api-key"
    assert settings.api_key_scope == "https://api.example.com"
    assert settings.mongo_uri == "mongodb://example"
    assert settings.api_key_cache_ttl == 3
    assert settings.request_timeout == 5


def test_filter_openapi_spec_keeps_allowed_gets_only():
    settings = Settings(
        api_base_url="https://api.example.com",
        api_key="api-key",
        api_key_scope="https://api.example.com",
        mongo_uri="mongodb://example",
        api_key_cache_ttl=5,
        request_timeout=20,
    )
    openapi_spec = {
        "openapi": "3.1.0",
        "paths": {
            "/v2/mainnet/accounts/info/count": {
                "get": {"operationId": "get_accounts_count"},
            },
            "/account/keys": {
                "get": {"operationId": "get_account_keys"},
            },
            "/v2/mainnet/token": {
                "post": {"operationId": "create_token"},
            },
        },
    }

    filtered_spec = _filter_openapi_spec(openapi_spec, settings)

    assert filtered_spec["servers"] == [{"url": "https://api.example.com"}]
    assert "/v2/mainnet/accounts/info/count" in filtered_spec["paths"]
    assert "/account/keys" not in filtered_spec["paths"]
    assert "/v2/mainnet/token" not in filtered_spec["paths"]


@pytest.mark.asyncio
async def test_api_key_validator_accepts_cached_key():
    validator = ApiKeyValidator(
        mongo_uri="mongodb://example",
        scope="https://api.example.com",
        cache_ttl=5,
    )
    validator.api_keys = {
        "api-key": {
            "api_account_id": "account",
            "api_group": "group",
        }
    }
    validator.api_keys_last_requested = dt.datetime.now().astimezone(dt.UTC)

    try:
        assert await validator.is_valid("api-key") is True
        assert await validator.is_valid("unknown-key") is False
    finally:
        await validator.aclose()


@pytest.mark.asyncio
async def test_api_key_auth_middleware_allows_health_without_auth():
    calls = []

    async def inner(scope, receive, send):
        calls.append(scope["path"])

    middleware = ApiKeyAuthMiddleware(inner, validator=None)

    await middleware({"type": "http", "path": "/health", "headers": []}, None, None)

    assert calls == ["/health"]


@pytest.mark.asyncio
async def test_api_key_auth_middleware_allows_valid_key():
    calls = []

    class Validator:
        async def is_valid(self, api_key):
            return api_key == "api-key"

    async def inner(scope, receive, send):
        calls.append(scope["path"])

    middleware = ApiKeyAuthMiddleware(inner, validator=Validator())

    await middleware(
        {
            "type": "http",
            "path": "/mcp",
            "headers": [(b"x-ccdexplorer-key", b"api-key")],
        },
        None,
        None,
    )

    assert calls == ["/mcp"]


@pytest.mark.asyncio
async def test_api_key_auth_middleware_rejects_unknown_key():
    messages = []

    class Validator:
        async def is_valid(self, api_key):
            return False

    async def inner(scope, receive, send):
        raise AssertionError("inner app should not be called")

    async def send(message):
        messages.append(message)

    middleware = ApiKeyAuthMiddleware(inner, validator=Validator())

    await middleware(
        {
            "type": "http",
            "path": "/mcp",
            "headers": [(b"x-ccdexplorer-key", b"unknown-key")],
        },
        None,
        send,
    )

    assert messages[0]["status"] == 401


@pytest.mark.asyncio
async def test_mcp_exposes_openapi_tools(monkeypatch):
    openapi_spec = {
        "openapi": "3.1.0",
        "info": {"title": "CCDExplorer", "version": "1.0.0"},
        "paths": {
            "/v2/mainnet/accounts/info/count": {
                "get": {
                    "operationId": "get_accounts_count",
                    "description": "Return the current account/address count.",
                    "responses": {"200": {"description": "OK"}},
                },
            },
        },
    }

    monkeypatch.setattr(core, "_load_openapi_spec", lambda settings: openapi_spec)
    mcp = core.create_mcp(
        Settings(
            api_base_url="https://api.example.com",
            api_key="api-key",
            api_key_scope="https://api.example.com",
            mongo_uri="mongodb://example",
            api_key_cache_ttl=5,
            request_timeout=20,
        )
    )

    async with Client(mcp) as client:
        tools = await client.list_tools()

    assert {tool.name for tool in tools} == {"get_accounts_count"}
