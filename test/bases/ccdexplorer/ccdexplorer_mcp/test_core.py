import pytest
from fastmcp import Client

from ccdexplorer.ccdexplorer_mcp.core import (
    BearerTokenMiddleware,
    Settings,
    _auth_headers,
    _validate_net,
    load_settings,
)


def test_auth_headers_use_api_key():
    settings = Settings(
        api_base_url="https://api.ccdexplorer.io",
        api_key="api-key",
        mcp_auth_token="mcp-token",
        request_timeout=20,
    )

    assert _auth_headers(settings) == {"x-ccdexplorer-key": "api-key"}


def test_validate_net_rejects_unknown_net():
    with pytest.raises(ValueError):
        _validate_net("devnet")


def test_load_settings_prefers_mcp_base_url(monkeypatch):
    monkeypatch.setenv("API_CODEX_KEY", "api-key")
    monkeypatch.setenv("CCDEXPLORER_API_BASE_URL", "https://api.example.com/")
    monkeypatch.setenv("CCDEXPLORER_MCP_REQUEST_TIMEOUT", "5")

    settings = load_settings()

    assert settings.api_base_url == "https://api.example.com"
    assert settings.api_key == "api-key"
    assert settings.mcp_auth_token == "api-key"
    assert settings.request_timeout == 5


@pytest.mark.asyncio
async def test_bearer_token_middleware_allows_health_without_auth():
    calls = []

    async def inner(scope, receive, send):
        calls.append(scope["path"])

    middleware = BearerTokenMiddleware(inner, token="secret")

    await middleware({"type": "http", "path": "/health", "headers": []}, None, None)

    assert calls == ["/health"]


@pytest.mark.asyncio
async def test_bearer_token_middleware_allows_bearer_token():
    calls = []

    async def inner(scope, receive, send):
        calls.append(scope["path"])

    middleware = BearerTokenMiddleware(inner, token="secret")

    await middleware(
        {
            "type": "http",
            "path": "/mcp",
            "headers": [(b"authorization", b"Bearer secret")],
        },
        None,
        None,
    )

    assert calls == ["/mcp"]


@pytest.mark.asyncio
async def test_mcp_exposes_accounts_count_tool(monkeypatch):
    from ccdexplorer.ccdexplorer_mcp import core

    async def fake_get_json(settings, path):
        assert settings.api_base_url == "https://api.example.com"
        assert path == "/v2/mainnet/accounts/info/count"
        return 104415

    monkeypatch.setattr(core, "_get_json", fake_get_json)
    mcp = core.create_mcp(
        Settings(
            api_base_url="https://api.example.com",
            api_key="api-key",
            mcp_auth_token="mcp-token",
            request_timeout=20,
        )
    )

    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = {tool.name for tool in tools}
        assert "get_mainnet_addresses_count" in tool_names

        result = await client.call_tool("get_mainnet_addresses_count", {})

    assert result.data == 104415
