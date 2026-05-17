from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

import httpx
from dotenv import load_dotenv
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse


Network = Literal["mainnet", "testnet"]


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str
    mcp_auth_token: str | None
    request_timeout: float


def load_settings() -> Settings:
    load_dotenv()

    api_key = os.environ.get("API_CODEX_KEY") or os.environ.get("CCDEXPLORER_API_KEY")
    if not api_key:
        raise RuntimeError("Set API_CODEX_KEY or CCDEXPLORER_API_KEY for CCDExplorer MCP.")

    api_base_url = os.environ.get(
        "CCDEXPLORER_API_BASE_URL", "https://api.ccdexplorer.io"
    ).rstrip("/")

    return Settings(
        api_base_url=api_base_url,
        api_key=api_key,
        mcp_auth_token=os.environ.get("CCDEXPLORER_MCP_AUTH_TOKEN", api_key),
        request_timeout=float(os.environ.get("CCDEXPLORER_MCP_REQUEST_TIMEOUT", "20")),
    )


def _auth_headers(settings: Settings) -> dict[str, str]:
    return {"x-ccdexplorer-key": settings.api_key}


async def _get_json(settings: Settings, path: str):
    async with httpx.AsyncClient(
        base_url=settings.api_base_url,
        headers=_auth_headers(settings),
        timeout=settings.request_timeout,
    ) as client:
        response = await client.get(path)
        response.raise_for_status()
        return response.json()


def _validate_net(net: str) -> Network:
    if net not in {"mainnet", "testnet"}:
        raise ValueError("net must be 'mainnet' or 'testnet'")
    return net  # type: ignore[return-value]


def create_mcp(settings: Settings | None = None) -> FastMCP:
    settings = settings or load_settings()
    mcp = FastMCP("CCDExplorer MCP")

    @mcp.custom_route("/health", methods=["GET"])
    async def health_check(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @mcp.tool
    async def get_accounts_count(net: Network = "mainnet") -> dict[str, int | str]:
        """Return the current account/address count for mainnet or testnet."""
        selected_net = _validate_net(net)
        count = await _get_json(settings, f"/v2/{selected_net}/accounts/info/count")
        return {"net": selected_net, "accounts_count": int(count)}

    @mcp.tool
    async def get_mainnet_addresses_count() -> int:
        """Return the current mainnet account/address count."""
        count = await _get_json(settings, "/v2/mainnet/accounts/info/count")
        return int(count)

    return mcp


class BearerTokenMiddleware:
    def __init__(self, app, token: str | None):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or self.token is None or scope.get("path") == "/health":
            await self.app(scope, receive, send)
            return

        headers = {name.lower(): value for name, value in scope.get("headers", [])}
        authorization = headers.get(b"authorization", b"").decode()
        api_key = headers.get(b"x-ccdexplorer-key", b"").decode()
        expected_bearer = f"Bearer {self.token}"

        if authorization == expected_bearer or api_key == self.token:
            await self.app(scope, receive, send)
            return

        response = JSONResponse({"detail": "Unauthorized access."}, status_code=401)
        await response(scope, receive, send)


def create_app(settings: Settings | None = None):
    settings = settings or load_settings()
    mcp = create_mcp(settings)
    return BearerTokenMiddleware(
        mcp.http_app(path="/mcp", stateless_http=True),
        settings.mcp_auth_token,
    )
