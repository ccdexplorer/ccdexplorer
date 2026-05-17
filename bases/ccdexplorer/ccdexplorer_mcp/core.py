from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import httpx
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType, RouteMap
from starlette.requests import Request
from starlette.responses import JSONResponse


OPENAPI_HTTP_METHODS = {"get"}
DEFAULT_DENIED_PATH_PREFIXES = (
    "/account",
    "/auth",
    "/live_port",
    "/metrics",
    "/node",
    "/plans",
    "/static",
)


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

    api_base_url = os.environ.get("CCDEXPLORER_API_BASE_URL", "https://api.ccdexplorer.io").rstrip(
        "/"
    )

    return Settings(
        api_base_url=api_base_url,
        api_key=api_key,
        mcp_auth_token=os.environ.get("CCDEXPLORER_MCP_AUTH_TOKEN", api_key),
        request_timeout=float(os.environ.get("CCDEXPLORER_MCP_REQUEST_TIMEOUT", "20")),
    )


def _auth_headers(settings: Settings) -> dict[str, str]:
    return {"x-ccdexplorer-key": settings.api_key}


def _split_env_list(name: str) -> set[str]:
    return {value.strip() for value in os.environ.get(name, "").split(",") if value.strip()}


def _denied_method_paths() -> set[tuple[str, str]]:
    denied = set()
    for value in _split_env_list("CCDEXPLORER_MCP_DENY_METHOD_PATHS"):
        method, _, path = value.partition(" ")
        if method and path:
            denied.add((method.lower(), path.strip()))
    return denied


def _operation_is_denied(path: str, method: str, operation: dict[str, Any]) -> bool:
    operation_id = operation.get("operationId")
    tags = set(operation.get("tags") or [])

    denied_operation_ids = _split_env_list("CCDEXPLORER_MCP_DENY_OPERATION_IDS")
    denied_paths = _split_env_list("CCDEXPLORER_MCP_DENY_PATHS")
    denied_path_prefixes = set(DEFAULT_DENIED_PATH_PREFIXES) | _split_env_list(
        "CCDEXPLORER_MCP_DENY_PATH_PREFIXES"
    )
    denied_tags = _split_env_list("CCDEXPLORER_MCP_DENY_TAGS")

    return (
        operation_id in denied_operation_ids
        or path in denied_paths
        or any(path.startswith(prefix) for prefix in denied_path_prefixes)
        or bool(tags & denied_tags)
        or (method, path) in _denied_method_paths()
    )


def _filter_openapi_spec(openapi_spec: dict[str, Any], settings: Settings) -> dict[str, Any]:
    filtered_spec = deepcopy(openapi_spec)
    filtered_paths = {}

    for path, path_item in filtered_spec.get("paths", {}).items():
        filtered_path_item = {}
        for method, operation in path_item.items():
            if method not in OPENAPI_HTTP_METHODS:
                filtered_path_item[method] = operation
                continue

            if not _operation_is_denied(path, method, operation):
                filtered_path_item[method] = operation

        if any(method in OPENAPI_HTTP_METHODS for method in filtered_path_item):
            filtered_paths[path] = filtered_path_item

    filtered_spec["paths"] = filtered_paths
    filtered_spec["servers"] = [{"url": settings.api_base_url}]
    return filtered_spec


def _load_openapi_spec(settings: Settings) -> dict[str, Any]:
    with httpx.Client(
        base_url=settings.api_base_url,
        headers=_auth_headers(settings),
        timeout=settings.request_timeout,
    ) as client:
        response = client.get("/openapi.json")
        response.raise_for_status()
        return response.json()


def create_mcp(settings: Settings | None = None) -> FastMCP:
    settings = settings or load_settings()
    openapi_spec = _filter_openapi_spec(_load_openapi_spec(settings), settings)
    client = httpx.AsyncClient(
        base_url=settings.api_base_url,
        headers=_auth_headers(settings),
        timeout=settings.request_timeout,
    )
    mcp = FastMCP.from_openapi(
        openapi_spec,
        client=client,
        name="CCDExplorer MCP",
        route_maps=[RouteMap(methods="*", pattern=r".*", mcp_type=MCPType.TOOL)],
        validate_output=False,
    )

    @mcp.custom_route("/health", methods=["GET"])
    async def health_check(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

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
