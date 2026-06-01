from __future__ import annotations

import base64
import datetime as dt
import os
import uuid
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping

import httpx
from dotenv import find_dotenv, load_dotenv
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType, RouteMap
from pymongo import AsyncMongoClient
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
    api_key_scope: str
    mongo_uri: str | None
    api_key_cache_ttl: float
    request_timeout: float


def load_settings() -> Settings:
    load_dotenv(find_dotenv())

    api_key = os.environ.get("API_CODEX_KEY") or os.environ.get("CCDEXPLORER_API_KEY")
    if not api_key:
        raise RuntimeError("Set API_CODEX_KEY or CCDEXPLORER_API_KEY for CCDExplorer MCP.")

    mongo_uri = os.environ.get("MONGO_URI")  # optional; only required for HTTP server auth

    api_base_url = os.environ.get("CCDEXPLORER_API_BASE_URL", "https://api.ccdexplorer.io").rstrip(
        "/"
    )

    return Settings(
        api_base_url=api_base_url,
        api_key=api_key,
        api_key_scope=os.environ.get("CCDEXPLORER_API_KEY_SCOPE", api_base_url),
        mongo_uri=mongo_uri,
        api_key_cache_ttl=float(os.environ.get("CCDEXPLORER_MCP_API_KEY_CACHE_TTL", "5")),
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

    @mcp.custom_route("/mcp/.well-known/oauth-protected-resource", methods=["GET"])
    async def oauth_protected_resource(request: Request) -> JSONResponse:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host", "api.ccdexplorer.io")
        base = f"https://{host}/mcp"
        return JSONResponse({
            "resource": base,
            "authorization_servers": [base],
            "bearer_methods_supported": ["header"],
            "scopes_supported": ["mcp"],
        })

    @mcp.custom_route("/mcp/.well-known/oauth-authorization-server", methods=["GET"])
    async def oauth_authorization_server(request: Request) -> JSONResponse:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host", "api.ccdexplorer.io")
        issuer = f"https://{host}/mcp"
        return JSONResponse({
            "issuer": issuer,
            "token_endpoint": f"{issuer}/oauth/token",
            "registration_endpoint": f"{issuer}/oauth/register",
            "token_endpoint_auth_methods_supported": ["client_secret_post", "client_secret_basic"],
            "grant_types_supported": ["client_credentials"],
            "response_types_supported": [],
            "scopes_supported": ["mcp"],
        })

    @mcp.custom_route("/mcp/oauth/register", methods=["POST"])
    async def oauth_register(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:
            body = {}
        return JSONResponse(
            {
                "client_id": body.get("client_id") or str(uuid.uuid4()),
                "client_secret": body.get("client_secret", ""),
                "client_secret_expires_at": 0,
                "token_endpoint_auth_method": "client_secret_post",
                "grant_types": ["client_credentials"],
            },
            status_code=201,
        )

    @mcp.custom_route("/mcp/oauth/token", methods=["POST"])
    async def oauth_token(request: Request) -> JSONResponse:
        client_secret = ""
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("basic "):
            try:
                _, _, client_secret = base64.b64decode(auth[6:]).decode().partition(":")
            except Exception:
                pass
        if not client_secret:
            form = await request.form()
            client_secret = form.get("client_secret", "")
        return JSONResponse({
            "access_token": client_secret,
            "token_type": "Bearer",
            "expires_in": 86400,
            "scope": "mcp",
        })

    return mcp


def api_key_from_headers(headers: Mapping[bytes, bytes]) -> str | None:
    api_key = headers.get(b"x-ccdexplorer-key", b"").decode().strip()
    if api_key:
        return api_key

    authorization = headers.get(b"authorization", b"").decode()
    if not authorization:
        return None

    scheme, _, credentials = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return None

    credentials = credentials.strip()
    return credentials or None


class ApiKeyValidator:
    def __init__(self, mongo_uri: str, scope: str, cache_ttl: float):
        self.mongo_uri = mongo_uri
        self.scope = scope
        self.cache_ttl = cache_ttl
        self.client = AsyncMongoClient(mongo_uri)
        self.api_keys: dict[str, dict[str, Any]] = {}
        self.api_keys_last_requested = dt.datetime.min.replace(tzinfo=dt.UTC)

    async def get_api_keys(self) -> dict[str, dict[str, Any]]:
        now = dt.datetime.now().astimezone(dt.UTC)
        if (
            self.api_keys
            and (now - self.api_keys_last_requested).total_seconds() < self.cache_ttl
        ):
            return self.api_keys

        pipeline = [
            {"$match": {"scope": self.scope}},
            {"$match": {"api_key_end_date": {"$gte": now}}},
        ]
        coll = self.client["concordium_utilities"]["api_api_keys"]

        keys = {}
        cursor = await coll.aggregate(pipeline)
        async for document in cursor:
            keys[document["_id"]] = document

        self.api_keys = keys
        self.api_keys_last_requested = now
        return keys

    async def is_valid(self, api_key: str | None) -> bool:
        if not api_key:
            return False

        api_keys = await self.get_api_keys()
        return api_key in api_keys

    async def aclose(self) -> None:
        close = getattr(self.client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result


_UNAUTHED_PATHS = frozenset({
    "/health",
    "/mcp/.well-known/oauth-protected-resource",
    "/mcp/.well-known/oauth-authorization-server",
    "/mcp/oauth/register",
    "/mcp/oauth/token",
})


class ApiKeyAuthMiddleware:
    def __init__(self, app, validator: ApiKeyValidator):
        self.app = app
        self.validator = validator

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") in _UNAUTHED_PATHS:
            await self.app(scope, receive, send)
            return

        headers = {name.lower(): value for name, value in scope.get("headers", [])}
        api_key = api_key_from_headers(headers)
        if await self.validator.is_valid(api_key):
            await self.app(scope, receive, send)
            return

        host = headers.get(b"host", b"api.ccdexplorer.io").decode()
        www_auth = (
            f'Bearer realm="https://{host}/mcp",'
            f' resource_metadata="https://{host}/mcp/.well-known/oauth-protected-resource"'
        )
        response = JSONResponse(
            {"detail": "Unauthorized access."},
            status_code=401,
            headers={"WWW-Authenticate": www_auth},
        )
        await response(scope, receive, send)


def create_app(settings: Settings | None = None):
    settings = settings or load_settings()
    if not settings.mongo_uri:
        raise RuntimeError("Set MONGO_URI for CCDExplorer MCP HTTP server.")
    mcp = create_mcp(settings)
    return ApiKeyAuthMiddleware(
        mcp.http_app(path="/mcp", stateless_http=True),
        ApiKeyValidator(
            mongo_uri=settings.mongo_uri,
            scope=settings.api_key_scope,
            cache_ttl=settings.api_key_cache_ttl,
        ),
    )
