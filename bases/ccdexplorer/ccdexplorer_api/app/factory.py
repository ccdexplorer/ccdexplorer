# ruff: noqa: F403, F405, E402, E501, E722, F401
# pyright: reportAttributeAccessIssue=false
import base64
import datetime as dt
import hashlib
import hmac as _hmac
import html as _html
import importlib
import json
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Callable, Optional

import httpx
import humanize
import urllib3
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.mongodb.core import CollectionsUtilities
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi_mcp import AuthConfig, FastApiMCP, OAuthMetadata
from fastapi.routing import APIRoute
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from httpx import ASGITransport

_prometheus_client = importlib.import_module("prometheus_client")
_prometheus_multiprocess = importlib.import_module("prometheus_client.multiprocess")
CONTENT_TYPE_LATEST = _prometheus_client.CONTENT_TYPE_LATEST
CollectorRegistry = _prometheus_client.CollectorRegistry
generate_latest = _prometheus_client.generate_latest
multiprocess = _prometheus_multiprocess

from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware

from .state_getters import get_api_keys as _get_api_keys

urllib3.disable_warnings()

from ccdexplorer.ccdexplorer_api.app.routers.account import account
from ccdexplorer.ccdexplorer_api.app.routers.auth import auth
from ccdexplorer.ccdexplorer_api.app.routers.home import home
from ccdexplorer.ccdexplorer_api.app.routers.plans import plans

# # V2
from ccdexplorer.ccdexplorer_api.app.routers.v2 import (
    account_v2,
    accounts_v2,
    block_v2,
    blocks_v2,
    contract_v2,
    contracts_v2,
    markets_v2,
    misc_v2,
    module_v2,
    modules_v2,
    plt_v2,
    plts_v2,
    site_user_v2,
    smart_wallet_v2,
    smart_wallets_v2,
    token_v2,
    tokens_v2,
    transaction_v2,
    transactions_v2,
)
from ccdexplorer.env import REDIS_URL, environment
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.tooter import Tooter

from .models import rate_limit_rules

r = Redis.from_url(REDIS_URL, decode_responses=False)  # type: ignore
# ratelimit


import sentry_sdk
from ratelimit import RateLimitMiddleware
from ratelimit.backends.redis import RedisBackend
from redis.asyncio import StrictRedis

from .ratelimiting import (
    AUTH_FUNCTION,
    handle_429,
    handle_auth_error,
)
from ccdexplorer.env import API_KEY_HEADER

if environment["SITE_URL"] != "http://127.0.0.1:8000":
    sentry_sdk.init(
        dsn=environment["SENTRY_DSN"],
        traces_sample_rate=1.0,
        _experiments={"continuous_profiling_auto_start": True},
    )


def _make_auth_code(api_key: str, code_challenge: str, signing_key: str) -> str:
    payload = json.dumps({"k": api_key, "cc": code_challenge, "exp": int(time.time()) + 300}, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    sig = _hmac.new(signing_key.encode(), b64.encode(), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def _decode_auth_code(code: str, signing_key: str) -> dict | None:
    try:
        b64, sig = code.rsplit(".", 1)
    except ValueError:
        return None
    expected = _hmac.new(signing_key.encode(), b64.encode(), hashlib.sha256).hexdigest()
    if not _hmac.compare_digest(sig, expected):
        return None
    try:
        data = json.loads(base64.urlsafe_b64decode(b64 + "=="))
    except Exception:
        return None
    if data.get("exp", 0) < time.time():
        return None
    return data


def _verify_pkce_s256(code_verifier: str, code_challenge: str) -> bool:
    computed = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode("ascii")).digest()
    ).rstrip(b"=").decode("ascii")
    return _hmac.compare_digest(computed, code_challenge)


def classify_endpoint(request: Request) -> tuple[str, str] | tuple[None, None]:
    """
    Returns (net, resource) based on the URL structure.
    Example:
        /v2/mainnet/account/...  → ("mainnet", "account")
        /v2/testnet/module/...   → ("testnet", "module")
    """
    parts = request.url.path.strip("/").split("/")

    # parts[0] = "v2"
    # parts[1] = net
    # parts[2] = resource
    if len(parts) < 3:
        return None, None

    net = parts[1]
    resource = parts[2]

    return net, resource


class UsageMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, mongomotor: MongoMotor):
        super().__init__(app)
        self.mongomotor = mongomotor

    async def dispatch(self, request: Request, call_next):
        # user = request.state.user  # however you attach your user
        # if not user:
        #     return await call_next(request)
        net, resource = classify_endpoint(request)
        today = dt.datetime.now().astimezone(dt.UTC).strftime("%Y-%m-%d")
        # Extract values placed by AUTH_FUNCTION
        api_account_id, group_name = None, None
        authenticated = request.scope.get("api_auth")
        if authenticated:
            api_account_id, group_name = authenticated
        host = request.url.hostname
        # Call the API endpoint first
        response: Response = await call_next(request)

        if request.method not in ["GET", "POST"]:
            return response
        # Increment counters only if successful or depending on your choice
        if (
            (response.status_code < 500)
            and (response.status_code != 429)
            and resource is not None
            and api_account_id is not None
        ):
            doc_id = f"{host}:{api_account_id}:{net}:{today}"
            await self.mongomotor.utilities[CollectionsUtilities.api_usage_daily].update_one(
                {"_id": doc_id},
                {
                    "$setOnInsert": {
                        "host": host,
                        "api_account_id": api_account_id,
                        "net": net,
                        "date": today,
                    },
                    "$inc": {"total_calls": 1, f"endpoints.{resource}": 1},
                },
                upsert=True,
            )

        return response


class BearerTokenApiKeyMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = list(scope.get("headers") or [])
        header_names = {name.lower() for name, _ in headers}
        api_key_header = API_KEY_HEADER.encode()

        if api_key_header not in header_names:
            authorization = None
            for name, value in headers:
                if name.lower() == b"authorization":
                    authorization = value.decode()
                    break

            if authorization:
                scheme, _, credentials = authorization.partition(" ")
                credentials = credentials.strip()
                if scheme.lower() == "bearer" and credentials:
                    headers.append((api_key_header, credentials.encode()))
                    scope = {**scope, "headers": headers}

        await self.app(scope, receive, send)


async def _aclose(resource) -> None:
    if resource is None:
        return
    close = getattr(resource, "aclose", None)
    if callable(close):
        await close()  # pyright: ignore[reportGeneralTypeIssues]
        return
    close = getattr(resource, "close", None)
    if callable(close):
        res = close()
        if hasattr(res, "__await__"):  # some close() are async
            await res  # pyright: ignore[reportGeneralTypeIssues]
        return


def datetime_to_date(value: dt.datetime):
    return f"{value:%Y-%m-%d}"


def datetime_to_date_and_time_no_sec(value: dt.datetime):
    return f"{value:%Y-%m-%d %H:%M} UTC"


def seperator_no_decimals(value: int):
    return f"{int(value):,.0f}"


def humanize_timedelta(value: dt.timedelta):
    return humanize.precisedelta(value, suppress=["days"], format="%0.0f")


tags_metadata = [
    {
        "name": "Transaction",
        "description": "Routes to retrieve information from a transaction.",
    },
    {
        "name": "Account",
        "description": "Routes to retrieve information from an account.",
        "externalDocs": {
            "description": "docs.ccdexlorer.io",
            "url": "https://docs.ccdexlorer.io",
        },
    },
]


class AppSettings(BaseModel):
    static_dir: Path
    templates_dir: Path
    node_modules_dir: Path
    get_api_keys_fn: Callable = _get_api_keys
    mongo_factory: Optional[Callable[[], MongoDB]] = None
    motor_factory: Optional[Callable[[], MongoMotor]] = None
    grpc_factory: Optional[Callable[[], GRPCClient]] = None
    tooter_factory: Optional[Callable[[], Tooter]] = None
    ccdexplorer_api_key: str | None = None
    api_url: str | None = None


def use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            if not route.operation_id:
                route.operation_id = route.name  # in this case, 'read_items'


def create_app(app_settings: AppSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        created = {"mongo": False, "motor": False, "grpc": False, "tooter": False}

        # Mongo
        if app_settings.mongo_factory is not None:
            app.mongodb = app_settings.mongo_factory()
            created["mongo"] = True
        else:
            app.mongodb = None  # or create a default if you want

        if app_settings.motor_factory is not None:
            app.motormongo = app_settings.motor_factory()
            created["motor"] = True
        else:
            app.motormongo = None  # or create a default if you want

        # gRPC
        if app_settings.grpc_factory is not None:
            app.grpcclient = app_settings.grpc_factory()
            created["grpc"] = True
        else:
            app.grpcclient = None

        # Tooter (whatever notifier you use)
        if app_settings.tooter_factory is not None:
            app.tooter = app_settings.tooter_factory()
            created["tooter"] = True
        else:
            app.tooter = None

        app.redis = StrictRedis.from_url(REDIS_URL)  # type: ignore
        app.api_url = app_settings.api_url or environment["API_URL"]
        app.httpx_client = httpx.AsyncClient(
            transport=ASGITransport(app=app),
            timeout=None,
            headers={
                "x-ccdexplorer-key": app_settings.ccdexplorer_api_key
                or environment["CCDEXPLORER_API_KEY"]
            },
        )
        app.r = r
        init_time = dt.datetime.now().astimezone(dt.timezone.utc) - timedelta(seconds=10)
        app.users_last_requested = init_time
        app.exchange_rates_last_requested = init_time
        app.exchange_rates_historical_last_requested = init_time
        app.memos_last_requested = init_time
        app.blocks_per_day_last_requested = init_time
        app.api_keys_last_requested = init_time
        app.api_keys = await app.state.get_api_keys_fn(
            motormongo=app.motormongo, app=app, for_="lifespan"
        )
        app.exchange_rates = None
        app.exchange_rates_historical = None
        app.blocks_per_day = None
        app.memos = None
        app.REQUEST_LIMIT = 500  # default request limit for the API

        try:
            yield
        finally:
            # ---- tear down in reverse order ----
            await _aclose(app.tooter) if created["tooter"] else None
            await _aclose(app.grpcclient) if created["grpc"] else None
            await _aclose(app.mongodb) if created["mongo"] else None
            await _aclose(app.motormongo) if created["motor"] else None
            await app.httpx_client.aclose()

    app = FastAPI(
        lifespan=lifespan,
        # docs_url=None,
        swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"},
        openapi_tags=tags_metadata,
        separate_input_output_schemas=False,
        title="CCDExplorer.io API",
        summary="The API service for CCDExplorer.io.",
        version="1.0.0",
        contact={
            "name": "explorer.ccd on Telegram",
        },
        license_info={
            "name": "Apache 2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
        },
    )
    app.state.get_api_keys_fn = app_settings.get_api_keys_fn
    app.mount("/static", StaticFiles(directory=app_settings.static_dir), name="static")
    app.state.templates = Jinja2Templates(directory=app_settings.templates_dir)
    app.state.templates = app.state.templates
    app.mount("/node", StaticFiles(directory=app_settings.node_modules_dir), name="node_modules")

    origins = [
        "http://127.0.0.1:7000",
        "https://127.0.0.1:7000",
        # MCP Inspector
        "http://127.0.0.1:6274",
        "http://localhost:6274",
        "http://127.0.0.1:6277",
        "http://localhost:6277",
        "http://api.ccdexplorer.io",
        "https://api.ccdexplorer.io",
        "http://dev-api.ccdexplorer.io",
        "https://dev-api.ccdexplorer.io",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=[
            "content-type",
            "authorization",
            "x-ccdexplorer-key",
            "mcp-session-id",
            "mcp-protocol-version",
            "x-mcp-proxy-auth",
        ],
    )
    if app_settings.motor_factory is not None:
        app.add_middleware(UsageMiddleware, mongomotor=app_settings.motor_factory())

    app.add_middleware(
        RateLimitMiddleware,
        authenticate=AUTH_FUNCTION,
        # if ever the plan to go to a sliding window technique, use this.
        # backend=SlidingRedisBackend(StrictRedis.from_url(REDIS_URL)),
        backend=RedisBackend(StrictRedis.from_url(REDIS_URL)),  # type: ignore
        on_auth_error=handle_auth_error,
        on_blocked=handle_429,
        config={r"^/v2": rate_limit_rules},
    )
    app.add_middleware(BearerTokenApiKeyMiddleware)
    Instrumentator().instrument(app)

    # @app.middleware("http")
    # async def log_cors_origin(request, call_next):
    #     if request.method == "OPTIONS":
    #         print(
    #             "OPTIONS",
    #             request.url.path,
    #             "Origin:",
    #             request.headers.get("origin"),
    #             "ACR-Method:",
    #             request.headers.get("access-control-request-method"),
    #             "ACR-Headers:",
    #             request.headers.get("access-control-request-headers"),
    #         )

    #     return await call_next(request)

    signing_key = (
        environment.get("CCDEXPLORER_MCP_SIGNING_KEY")
        or environment.get("CCDEXPLORER_API_KEY", "")
    )

    # ── OAuth discovery ──────────────────────────────────────────────────────
    @app.get("/.well-known/oauth-protected-resource", include_in_schema=False)
    async def mcp_oauth_protected_resource(request: Request) -> JSONResponse:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host", "api.ccdexplorer.io")
        base = f"https://{host}"
        return JSONResponse({
            "resource": f"{base}/mcp",
            "authorization_servers": [base],
            "bearer_methods_supported": ["header"],
            "scopes_supported": ["mcp"],
        })

    # ── Authorize page (Authorization Code + PKCE) ───────────────────────────
    @app.get("/authorize", include_in_schema=False)
    async def mcp_authorize(request: Request) -> HTMLResponse:
        p = request.query_params
        def esc(v: str | None) -> str:
            return _html.escape(v or "")
        return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>CCDExplorer — Authorize</title>
  <style>
    body{{font-family:system-ui,sans-serif;max-width:400px;margin:80px auto;padding:0 20px;color:#1a1a1a}}
    h2{{margin-bottom:.25rem}}
    p{{color:#555;font-size:.9rem;margin-bottom:1.5rem}}
    label{{display:block;font-size:.875rem;font-weight:500;margin-bottom:.4rem}}
    input[type=password]{{width:100%;padding:.5rem .75rem;border:1px solid #ccc;border-radius:6px;font-size:1rem;box-sizing:border-box;margin-bottom:1rem}}
    button{{background:#0f172a;color:#fff;border:none;padding:.6rem 1.5rem;border-radius:6px;font-size:1rem;cursor:pointer;width:100%}}
    button:hover{{background:#1e293b}}
  </style>
</head>
<body>
  <h2>CCDExplorer — Authorize Claude</h2>
  <p>Enter your CCDExplorer MCP API key (<code>API_CODEX_KEY</code>) to grant access.</p>
  <form method="post">
    <input type="hidden" name="redirect_uri" value="{esc(p.get('redirect_uri'))}">
    <input type="hidden" name="code_challenge" value="{esc(p.get('code_challenge'))}">
    <input type="hidden" name="code_challenge_method" value="{esc(p.get('code_challenge_method','S256'))}">
    <input type="hidden" name="state" value="{esc(p.get('state'))}">
    <label for="k">MCP API Key</label>
    <input type="password" id="k" name="api_key" placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" autofocus>
    <button type="submit">Authorize</button>
  </form>
</body>
</html>""")

    @app.post("/authorize", include_in_schema=False, response_model=None)
    async def mcp_authorize_post(request: Request) -> RedirectResponse | HTMLResponse:
        form = await request.form()
        api_key = str(form.get("api_key", ""))
        redirect_uri = str(form.get("redirect_uri", ""))
        code_challenge = str(form.get("code_challenge", ""))
        state = str(form.get("state", ""))
        if not api_key or not redirect_uri or not code_challenge:
            return HTMLResponse("Missing required parameters", status_code=400)
        code = _make_auth_code(api_key, code_challenge, signing_key)
        return RedirectResponse(url=f"{redirect_uri}?code={code}&state={state}", status_code=302)

    # ── Token endpoint ────────────────────────────────────────────────────────
    @app.post("/oauth/token", include_in_schema=False)
    async def oauth_token(request: Request) -> JSONResponse:
        form = await request.form()
        grant_type = str(form.get("grant_type", ""))
        if grant_type != "authorization_code":
            return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)
        code = str(form.get("code", ""))
        code_verifier = str(form.get("code_verifier", ""))
        data = _decode_auth_code(code, signing_key)
        if data is None:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        if not _verify_pkce_s256(code_verifier, data["cc"]):
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        return JSONResponse({
            "access_token": data["k"],
            "token_type": "Bearer",
            "expires_in": 86400,
            "scope": "mcp",
        })

    # ── Fake dynamic client registration ─────────────────────────────────────
    @app.post("/oauth/register", include_in_schema=False, status_code=201)
    async def oauth_register(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:
            body = {}
        return JSONResponse({
            "client_id": body.get("client_name", "mcp-client"),
            "client_id_issued_at": int(time.time()),
            "redirect_uris": body.get("redirect_uris", []),
            "grant_types": ["authorization_code"],
            "token_endpoint_auth_method": "none",
            "client_name": body.get("client_name", "MCP Client"),
        }, status_code=201)

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        # Create a registry that reads from PROMETHEUS_MULTIPROC_DIR
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)

        data = generate_latest(registry)
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    # # V2
    app.include_router(account_v2.router)
    app.include_router(accounts_v2.router)
    app.include_router(transaction_v2.router)
    app.include_router(transactions_v2.router)
    app.include_router(token_v2.router)
    app.include_router(tokens_v2.router)
    app.include_router(block_v2.router)
    app.include_router(blocks_v2.router)
    app.include_router(markets_v2.router)
    app.include_router(contract_v2.router)
    app.include_router(contracts_v2.router)
    app.include_router(misc_v2.router)
    app.include_router(module_v2.router)
    app.include_router(modules_v2.router)
    app.include_router(smart_wallet_v2.router)
    app.include_router(smart_wallets_v2.router)
    app.include_router(plt_v2.router)
    app.include_router(plts_v2.router)

    # auth, content, key management
    app.include_router(auth.router)
    app.include_router(home.router)
    app.include_router(account.router)
    app.include_router(plans.router)

    # site user
    app.include_router(site_user_v2.router)

    app.state.templates.env.filters["datetime_to_date"] = datetime_to_date
    app.state.templates.env.filters["datetime_to_date_and_time_no_sec"] = (
        datetime_to_date_and_time_no_sec
    )
    app.state.templates.env.filters["seperator_no_decimals"] = seperator_no_decimals

    app.state.templates.env.filters["humanize_timedelta"] = humanize_timedelta

    use_route_names_as_operation_ids(app)

    # ── MCP server (fastapi-mcp) ───────────────────────────────────────────
    async def verify_mcp_key(request: Request) -> None:
        api_key = request.headers.get("x-ccdexplorer-key", "").strip()
        if not api_key:
            auth = request.headers.get("authorization", "")
            scheme, _, creds = auth.partition(" ")
            if scheme.lower() == "bearer":
                api_key = creds.strip()
        if not api_key:
            raise HTTPException(status_code=401, detail="API key required")
        keys = await request.app.state.get_api_keys_fn(
            motormongo=request.app.motormongo, app=request.app, for_="mcp_auth"
        )
        if api_key not in keys:
            raise HTTPException(status_code=401, detail="Invalid API key")

    host = environment.get("CCDEXPLORER_API_BASE_URL", "https://api.ccdexplorer.io").rstrip("/")
    mcp = FastApiMCP(
        app,
        name="CCDExplorer MCP",
        include_tags=[
            "Account", "Accounts",
            "Transaction", "Transactions",
            "Token", "Tokens",
            "Block", "Blocks",
            "Contract", "Contracts",
            "Module", "Modules",
            "Smart Wallet", "Smart Wallets",
            "Protocol-Level Token", "Protocol-Level Tokens",
            "Misc", "Markets",
        ],
        auth_config=AuthConfig(
            dependencies=[Depends(verify_mcp_key)],
            custom_oauth_metadata=OAuthMetadata(
                issuer=host,
                authorization_endpoint=f"{host}/authorize",
                token_endpoint=f"{host}/oauth/token",
                registration_endpoint=f"{host}/oauth/register",
                grant_types_supported=["authorization_code"],
                response_types_supported=["code"],
                scopes_supported=["mcp"],
                code_challenge_methods_supported=["S256"],
                token_endpoint_auth_methods_supported=["none"],
            ),
        ),
    )
    mcp.mount_http()

    return app
