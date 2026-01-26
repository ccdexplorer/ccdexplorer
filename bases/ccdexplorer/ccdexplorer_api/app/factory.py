# ruff: noqa: F403, F405, E402, E501, E722, F401
# pyright: reportAttributeAccessIssue=false
import datetime as dt
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Callable, Optional

import httpx
import importlib
import humanize
import urllib3
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.mongodb.core import CollectionsUtilities
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from httpx import ASGITransport
_prometheus_client = importlib.import_module("prometheus_client")
CONTENT_TYPE_LATEST = _prometheus_client.CONTENT_TYPE_LATEST
CollectorRegistry = _prometheus_client.CollectorRegistry
generate_latest = _prometheus_client.generate_latest
multiprocess = _prometheus_client.multiprocess

# from fastapi_mcp import FastApiMCP
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

from .ratelimiting import AUTH_FUNCTION, handle_429, handle_auth_error

if environment["SITE_URL"] != "http://127.0.0.1:8000":
    sentry_sdk.init(
        dsn=environment["SENTRY_DSN"],
        traces_sample_rate=1.0,
        _experiments={"continuous_profiling_auto_start": True},
    )


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

    # mcp = FastApiMCP(
    #     app,
    #     name="CCDexplorer.io MCP Server",
    #     description="The CCDExplorer.io API MCP server",
    # )
    # # Mount the MCP server directly to your FastAPI app
    # mcp.mount()

    origins = [
        "http://127.0.0.1:7000",
        "https://127.0.0.1:7000",
        "http://api.ccdexplorer.io",
        "https://api.ccdexplorer.io",
        "http://dev-api.ccdexplorer.io",
        "https://dev-api.ccdexplorer.io",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
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
    Instrumentator().instrument(app)

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

    return app
