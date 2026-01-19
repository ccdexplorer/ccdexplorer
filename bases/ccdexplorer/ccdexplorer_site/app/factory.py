# ruff: noqa: F403, F405, E402, E501, E722, F401
# pyright: reportAttributeAccessIssue=false
import datetime as dt
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import httpx
import urllib3
from ccdexplorer.ccdexplorer_site.app.utils import *  # noqa: F403
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountInfo,
    CCD_IpInfo,
)
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from httpx import ASGITransport, Request
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    multiprocess,
)

# from fastapi_mcp import FastApiMCP
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

urllib3.disable_warnings()

import pickle

import sentry_sdk
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ccdexplorer.ccdexplorer_site.app.routers import (
    account,
    account_pool,
    account_tab_tokens,
    account_tab_transactions,
    account_tab_validator,
    block,
    home,
    node,
    nodes,
    projects,
    smart_contract_tab_tokens,
    smart_contracts,
    smart_wallets,
    staking,
    statistics,
    tokens,
    tools,
    transaction,
    usersv2,
)
from ccdexplorer.ccdexplorer_site.app.routers.charts import (
    charts_home,
    sc_accounts_growth,
    sc_active_addresses,
    sc_holders,
    sc_plt_transfers,
    sc_transactions_count,
)
from ccdexplorer.ccdexplorer_site.app.utils import add_account_info_to_cache, get_url_from_api
from ccdexplorer.env import environment
from fastapi.middleware.gzip import GZipMiddleware

scheduler = AsyncIOScheduler(timezone=dt.UTC)


if environment["SITE_URL"] != "http://127.0.0.1:8000":
    sentry_sdk.init(
        dsn=environment["SENTRY_DSN"],
        traces_sample_rate=1.0,
        _experiments={"continuous_profiling_auto_start": True},
    )


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


# def datetime_to_date(value: dt.datetime):
#     return f"{value:%Y-%m-%d}"


# def datetime_to_date_and_time_no_sec(value: dt.datetime):
#     return f"{value:%Y-%m-%d %H:%M} UTC"


# def seperator_no_decimals(value: int):
#     return f"{int(value):,.0f}"


# def humanize_timedelta(value: dt.timedelta):
#     return humanize.precisedelta(value, suppress=["days"], format="%0.0f")


if environment["SITE_URL"] != "http://127.0.0.1:8000":
    sentry_sdk.init(
        dsn=environment["SENTRY_DSN"],
        traces_sample_rate=1.0,
        _experiments={"continuous_profiling_auto_start": True},
    )


async def log_request(request):
    print(
        f"Request event hook: {request.method} {request.url} {request.headers} - Waiting for response"
    )


async def log_response(response):
    request = response.request
    print(
        f"Response event hook: {request.method} {request.url} {response.headers}- Status {response.status_code}"
    )


def read_addresses_if_available(app):
    print("Start getting addresses to indexes.")
    app.addresses_to_indexes = {"mainnet": {}, "testnet": {}}
    app.addresses_to_indexes_complete = {"mainnet": {}, "testnet": {}}
    app.max_index_known = {"mainnet": 0, "testnet": 0}
    try:
        for net in ["mainnet", "testnet"]:
            print(f"Start getting addresses to indexes for {net}.")
            with open(
                f"{app.app_settings.addresses_dir}/{net}_addresses_to_indexes.pickle", "rb"
            ) as fp:  # Unpickling
                app.addresses_to_indexes[net] = pickle.load(fp)
                app.max_index_known[net] = max(app.addresses_to_indexes[net].values())

            with open(
                f"{app.app_settings.addresses_dir}/{net}_addresses_to_indexes_complete.pickle", "rb"
            ) as fp:  # Unpickling
                app.addresses_to_indexes_complete[net] = pickle.load(fp)
    except Exception as error:
        print(f"ERROR getting addresses: {error}")


class AppSettings(BaseModel):
    static_dir: Path
    templates_dir: Path
    node_modules_dir: Path
    addresses_dir: Path
    ccdexplorer_api_key: str | None = None
    api_url: str | None = None


def create_app(app_settings: AppSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.api_url = app_settings.api_url or environment["API_URL"]
        app.app_settings = app_settings
        app.httpx_client = httpx.AsyncClient(
            transport=ASGITransport(app=app),
            timeout=None,
            headers={
                "x-ccdexplorer-key": app_settings.ccdexplorer_api_key
                or environment["CCDEXPLORER_API_KEY"]
            },
        )

        init_time = dt.datetime.now().astimezone(dt.timezone.utc) - timedelta(seconds=10)

        app.api_url = environment["API_URL"]
        app.httpx_client = httpx.AsyncClient(
            # event_hooks={"request": [log_request], "response": [log_response]},
            timeout=None,
            headers={"x-ccdexplorer-key": environment["CCDEXPLORER_API_KEY"]},
        )
        app.env = environment
        app.credential_issuers = None
        app.env["API_KEY"] = str(uuid.uuid1())
        app.labeled_accounts_last_requested = init_time
        app.users_last_requested = init_time
        app.nodes_last_requested = init_time
        app.credential_issuers_last_requested = init_time
        app.consensus_last_requested = init_time
        app.staking_pools_last_requested = init_time
        app.exchange_rates_last_requested = init_time
        app.tags = None
        app.nodes = None
        read_addresses_if_available(app)
        app.schema_cache = {"mainnet": {}, "testnet": {}}
        app.token_information_cache = {"mainnet": {}, "testnet": {}}
        app.schema_cache = {"mainnet": {}, "testnet": {}}
        app.cns_domain_cache = {"mainnet": {}, "testnet": {}}
        app.blocks_cache = {"mainnet": [], "testnet": []}
        app.last_finalized_block = {"mainnet": 0, "testnet": 0}

        app.transactions_cache = {"mainnet": [], "testnet": []}
        app.accounts_cache = {"mainnet": [], "testnet": []}
        app.identity_providers_cache = {"mainnet": {}, "testnet": {}}
        app.consensus_cache = {"mainnet": {}, "testnet": {}}
        app.plt_cache = {"mainnet": {}, "testnet": {}}
        app.primed_suspended_cache = {}
        app.staking_pools_cache = {
            "open_for_all": {},
            "closed_for_new": {},
            "closed_for_all": {},
        }
        app.community_labels = None
        app.community_labels_last_requested = init_time
        await repeated_task_get_staking_pools(app)
        await repeated_task_get_accounts_id_providers(app)
        await repeated_task_get_community_labeled_accounts(app)
        scheduler.start()
        yield
        scheduler.shutdown()
        await app.httpx_client.aclose()
        print("END")
        pass

        try:
            yield
        finally:
            await app.httpx_client.aclose()

    app = FastAPI(
        lifespan=lifespan,
        # docs_url=None,
        swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"},
        separate_input_output_schemas=False,
        title="ccdexplorer.io",
        summary="ccdexplorer.io.",
        version="1.0.0",
        contact={
            "name": "explorer.ccd on Telegram",
        },
        license_info={
            "name": "Apache 2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
        },
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    app.mount("/static", StaticFiles(directory=app_settings.static_dir), name="static")
    app.mount("/node", StaticFiles(directory=app_settings.node_modules_dir), name="node_modules")
    app.mount("/addresses", StaticFiles(directory=app_settings.addresses_dir), name="addresses")

    app.state.templates = Jinja2Templates(directory=app_settings.templates_dir)
    app.state.templates = app.state.templates
    app.templates = app.state.templates

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

    Instrumentator().instrument(app)

    @app.get("/metrics")
    def metrics() -> Response:
        # Create a registry that reads from PROMETHEUS_MULTIPROC_DIR
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)

        data = generate_latest(registry)
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(home.router)
    app.include_router(transaction.router)
    app.include_router(block.router)
    app.include_router(account.router)
    app.include_router(account_tab_transactions.router)
    app.include_router(account_tab_tokens.router)
    app.include_router(account_tab_validator.router)
    app.include_router(account_pool.router)
    app.include_router(node.router)
    app.include_router(smart_contracts.router)
    app.include_router(smart_contract_tab_tokens.router)
    app.include_router(tools.router)
    app.include_router(projects.router)
    app.include_router(usersv2.router)
    app.include_router(statistics.router)
    app.include_router(tokens.router)
    app.include_router(nodes.router)
    app.include_router(staking.router)
    app.include_router(smart_wallets.router)
    app.include_router(charts_home.router)
    app.include_router(sc_accounts_growth.router)
    app.include_router(sc_transactions_count.router)
    app.include_router(sc_active_addresses.router)
    app.include_router(sc_holders.router)
    app.include_router(sc_plt_transfers.router)

    @app.exception_handler(404)
    async def exception_handler_404(request: Request, exc: Exception):
        return request.app.templates.TemplateResponse(
            "base/error.html",
            {
                "env": request.app.env,
                "request": request,
                "error": "Can't find the page you are looking for!",
            },
        )

    @app.exception_handler(500)
    async def exception_handler_500(request: Request, exc: Exception):
        return request.app.templates.TemplateResponse(
            "base/error.html",
            {
                "env": request.app.env,
                "request": request,
                "error": "Something's not quite right!",
            },
        )

    @scheduler.scheduled_job("interval", seconds=5, args=[app])
    async def repeated_task_get_blocks_and_transactions(app: FastAPI):
        for net in ["mainnet", "testnet"]:
            api_result = await get_url_from_api(
                f"{app.api_url}/v2/{net}/blocks/last/50", app.httpx_client
            )
            app.blocks_cache[net] = api_result.return_value if api_result.ok else None
            app.last_finalized_block[net] = (
                app.blocks_cache[net][0]["height"] if app.blocks_cache[net] else 0
            )
            api_result = await get_url_from_api(
                f"{app.api_url}/v2/{net}/transactions/last/50", app.httpx_client
            )
            app.transactions_cache[net] = api_result.return_value if api_result.ok else None

    @scheduler.scheduled_job("interval", seconds=20, args=[app])
    async def repeated_task_get_consensus(app: FastAPI):
        # for net in ["mainnet", "testnet"]:
        for net in ["mainnet", "testnet"]:
            try:
                api_result = await get_url_from_api(
                    f"{app.api_url}/v2/{net}/misc/consensus-detailed-status", app.httpx_client
                )
                app.consensus_cache[net] = api_result.return_value if api_result.ok else None
            except Exception as _:
                pass

    @scheduler.scheduled_job("interval", seconds=60, args=[app])
    async def repeated_task_get_community_labeled_accounts(app: FastAPI):
        try:
            print("community labeled accounts update...")
            api_result = await get_url_from_api(
                f"{app.api_url}/v2/mainnet/misc/community-labeled-accounts", app.httpx_client
            )
            app.community_labels = api_result.return_value if api_result.ok else None

            app.community_labels_last_requested = dt.datetime.now().astimezone(dt.timezone.utc)
        except httpx.HTTPError:
            app.community_labels = None

    @scheduler.scheduled_job("interval", seconds=60, args=[app])
    async def repeated_task_get_accounts_id_providers(app: FastAPI):
        for net in ["mainnet", "testnet"]:
            api_result = await get_url_from_api(
                f"{app.api_url}/v2/{net}/accounts/last/50", app.httpx_client
            )
            app.accounts_cache[net] = api_result.return_value if api_result.ok else []

            identity_providers = {}
            api_result = await get_url_from_api(
                f"{app.api_url}/v2/{net}/misc/identity-providers",
                app.httpx_client,
            )
            if api_result.return_value is None:
                continue
            for id in api_result.return_value:  # type: ignore
                id = CCD_IpInfo(**id)
                identity_providers[str(id.identity)] = {
                    "ip_identity": id.identity,
                    "ip_description": id.description.name,
                }

            app.identity_providers_cache[net] = identity_providers if api_result.ok else None
            if app.accounts_cache[net]:
                for account_ in app.accounts_cache[net]:
                    account_info: CCD_AccountInfo = CCD_AccountInfo(**account_["account_info"])
                    if account_info.address[:29] not in app.addresses_to_indexes[net]:
                        print(f"Adding {account_info.index} to cache... FROM SCHEDULE")
                        add_account_info_to_cache(account_info, app, net)

            api_result = await get_url_from_api(
                f"{app.api_url}/v2/{net}/plts/overview", app.httpx_client
            )
            app.plt_cache[net] = api_result.return_value if api_result.ok else None
            if not api_result.ok:
                print(f"ERROR: {api_result.return_value}")

    @scheduler.scheduled_job("interval", seconds=5 * 60, args=[app])
    async def repeated_task_get_staking_pools(app: FastAPI):
        print("Staking pools cache + primed suspended cache...")

        temp_dict = {}
        for status in ["open_for_all", "closed_for_new", "closed_for_all"]:
            api_result = await get_url_from_api(
                f"{app.api_url}/v2/mainnet/accounts/paydays/pools/{status}",
                app.httpx_client,
            )

            temp_dict[status] = api_result.return_value if api_result.ok else {}

        all_pool_with_status = []
        for status in ["open_for_all", "closed_for_all", "closed_for_new"]:
            for pool_id, pool in temp_dict.get(status, {}).items():  # use [] if it's a list
                pool_with_status = {**pool, "status": status}
                all_pool_with_status.append(pool_with_status)

        app.staking_pools_cache = all_pool_with_status

        api_result = await get_url_from_api(
            f"{app.api_url}/v2/mainnet/accounts/validators/primed-suspended",
            app.httpx_client,
        )
        app.primed_suspended_cache = api_result.return_value if api_result.ok else {}

        print("Staking pools cache + primed suspended cache updated.")

        app.templates.env.filters["datetime_format_day_only_from_ms_timestamp"] = (
            datetime_format_day_only_from_ms_timestamp  # noqa: F405
        )
        app.templates.env.filters["datetime_format_normal_from_ms_timestamp"] = (
            datetime_format_normal_from_ms_timestamp  # noqa: F405
        )
        app.templates.env.filters["datetime_format_day_only"] = datetime_format_day_only  # noqa: F405

        app.templates.env.filters["hex_to_rgba"] = hex_to_rgba  # noqa: F405
        app.templates.env.filters["token_value_no_decimals"] = token_value_no_decimals  # noqa: F405
        app.templates.env.filters["uptime"] = uptime  # noqa: F405
        app.templates.env.filters["round_x_decimal_with_comma"] = round_x_decimal_with_comma  # noqa: F405
        app.templates.env.filters["round_x_decimal_no_comma"] = round_x_decimal_no_comma  # noqa: F405
        app.templates.env.filters["lottery_power"] = lottery_power  # noqa: F405
        app.templates.env.filters["datetime_delta_format_since"] = datetime_delta_format_since  # noqa: F405
        app.templates.env.filters["datetime_delta_format_between_dates"] = (
            datetime_delta_format_between_dates  # noqa: F405
        )
        app.templates.env.filters["millify"] = millify  # noqa: F405
        app.templates.env.filters["datetime_delta_format_since_parse"] = (
            datetime_delta_format_since_parse  # noqa: F405
        )
        app.templates.env.filters["datetime_delta_format_until"] = datetime_delta_format_until  # noqa: F405
        app.templates.env.filters["instance_link_from_str"] = instance_link_from_str  # noqa: F405

        app.templates.env.filters["split_into_url_slug"] = split_into_url_slug  # noqa: F405
        app.templates.env.filters["token_amount_using_decimals"] = token_amount_using_decimals  # noqa: F405
        app.templates.env.filters["token_amount_using_decimals_rounded"] = (
            token_amount_using_decimals_rounded  # noqa: F405
        )
        app.templates.env.filters["from_address_to_index"] = from_address_to_index  # noqa: F405
        app.templates.env.filters["apy_perc"] = apy_perc  # noqa: F405

        app.templates.env.filters["cooldown_string"] = cooldown_string  # noqa: F405
        app.templates.env.filters["datetime_regular"] = datetime_regular  # noqa: F405
        app.templates.env.filters["datetime_regular_parse"] = datetime_regular_parse  # noqa: F405
        app.templates.env.filters["micro_ccd_display"] = micro_ccd_display  # noqa: F405
        app.templates.env.filters["tx_type_translator"] = tx_type_translator  # noqa: F405
        app.templates.env.filters["account_link"] = account_link  # noqa: F405
        app.templates.env.filters["block_height_link"] = block_height_link  # noqa: F405
        app.templates.env.filters["block_hash_link"] = block_hash_link  # noqa: F405
        app.templates.env.filters["tx_hash_link"] = tx_hash_link  # noqa: F405
        app.templates.env.filters["shorten_address"] = shorten_address  # noqa: F405
        app.templates.env.filters["micro_ccd_no_decimals"] = micro_ccd_no_decimals  # noqa: F405
        app.templates.env.filters["expectation_view"] = expectation_view  # noqa: F405
        app.templates.env.filters["format_preference_key"] = format_preference_key  # noqa: F405
        app.templates.env.filters["user_string"] = user_string  # noqa: F405
        app.templates.env.filters["split_contract_into_url_slug_and_token_id"] = (
            split_contract_into_url_slug_and_token_id  # noqa: F405
        )
        app.templates.env.filters["none"] = none  # noqa: F405
        app.templates.env.filters["account_label_on_index_for_label"] = (
            account_label_on_index_for_label  # noqa: F405
        )
        app.templates.env.filters["datetime_delta_format_schedule_node"] = (
            datetime_delta_format_schedule_node  # noqa: F405
        )
        app.templates.env.filters["datetime_delta_format_uptime"] = datetime_delta_format_uptime  # noqa: F405
        app.templates.env.filters["is_account_address"] = is_account_address  # noqa: F405

    return app
