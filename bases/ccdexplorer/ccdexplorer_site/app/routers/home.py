# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
# pyright: reportOptionalOperand=false
# pyright: reportOptionalIterable=false
# pyright: reportCallIssue=false
# pyright: reportReturnType=false

import datetime as dt
import os
from enum import Enum

import httpx2 as httpx
import pandas as pd
import plotly.express as px
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    CCD_BlockItemSummary,
    CCD_ConsensusDetailedStatus,
)
from ccdexplorer.ccdexplorer_site.app.classes.dressingroom import (
    MakeUp,
    MakeUpRequest,
    RequestingRoute,
)
import math
from ccdexplorer.site_user import SiteUser
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    RedirectResponse,
    Response,
    JSONResponse,
)
from pydantic import BaseModel

from ccdexplorer.env import environment, LIVE_PORT

from ccdexplorer.ccdexplorer_site.app.state import (
    get_httpx_client,
    get_labeled_accounts,
    get_user_detailsv2,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    ccdexplorer_plotly_template,
    create_dict_for_tabulator_display,
    create_dict_for_tabulator_display_for_accounts,
    create_dict_for_tabulator_display_for_blocks,
    get_url_from_api,
    millify,
    refresh_consensus_cache,
    tx_type_translation,
    tx_type_translation_for_js,
)

router = APIRouter()


class MarketCapInfo(Enum):
    TPS = 0
    TX_COUNT = 1
    TOKENS_COUNT = 2
    CMC = 3
    ACCOUNTS_COUNT = 4
    VALIDATORS_COUNT = 5


async def get_marketcap_info(
    httpx_client: httpx.AsyncClient,
    info: MarketCapInfo,
    api_url: str,
    net: str = "mainnet",
):
    if info == MarketCapInfo.TPS:
        url = f"{api_url}/v2/{net}/transactions/info/tps"
    elif info == MarketCapInfo.TX_COUNT:
        url = f"{api_url}/v2/{net}/transactions/info/count"
    elif info == MarketCapInfo.TOKENS_COUNT:
        url = f"{api_url}/v2/{net}/tokens/info/count"
    elif info == MarketCapInfo.CMC:
        url = f"{api_url}/v2/markets/info"
    elif info == MarketCapInfo.ACCOUNTS_COUNT:
        url = f"{api_url}/v2/{net}/accounts/info/count"
    elif info == MarketCapInfo.VALIDATORS_COUNT:
        url = f"{api_url}/v2/{net}/misc/validator-nodes/count"

    api_result = await get_url_from_api(url, httpx_client)
    response = api_result.return_value if api_result.ok else None
    return response


@router.get("/live_port")
async def live_port(request: Request):
    return LIVE_PORT


@router.get("/", response_class=RedirectResponse)
async def home(
    request: Request,
) -> RedirectResponse:
    response = RedirectResponse(url="/mainnet", status_code=302)
    return response


@router.get("/{net}", response_class=HTMLResponse)
async def redirect_to_mainnet(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)  # type: ignore
    user: SiteUser | None = await get_user_detailsv2(request)
    request.state.api_calls = {}
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}

    if net == "mainnet":
        request.state.api_calls["TPS"] = (
            f"{request.app.api_url}/docs#/Transactions/get_transactions_tps"
        )
        request.state.api_calls["Markets Info"] = (
            f"{request.app.api_url}/docs#/Markets/get_markets_info"
        )

    request.state.api_calls["Tx Count"] = (
        f"{request.app.api_url}/docs#/Transactions/get_transactions_count_estimate"
    )

    request.state.api_calls["Account Count"] = (
        f"{request.app.api_url}/docs#/Accounts/get_accounts_count_estimate"
    )
    request.state.api_calls["Latest Blocks"] = f"{request.app.api_url}/docs#/Blocks/get_last_blocks"
    request.state.api_calls["Latest Txs"] = (
        f"{request.app.api_url}/docs#/Transactions/get_last_transactions"
    )
    return request.app.templates.TemplateResponse(
        request,
        "home/home.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "API_KEY": request.app.env["CCDEXPLORER_API_KEY"],
        },
    )


@router.post("/mainnet/ajax_market_cap_table", response_class=HTMLResponse)
async def ajax_market_cap_table(
    request: Request,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    theme = "dark"
    body = await request.body()
    if body:
        theme = body.decode("utf-8").split("=")[1]

    api_url = request.app.api_url

    tps_table = {"hour_tps": 0}
    total_txs = 0
    total_accounts = 0
    total_tokens = 0
    total_validators = 0

    try:
        tps_table = await get_marketcap_info(httpx_client, MarketCapInfo.TPS, api_url)
        if not tps_table:
            tps_table = {"hour_tps": 0}

        total_txs = await get_marketcap_info(
            httpx_client, MarketCapInfo.TX_COUNT, api_url, "mainnet"
        )
        if not total_txs:
            total_txs = 0

        total_tokens = await get_marketcap_info(
            httpx_client, MarketCapInfo.TOKENS_COUNT, api_url, "mainnet"
        )
        if not total_tokens:
            total_tokens = 0

        cmc = await get_marketcap_info(httpx_client, MarketCapInfo.CMC, api_url)
        if not cmc:
            cmc = {
                "cmc_rank": 0,
                "quote": {
                    "USD": {
                        "price": 0,
                        "percent_change_24h": 0,
                        "market_cap": 0,
                    }
                },
            }
        total_accounts = await get_marketcap_info(
            httpx_client, MarketCapInfo.ACCOUNTS_COUNT, api_url
        )
        if not total_accounts:
            total_accounts = 0

        total_validators = await get_marketcap_info(
            httpx_client, MarketCapInfo.VALIDATORS_COUNT, api_url
        )
        if not total_validators:
            total_validators = 0

    except Exception as error:
        print(error)

    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    html = request.app.templates.TemplateResponse(
        request,
        "home/crypto_dashboard.html",
        {
            "request": request,
            "net": "mainnet",
            "theme": theme,
            "cmc_rank": cmc["cmc_rank"],
            "tps_h": tps_table["hour_tps"],
            "total_txs": millify(total_txs),
            "total_validators": total_validators,
            "ccd_price": f"{cmc['quote']['USD']['price']:,.5f}",
            "ccd_change": cmc["quote"]["USD"]["percent_change_24h"],
            "market_cap": millify(float(cmc["quote"]["USD"].get("fully_diluted_market_cap", 0))),
            "total_accounts": f"{total_accounts:,.0f}",
            "total_tokens": f"{(total_tokens / 1_000_000):,.1f}M",
        },
    )
    request.state.last_requests["marketcap"] = html
    return html


@router.get("/{net}/search_all/{value}", response_class=HTMLResponse)
async def search_all(
    request: Request,
    net: str,
    value: int | str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)  # type: ignore

    user: SiteUser | None = await get_user_detailsv2(request)
    single_urls = {}
    # order: account, block, transaction, lock, module, instance, token
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/accounts/search/{value}",
        httpx_client,
    )
    accounts_list = api_result.return_value if api_result.ok else []
    for a in accounts_list:
        if a["account_index"] != value:
            single_urls[f"account-{a['account_index']}"] = (
                f"/{net}/account/{accounts_list[0]['account_index']}/alias/{value[29:]}"
            )
        else:
            single_urls[f"account-{a['account_index']}"] = (
                f"/{net}/account/{accounts_list[0]['account_index']}"
            )

    try:
        value = int(value)
        if value <= request.app.max_index_known[net]:  # type: ignore
            return RedirectResponse(url=f"/{net}/account/{value}", status_code=302)  # type: ignore
    except ValueError:
        pass

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/block/{value}",
        httpx_client,
    )
    block_info = CCD_BlockInfo(**api_result.return_value) if api_result.ok else None  # type: ignore

    if block_info:
        single_urls["block"] = f"/{net}/block/{block_info.height}"

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/transaction/{value}", httpx_client
    )
    tx = CCD_BlockItemSummary(**api_result.return_value) if api_result.ok else None  # type: ignore

    if tx:
        single_urls["transaction"] = f"/{net}/transaction/{tx.hash}"

    if net == "devnet":
        # PLT locks only exist on devnet for now (see plt_locks_feature_visible) - skip
        # the lookup entirely on mainnet/testnet rather than firing a guaranteed-404 API
        # call on every single search there.
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/plt/lock/{value}", httpx_client
        )
        lock_info = api_result.return_value if api_result.ok else None
        if lock_info:
            single_urls["lock"] = f"/{net}/tokens/plt/lock/{value}"

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/modules/search/{value}",
        httpx_client,
    )
    modules = api_result.return_value if api_result.ok else []

    for m in modules:
        single_urls[f"module-{m['_id']}"] = f"/{net}/module/{modules[0]['_id']}"

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/tokens/search/{value}",
        httpx_client,
    )
    tokens = api_result.return_value if api_result.ok else []

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/contracts/search/{value}",
        httpx_client,
    )
    contracts = api_result.return_value if api_result.ok else []

    for c in contracts:
        single_urls[f"contract-{c['_id']}"] = f"/{net}/contract/{contracts[0]['_id']}"

    for t in tokens:
        single_urls[f"token-{t['_id']}"] = f"/{net}/tokens/{tokens[0]['_id']}"

    if len(single_urls) == 1:
        return RedirectResponse(url=single_urls[list(single_urls.keys())[0]], status_code=302)  # type: ignore

    html = request.app.templates.TemplateResponse(
        request,
        "home/search_all.html",
        {
            "request": request,
            "env": environment,
            "net": net,
            "tags": tags,
            "user": user,
            "accounts_list": accounts_list,
            "block_info": block_info,
            "tx": tx,
            "contracts": contracts,
            "tokens": tokens,
            "modules": modules,
            "search_value": value,
        },
    )

    return html


class SearchRequest(BaseModel):
    selector: str
    value: str
    net: str


@router.post(
    "/search",
    response_class=RedirectResponse,
)
async def search(request: Request, search_request: SearchRequest):
    if search_request.selector == "all":
        url = f"/{search_request.net}/search_all/{search_request.value}"
    if search_request.selector == "account":
        url = f"/{search_request.net}/account/{search_request.value.replace(' ', '')}"
    if search_request.selector == "block":
        url = f"/{search_request.net}/block/{search_request.value.replace(' ', '').replace(',', '').replace('.', '')}"
    if search_request.selector == "transaction":
        url = f"/{search_request.net}/transaction/{search_request.value.replace(' ', '')}"
    if search_request.selector == "contract":
        url = f"/{search_request.net}/contract/{search_request.value.replace(' ', '')}/0"
    if search_request.selector == "module":
        url = f"/{search_request.net}/module/{search_request.value.replace(' ', '')}"
    if search_request.selector == "lock":
        url = f"/{search_request.net}/tokens/plt/lock/{search_request.value.replace(' ', '')}"
    if search_request.selector == "token":
        search_request.value = search_request.value.replace(" ", "")
        if "-" in search_request.value:
            splits = search_request.value.split("-")
            if len(splits) == 2:
                contract_index = splits[0]
                try:
                    contract_index = int(contract_index)
                except:  # noqa: E722
                    pass
                if isinstance(contract_index, str):
                    tag = contract_index
                    contract_index = None
                    token_id = splits[1]
                    url = f"/{search_request.net}/tokens/{tag}/{token_id}"
                else:
                    token_id = splits[1]
                    url = f"/{search_request.net}/token/{contract_index}/0/{token_id}"
            else:
                # it's a tag name with a - in it...
                token_id = splits[len(splits) - 1]
                tag = "-".join(splits[: (len(splits) - 1)])
                url = f"/{search_request.net}/tokens/{tag}/{token_id}"
        else:
            try:
                contract_index = int(search_request.value)
            except:  # noqa: E722
                contract_index = search_request.value
            if isinstance(contract_index, str):
                url = f"/{search_request.net}/tokens/{contract_index}"
            else:
                contract_index = search_request.value
                token_id = "_"
                url = f"/{search_request.net}/token/{contract_index}/0/{token_id}"

    if url:
        response = RedirectResponse(url=url, status_code=200)
        # note do not remove this header! Very strange things will happen.
        # The new route is requested, however the browser page remains the same!
        response.headers["HX-Redirect"] = url
        return response


class SearchPlaceholderRequest(BaseModel):
    net: str
    search_selector: str


@router.post(
    "/search_placeholder",
    response_class=Response,
)
async def search_placeholder(
    request: Request,
    placeholder_request: SearchPlaceholderRequest,
):
    search_selector = placeholder_request.search_selector
    if search_selector == "all":
        return "Search ..."
    if search_selector == "block":
        return "Search for Block height or hash"
    if search_selector == "transaction":
        return "Search for Tx hash"
    if search_selector == "account":
        return "Search for account index or address"
    if search_selector == "contract":
        return "Search for contract index"
    if search_selector == "module":
        return "Search for module hash"
    if search_selector == "token":
        return '"contract_index-token_id"'
    if search_selector == "lock":
        return "Search for lock id"


@router.get("/tmp/{filename}", response_class=FileResponse)
async def tmp_files(request: Request, filename: str):
    file_path = f"/tmp/{filename}"

    # Check if the file exists
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return FileResponse(file_path, headers=headers, media_type="text/csv")


@router.post(
    "/home_tx_graph",
    response_class=Response,
)
async def home_tx_graph(
    request: Request,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    theme = "dark"
    body = await request.body()
    if body:
        theme = body.decode("utf-8").split("=")[1]
    today = f"{dt.datetime.now().astimezone(tz=dt.timezone.utc):%Y-%m-%d}"

    minus_6m = (
        f"{dt.datetime.now().astimezone(tz=dt.timezone.utc) - dt.timedelta(weeks=26):%Y-%m-%d}"
    )
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/mainnet/misc/tx-data/all/{minus_6m}/{today}",
        httpx_client,
    )
    all_data = api_result.return_value if api_result.ok else None
    if not all_data:
        error = "Request error getting tx data.."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": "mainnet",
            },
        )

    df = pd.json_normalize(all_data)
    df["sum_all"] = df.sum(axis=1, numeric_only=True)
    pass
    df["date"] = pd.to_datetime(df["date"])
    rng = ["#3EB7E5"]
    fig = px.scatter(
        df,
        x="date",
        y=df["sum_all"],
        # y=signal.savgol_filter(
        #     df["sum_all"], 60, 2  # window size used for filtering
        # ),  # order of fitted polynomial,
        color_discrete_sequence=rng,
        template=ccdexplorer_plotly_template(theme),
    )
    fig.update_yaxes(
        title_text=None,
        showgrid=False,
        linewidth=0,
        zerolinecolor="rgba(0,0,0,0)",
    )
    fig.update_traces(mode="lines")
    fig.update_xaxes(
        title=None,
        type="date",
        showgrid=False,
        linewidth=0,
        zerolinecolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        height=135,
        # width=320,
        margin=dict(l=0, r=0, t=0, b=0),
    )
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    html = fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )
    request.state.last_requests["txs_graph"] = html
    return html


@router.get("/{net}/ajax_last_finalized_height", response_class=HTMLResponse)
async def ajax_last_finalized_height(request: Request, net: str):
    """The last finalized block's height and slot time, for pollers.

    Two pipe-separated fields, because the running payday row shows both and
    one poll is cheaper than two. No gRPC call and no query: the scheduler
    keeps the head block in memory, so this is a dict lookup. Stamping
    chain_head_last_seen is what tells repeated_task_get_chain_head to keep it
    refreshing at 1s for this net -- without a poller it falls back to the 5s
    blocks job, whose cache is the fallback read here.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        return HTMLResponse("0|")
    request.app.chain_head_last_seen[net] = dt.datetime.now().astimezone(dt.timezone.utc)

    head = request.app.chain_head.get(net)
    if not head:
        blocks = request.app.blocks_cache.get(net)
        head = blocks[0] if blocks else None
    if not head:
        return HTMLResponse(f"{request.app.last_finalized_block.get(net, 0)}|")

    slot_time = head.get("slot_time")
    return HTMLResponse(f"{head.get('height', 0)}|{slot_time or ''}")


@router.get("/{net}/ajax_last_blocks", response_class=HTMLResponse)
async def ajax_last_blocks(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    # tells repeated_task_get_home_tables to keep these caches on a 2s refresh
    # while someone is here; otherwise the 5s baseline job covers them
    request.app.home_tables_last_seen[net] = dt.datetime.now().astimezone(dt.timezone.utc)
    latest_blocks = request.app.blocks_cache.get(net)
    if latest_blocks is None:
        error = f"Request error getting the most recent blocks on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error-request.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    result = [CCD_BlockInfo(**x) for x in latest_blocks[:10]]
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    html = request.app.templates.TemplateResponse(
        request,
        "home/last_blocks_table.html",
        {
            "request": request,
            "blocks": result,
            "net": net,
            "tags": tags,
            "user": user,
        },
    )
    request.state.last_requests["blocks"] = html
    return html


@router.get("/{net}/ajax_last_transactions", response_class=HTMLResponse)
async def ajax_last_txs(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    # api_result = await get_url_from_api(
    #     f"{request.app.api_url}/v2/{net}/transactions/last/10", httpx_client
    # )
    # latest_txs = api_result.return_value if api_result.ok else None
    # tells repeated_task_get_home_tables to keep these caches on a 2s refresh
    # while someone is here; otherwise the 5s baseline job covers them
    request.app.home_tables_last_seen[net] = dt.datetime.now().astimezone(dt.timezone.utc)
    latest_txs = request.app.transactions_cache.get(net)
    if not latest_txs:
        error = f"Request error getting the most recent transactions on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error-request.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    result = [CCD_BlockItemSummary(**x) for x in latest_txs[:10]]
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    html = request.app.templates.TemplateResponse(
        request,
        "home/last_txs_table.html",
        {
            "request": request,
            "tx_type_translation": tx_type_translation,
            "txs": result,
            "net": net,
            "tags": tags,
            "user": user,
        },
    )

    request.state.last_requests["txs"] = html
    return html


@router.get("/{net}/ajax_last_transactions_own_page", response_class=HTMLResponse)
async def ajax_last_txs_own_page(
    request: Request,
    net: str,
    page: int = Query(),
    size: int = Query(),
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)
    skip = (page - 1) * size
    user: SiteUser | None = await get_user_detailsv2(request)
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/transactions/paginated/skip/{skip}/limit/{size}",
        httpx_client,
    )
    latest_txs = api_result.return_value if api_result.ok else []
    # latest_txs = request.app.transactions_cache.get(net)
    if not latest_txs:
        error = f"Request error getting the most recent transactions on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    tb_made_up_txs = []

    for transaction in latest_txs["transactions"]:
        transaction = CCD_BlockItemSummary(**transaction)
        makeup_request = MakeUpRequest(
            **{
                "net": net,
                "httpx_client": httpx_client,
                "tags": tags,
                "user": user,
                "app": request.app,
                "requesting_route": RequestingRoute.transactions,
            }
        )

        classified_tx = await MakeUp(makeup_request=makeup_request).prepare_for_display(
            transaction, "", False
        )

        type_additional_info, sender = await classified_tx.transform_for_tabulator()

        tb_made_up_txs.append(
            create_dict_for_tabulator_display(net, classified_tx, type_additional_info, sender)
        )
    total_rows = latest_txs["total_rows"]
    last_page = math.ceil(total_rows / size)
    return JSONResponse(
        {
            "data": tb_made_up_txs,
            "last_page": max(1, last_page),
            "last_row": total_rows,
        }
    )


@router.get("/{net}/ajax_last_blocks_own_page", response_class=HTMLResponse)
async def ajax_last_blocks_own_page(
    request: Request,
    net: str,
    page: int = Query(),
    size: int = Query(),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    # latest_blocks = request.app.blocks_cache.get(net)
    skip = (page - 1) * size
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/blocks/{skip}/{size}",
        httpx_client,
    )
    blocks_result = api_result.return_value if api_result.ok else {}
    if not blocks_result:
        error = f"Request error getting the most recent blocks on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    result = [create_dict_for_tabulator_display_for_blocks(net, x) for x in blocks_result["blocks"]]
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    total_rows = blocks_result["total_rows"]
    last_page = math.ceil(total_rows / size)
    return JSONResponse(
        {
            "data": result,
            "last_page": max(1, last_page),
            "last_row": total_rows,
        }
    )


@router.get("/{net}/ajax_blocks/new", response_class=HTMLResponse)
async def ajax_last_blocks_since(
    request: Request,
    net: str,
    since_height: int = Query(),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/blocks/newer/than/{since_height}",
        httpx_client,
    )
    blocks_result = api_result.return_value if api_result.ok else {}
    if not blocks_result:
        error = f"Request error getting the most recent blocks on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    result = [create_dict_for_tabulator_display_for_blocks(net, x) for x in blocks_result]
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    return JSONResponse(result)


@router.get("/{net}/ajax_accounts/new", response_class=HTMLResponse)
async def ajax_last_accounts_since(
    request: Request,
    net: str,
    since_index: int = Query(),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/accounts/newer/than/{since_index}",
        httpx_client,
    )
    accounts_result = api_result.return_value if api_result.ok else []
    if not api_result.ok:
        error = f"Request error getting the most recent accounts on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    result = [
        create_dict_for_tabulator_display_for_accounts(net, request.app, x) for x in accounts_result
    ]
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    return JSONResponse(result)


@router.get("/{net}/ajax_transactions/new", response_class=HTMLResponse)
async def ajax_last_transactions_since(
    request: Request,
    net: str,
    height: int = Query(),
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    user: SiteUser | None = await get_user_detailsv2(request)
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/transactions/newer/than/{height}",
        httpx_client,
    )
    txs_result = api_result.return_value if api_result.ok else []
    if not api_result.ok:
        error = f"Request error getting the most recent blocks on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )
    tb_made_up_txs = []
    for transaction in txs_result:
        transaction = CCD_BlockItemSummary(**transaction)
        makeup_request = MakeUpRequest(
            **{
                "net": net,
                "httpx_client": httpx_client,
                "tags": tags,
                "user": user,
                "app": request.app,
                "requesting_route": RequestingRoute.block,
            }
        )

        classified_tx = await MakeUp(makeup_request=makeup_request).prepare_for_display(
            transaction, "", False
        )

        type_additional_info, sender = await classified_tx.transform_for_tabulator()

        tb_made_up_txs.append(
            create_dict_for_tabulator_display(net, classified_tx, type_additional_info, sender)
        )

    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    return JSONResponse(tb_made_up_txs)


@router.get("/{net}/ajax_last_accounts_own_page", response_class=HTMLResponse)
async def ajax_last_accounts_own_page(
    request: Request,
    net: str,
    page: int = Query(),
    size: int = Query(),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    skip = (page - 1) * size
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/accounts/paginated/skip/{skip}/limit/{size}",
        httpx_client,
    )
    accounts_result: dict = api_result.return_value if api_result.ok else {}
    accounts = accounts_result.get("accounts", [])
    if not accounts_result:
        error = f"Request error getting the most recent accounts on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    result = [create_dict_for_tabulator_display_for_accounts(net, request.app, x) for x in accounts]
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    total_rows = accounts_result["total_rows"]
    last_page = math.ceil(total_rows / size)
    return JSONResponse(
        {
            "data": result,
            "last_page": max(1, last_page),
            "last_row": total_rows,
        }
    )


@router.get("/{net}/transactions", response_class=HTMLResponse)
async def transactions_page(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    request.state.api_calls = {}
    request.state.api_calls["Latest Txs"] = (
        f"{request.app.api_url}/docs#/Transactions/get_last_transactions"
    )

    return request.app.templates.TemplateResponse(
        request,
        "home/transactions.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "tx_type_translation": tx_type_translation,
            "tx_type_translation_from_python": tx_type_translation_for_js(),
            "net": net,
            "API_KEY": request.app.env["CCDEXPLORER_API_KEY"],
        },
    )


@router.get("/{net}/blocks", response_class=HTMLResponse)
async def blocks_page(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    request.state.api_calls = {}
    request.state.api_calls["Latest Blocks"] = f"{request.app.api_url}/docs#/Blocks/get_last_blocks"

    return request.app.templates.TemplateResponse(
        request,
        "home/blocks.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "API_KEY": request.app.env["CCDEXPLORER_API_KEY"],
        },
    )


@router.get("/{net}/accounts", response_class=HTMLResponse)
async def accounts_page(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    request.state.api_calls = {}
    request.state.api_calls["Latest Accounts"] = (
        f"{request.app.api_url}/docs#/Accounts/get_last_accounts"
    )
    return request.app.templates.TemplateResponse(
        request,
        "home/accounts.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "API_KEY": request.app.env["CCDEXPLORER_API_KEY"],
        },
    )


@router.get("/{net}/ajax_consensus_own_page", response_class=HTMLResponse)
async def ajax_consensus_own_page(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    request.app.consensus_last_seen[net] = dt.datetime.now().astimezone(dt.timezone.utc)
    if not request.app.consensus_cache.get(net):
        # net was idle (or never watched since startup) -- don't wait for
        # the next scheduled tick, fetch now so this first response isn't
        # built from the empty {} placeholder
        await refresh_consensus_cache(request.app, net)
    user: SiteUser | None = await get_user_detailsv2(request)
    try:
        latest_consensus = CCD_ConsensusDetailedStatus(**request.app.consensus_cache.get(net))
    except Exception as error:
        print(f"ERROR parsing consensus detailed status for {net}: {error}")
        latest_consensus = None

    if not latest_consensus:
        error = f"Request error getting the most recent consensus detailed status on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error-request.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )
    latest_consensus.epoch_bakers = None
    if "last_requests" not in request.state._state:
        request.state.last_requests = {}
    html = request.app.templates.TemplateResponse(
        request,
        "home/last_consensus_own_page.html",
        {
            "request": request,
            "consensus": latest_consensus,
            "net": net,
            "tags": tags,
            "user": user,
        },
    )
    request.state.last_requests["blocks"] = html
    return html


@router.get("/{net}/consensus-details", response_class=HTMLResponse)
async def consensus_page(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    request.state.api_calls = {}
    request.state.api_calls["Consensus Detailed Status"] = (
        f"{request.app.api_url}/docs#/Misc/get_consensus_detailed_status"
    )
    return request.app.templates.TemplateResponse(
        request,
        "home/consensus.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "API_KEY": request.app.env["CCDEXPLORER_API_KEY"],
        },
    )


def build_consensus_visualization(
    consensus: CCD_ConsensusDetailedStatus,
    finalized_history: list[dict],
    new_hashes: set | None = None,
) -> dict:
    new_hashes = new_hashes or set()
    round_by_block: dict[str, int] = {}
    baker_by_block: dict[str, int] = {}
    for existing_block in consensus.round_existing_blocks or []:
        round_by_block[existing_block.block] = existing_block.round
        baker_by_block[existing_block.block] = existing_block.baker

    qc_rounds = {qc.round for qc in (consensus.round_existing_qcs or [])}
    highest_certified_hash = consensus.round_status.highest_certified_block.block_hash

    # last_finalized_block_height is relative to the current protocol era's
    # genesis, not the chain's absolute height - add genesis_block_height to
    # get the height shown everywhere else on the site
    base_height = consensus.genesis_block_height + consensus.last_finalized_block_height + 1

    # round_existing_qcs only tracks a narrow window of recent rounds, so a
    # block that got its QC a while ago (and is now aging toward finalization)
    # can fall out of that window and look uncertified even though it isn't.
    # A later block can't have a QC without its ancestor chain already being
    # certified, so every height at or below the current tip's height is
    # certified too - treat that as certified even if its round already aged
    # out of round_existing_qcs.
    tip_height = None
    for i, branch in enumerate(consensus.branches or []):
        if highest_certified_hash in branch.blocks_at_branch_height:
            tip_height = base_height + i
            break

    rows = []
    for i, branch in enumerate(consensus.branches or []):
        height = base_height + i
        blocks = []
        for block_hash in branch.blocks_at_branch_height:
            round_number = round_by_block.get(block_hash)
            has_qc = (round_number is not None and round_number in qc_rounds) or (
                tip_height is not None and height <= tip_height
            )
            blocks.append(
                {
                    "hash": block_hash,
                    "short_hash": block_hash[:4],
                    "round": round_number,
                    "has_qc": has_qc,
                    "baker": baker_by_block.get(block_hash),
                    "is_terminal": block_hash == consensus.terminal_block,
                    "is_highest_certified": block_hash == highest_certified_hash,
                    "is_new": block_hash in new_hashes,
                }
            )
        rows.append({"height": height, "blocks": blocks})
    rows.reverse()  # newest height first

    finalized_stack = [
        {"hash": entry["hash"], "short_hash": entry["hash"][:4], "height": entry["height"]}
        for entry in finalized_history
    ]

    return {
        "current_round": consensus.round_status.current_round,
        "current_epoch": consensus.round_status.current_epoch,
        "current_timeout": consensus.round_status.current_timeout,
        "terminal_block": consensus.terminal_block,
        "non_finalized_transaction_count": consensus.non_finalized_transaction_count,
        "live_blocks_count": len(consensus.block_table.live_blocks) if consensus.block_table else 0,
        "dead_block_cache_size": consensus.block_table.dead_block_cache_size
        if consensus.block_table
        else 0,
        "rows": rows,
        "finalized_stack": finalized_stack,
    }


@router.get("/{net}/consensus", response_class=HTMLResponse)
async def consensus_visual_page(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
) -> HTMLResponse:
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    user: SiteUser | None = await get_user_detailsv2(request)
    return request.app.templates.TemplateResponse(
        request,
        "home/consensus-visual.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "API_KEY": request.app.env["CCDEXPLORER_API_KEY"],
        },
    )


@router.get("/{net}/ajax_consensus_visual", response_class=HTMLResponse)
async def ajax_consensus_visual(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
):
    if net not in ["mainnet", "testnet", "devnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    request.app.consensus_last_seen[net] = dt.datetime.now().astimezone(dt.timezone.utc)
    if not request.app.consensus_cache.get(net):
        # net was idle (or never watched since startup) -- don't wait for
        # the next scheduled tick, fetch now so this first response isn't
        # built from the empty {} placeholder
        await refresh_consensus_cache(request.app, net)
    user: SiteUser | None = await get_user_detailsv2(request)
    try:
        latest_consensus = CCD_ConsensusDetailedStatus(**request.app.consensus_cache.get(net))
        finalized_history = list(request.app.finalized_history.get(net, []))
        new_hashes = request.app.new_block_hashes.get(net, set())
        visualization = build_consensus_visualization(
            latest_consensus, finalized_history, new_hashes
        )
    except Exception as error:
        print(f"ERROR building consensus visualization for {net}: {error}")
        visualization = None

    if not visualization:
        error = f"Request error getting the most recent consensus detailed status on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error-request.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    return request.app.templates.TemplateResponse(
        request,
        "home/consensus_visual_partial.html",
        {
            "request": request,
            "v": visualization,
            "net": net,
            "user": user,
        },
    )


@router.get("/misc/release-notes", response_class=HTMLResponse)
async def release_notes(
    request: Request,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/misc/release-notes",
        httpx_client,
    )
    release_notes = api_result.return_value if api_result.ok else []
    request.state.api_calls = {}
    request.state.api_calls["Release Notes"] = f"{request.app.api_url}/docs#/Misc/get_release_notes"
    return request.app.templates.TemplateResponse(
        request,
        "base/release_notes.html",
        {"env": request.app.env, "request": request, "release_notes": release_notes},
    )


@router.get("/misc/privacy-policy", response_class=HTMLResponse)
async def privacy_policy(
    request: Request,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    request.state.api_calls = {}
    request.state.api_calls["None"] = ""
    return request.app.templates.TemplateResponse(
        request,
        "base/privacy_policy.html",
        {"env": request.app.env, "request": request},
    )


@router.get("/misc/support", response_class=HTMLResponse)
async def support_explorer(
    request: Request,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    request.state.api_calls = {}
    request.state.api_calls["None"] = ""
    return request.app.templates.TemplateResponse(
        request,
        "base/support.html",
        {
            "env": request.app.env,
            "request": request,
            "donations_account_id": "3cunMsEt2M3o9Rwgs2pNdsCWZKB5MkhcVbQheFHrvjjcRLSoGP",
        },
    )


@router.get("/{net}/partners/acn", response_class=HTMLResponse)
async def partners_acn(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
):
    if net not in ["mainnet"]:
        return RedirectResponse(url="/mainnet", status_code=302)

    request.state.api_calls = {}
    request.state.api_calls["None"] = ""
    return request.app.templates.TemplateResponse(
        request,
        "base/partners/acn.html",
        {
            "env": request.app.env,
            "request": request,
        },
    )


@router.get("/health")
async def health():
    return {"status": "ok"}
