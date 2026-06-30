import datetime as dt
from typing import Optional

import httpx
import pandas as pd
import plotly.express as px


import plotly.graph_objects as go
import polars as polars
from ccdexplorer.domain.mongo import MongoTypeTokensTag
from ccdexplorer.domain.credential import Identity
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountInfo,
    CCD_BlockInfo,
    CCD_BlockItemSummary,
    CCD_ContractAddress,
)
from ccdexplorer.site_user import SiteUser
from fastapi import APIRouter, Depends, Query, Request, Body
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from plotly.subplots import make_subplots
from pydantic import BaseModel

from ccdexplorer.ccdexplorer_site.app.classes.dressingroom import (
    MakeUp,
    MakeUpRequest,
    RequestingRoute,
)
from ccdexplorer.ccdexplorer_site.app.classes.sankey import SanKey, ClusterSanKey
from ccdexplorer.env import environment

from ccdexplorer.ccdexplorer_site.app.state import (
    get_httpx_client,
    get_labeled_accounts,
    get_user_detailsv2,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    account_link,
    add_account_info_to_cache,
    ccdexplorer_plotly_template,
    from_address_to_index,
    get_url_from_api,
    post_url_from_api,
    tx_type_translation_for_js,
    create_dict_for_tabulator_display,
    create_dict_for_tabulator_display_for_rewards,
    return_plot_response,
)
import math

router = APIRouter()


@router.get(
    "/account_rewards/{net}/{account_id}",
    response_class=HTMLResponse,
)
async def get_paginated_account_rewards(
    request: Request,
    net: str,
    account_id: str,
    page: int = Query(),
    size: int = Query(),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    # recurring: Recurring = Depends(get_recurring),
    tags: dict = Depends(get_labeled_accounts),
):
    """ """

    skip = (page - 1) * size
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/rewards/{skip}/{size}",
        httpx_client,
    )
    rewards_result = api_result.return_value if api_result.ok else None

    if not rewards_result:
        error = f"Request error getting staking rewards for account at {account_id} on {net}."
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

    rewards_result_data = rewards_result["data"]
    total_rows = rewards_result["total_rows"]
    last_page = math.ceil(total_rows / size)
    tb_made_up_rewards = []
    for reward in rewards_result_data:
        dct = create_dict_for_tabulator_display_for_rewards(net, reward)

        tb_made_up_rewards.append(dct)
    return JSONResponse(
        {
            "data": tb_made_up_rewards,
            "last_page": max(1, last_page),
            "last_row": total_rows,
        }
    )


@router.post("/{net}/account/apy-graph/{index_or_hash}/{request_type}", response_class=Response)
async def account_apy_graph(
    request: Request,
    net: str,
    index_or_hash: int | str,
    request_type: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    tags: dict = Depends(get_labeled_accounts),
):
    theme = "dark"
    body = await request.body()
    if body:
        theme = body.decode("utf-8").split("=")[1]

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{index_or_hash}/apy-data/{request_type}",
        httpx_client,
    )
    result = api_result.return_value if api_result.ok else None
    if not result:
        return None

    df = pd.DataFrame(result)
    melt = df.melt(
        id_vars=["date"],
        value_vars=["30d", "90d", "180d"],
        var_name="APY period",
        value_name="value",
    )

    days_alive = (dt.datetime.now() - dt.datetime(2022, 6, 24, 9, 0, 0)).total_seconds() / (
        60 * 60 * 24
    )

    domain_pd = polars.Series(
        [dt.datetime(2022, 6, 24) + dt.timedelta(days=x) for x in range(0, int(days_alive) + 1)]
    ).to_list()

    rng = [
        "#AE7CF7",
        "#70B785",
        "#6E97F7",
    ]
    fig = px.scatter(
        melt,
        x="date",
        y="value",
        color="APY period",
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
        range=[domain_pd[0], domain_pd[-1]],
        linewidth=0,
        zerolinecolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        height=250,
        # width=320,
        legend=dict(orientation="h"),
        title=(f"Moving Averages for {request_type.capitalize()} APY"),
        legend_y=-0.7,
        # legend_x=0.7,
        title_y=0.35,
        margin=dict(l=0, r=0, t=0, b=0),
    )
    html = fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )

    return html


@router.post(
    "/{net}/account/rewards-bucketed/{account_id}/{account_index}",
    response_class=Response,
)
async def account_rewards_bucketed(
    request: Request,
    net: str,
    account_id: str,
    account_index: int,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    theme = "dark"
    body = await request.body()
    if body:
        theme = body.decode("utf-8").split("=")[1]
    if net == "mainnet":
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{account_id}/staking-rewards-bucketed",
            httpx_client,
        )
        result_pp = api_result.return_value if api_result.ok else None
        if not result_pp:
            return None

        ff = [
            {
                "date": x["date"],
                "Transaction Fees": x["reward"]["transaction_fees"] / 1_000_000,
                "Validation Reward": x["reward"]["baker_reward"] / 1_000_000,
                "Finalization Reward": x["reward"]["finalization_reward"] / 1_000_000,
            }
            for x in result_pp
        ]
        total_finalization = sum(x["Finalization Reward"] for x in ff)

        if total_finalization == 0:
            for x in ff:
                x.pop("Finalization Reward", None)
            aggsum = [
                polars.sum("Transaction Fees"),
                polars.sum("Validation Reward"),
            ]
            melt_on = [
                "Transaction Fees",
                "Validation Reward",
            ]

        else:
            aggsum = [
                polars.sum("Transaction Fees"),
                polars.sum("Validation Reward"),
                polars.sum("Finalization Reward"),
            ]
            melt_on = [
                "Transaction Fees",
                "Validation Reward",
                "Finalization Reward",
            ]
        df = polars.DataFrame(ff)
        df = df.with_columns(polars.col("date").str.to_datetime("%Y-%m-%d"))

        if len(df) > 0:
            # df["date"] = polars.Expr.str.to_date(df["date"])

            df_group = (
                df.sort("date")
                .group_by_dynamic("date", every="1mo", period="1mo", offset="0mo", closed="right")
                .agg(aggsum)
            )

            melt = df_group.unpivot(
                index=["date"],  # Identifier column(s) to keep
                on=melt_on,  # Columns to unpivot
                variable_name="Reward Type",  # Name of the variable column
                value_name="Value",  # Name of the value column
            )

            rng = [
                "#AE7CF7",
                "#70B785",
                "#6E97F7",
            ]
            fig = px.bar(
                melt,
                x="date",
                y="Value",  # order of fitted polynomial,
                color="Reward Type",
                color_discrete_sequence=rng,
                template=ccdexplorer_plotly_template(theme),
            )
            fig.update_yaxes(
                title_text=None,
                showgrid=False,
                linewidth=0,
                zerolinecolor="rgba(0,0,0,0)",
            )
            # fig.update_traces(mode="lines")
            fig.update_xaxes(
                title=None,
                type="date",
                showgrid=False,
                linewidth=0,
                zerolinecolor="rgba(0,0,0,0)",
            )
            fig.update_layout(
                height=250,
                # width=320,
                legend_y=-0.25,
                title=f"Rewards from staking/delegation per month<br>Account {account_index}",
                # title_y=0.46,
                legend=dict(orientation="h"),
                # legend_x=0.7,
                margin=dict(l=0, r=0, t=0, b=0),
            )
            if "last_requests" not in request.state._state:
                request.state.last_requests = {}
            html = fig.to_html(
                config={"responsive": True, "displayModeBar": False},
                full_html=False,
                include_plotlyjs=False,
            )

            return html
        else:
            return ""
    else:
        return ""


async def get_account_rewards_download(
    request: Request,
    index_or_hash: int | str,
    year_month: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/mainnet/account/{index_or_hash}/staking-rewards/{year_month}",
        httpx_client,
    )
    staking_rewards = api_result.return_value if api_result.ok else None
    if staking_rewards:
        filename = f"/tmp/staking_rewards - {index_or_hash} - {year_month}.csv"
        df = pd.json_normalize(staking_rewards)
        df["reward.transaction_fees"] = df["reward.transaction_fees"] / 1_000_000
        df["reward.baker_reward"] = df["reward.baker_reward"] / 1_000_000
        df["reward.finalization_reward"] = df["reward.finalization_reward"] / 1_000_000
        df = df.drop(columns=["reward.account"])
        df.to_csv(filename, index=False)
        return filename
    else:
        return "NO_REWARDS"


@router.get(
    "/ajax_rewards_download/{index_or_hash}/{year_month}",
    response_model=None,
)
async def get_account_rewards_download_route(
    request: Request,
    index_or_hash: int | str,
    year_month: str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    filename = await get_account_rewards_download(request, index_or_hash, year_month, httpx_client)
    return filename


@router.get("/{net}/account/{index_or_hash}")
@router.get("/{net}/account/{index_or_hash}/alias/{alias_portion}")
async def get_account(
    request: Request,
    net: str,
    index_or_hash: int | str,
    alias_portion: str | None = None,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    request.state.api_calls = {}
    if "hx-request" in request.headers:
        print("hx-request", request.headers["hx-request"])
    user: SiteUser | None = await get_user_detailsv2(request)

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{index_or_hash}/info",
        httpx_client,
    )
    account_info = CCD_AccountInfo(**api_result.return_value) if api_result.ok else None  # type: ignore
    if not account_info:
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/smart-wallet/public-key/{index_or_hash}",
            httpx_client,
        )
        public_key_info = api_result.return_value if api_result.ok else None
        if not public_key_info:
            error = f"Can't find the account at {index_or_hash} on {net}."
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
        else:
            wallet_contract = CCD_ContractAddress.from_str(
                public_key_info["wallet_contract_address"]
            )
            url = f"/{net}/smart-wallet/{wallet_contract.index}/{wallet_contract.subindex}/{public_key_info['public_key']}"
            response = RedirectResponse(url, status_code=303)
            return response

    # add to address_to_indexes cache if not already there.
    if account_info.address[:29] not in request.app.addresses_to_indexes[net]:
        print(f"Adding {account_info.index} to cache...")
        add_account_info_to_cache(account_info, request.app, net)

    # make sure the alias isn't just the canonical address
    if alias_portion == account_info.address[29:]:
        alias_portion = None
    if not alias_portion:
        account_id = account_info.address
    else:
        account_id = f"{account_info.address[:29]}{alias_portion}"
    account_index = account_info.index

    if account_info.stake:
        delegation = account_info.stake.delegator
        validator_id = (
            account_info.stake.baker.baker_info.baker_id if account_info.stake.baker else None
        )
    else:
        delegation = None
        validator_id = None

    identity = Identity(account_info)

    account_link_found = account_link(account_id, net, user, tags, request.app)

    delegation_target_address = None
    account_apy_object = None
    if delegation:
        delegation_target_address = delegation.target
        if delegation_target_address.baker:
            api_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/account/{delegation_target_address.baker}/staking-rewards-object/delegator",
                httpx_client,
            )
        else:
            # passive_delegation
            api_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/account/passive_delegation/staking-rewards-object/passive_delegation",
                httpx_client,
            )
        account_apy_object = api_result.return_value if api_result.ok else None

    if validator_id is not None:
        account_is_validator = True

        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{validator_id}/pool-info",
            httpx_client,
        )
        pool = api_result.return_value if api_result.ok else None

        # api_result = await get_url_from_api(
        #     f"{request.app.api_url}/v2/{net}/account/{account_index}/node", httpx_client
        # )
        # node = api_result.return_value if api_result.ok else None

        # api_result = await get_url_from_api(
        #     f"{request.app.api_url}/v2/{net}/account/{validator_id}/earliest-win-time",
        #     httpx_client,
        # )
        # earliest_win_time = api_result.return_value if api_result.ok else None

        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{validator_id}/staking-rewards-object/delegator",
            httpx_client,
        )
        pool_apy_object = api_result.return_value if api_result.ok else None

        # api_result = await get_url_from_api(
        #     f"{request.app.api_url}/v2/{net}/account/{account_id}/staking-rewards-object/account",
        #     httpx_client,
        # )
        # account_apy_object = api_result.return_value if api_result.ok else None

    else:
        account_is_validator = False
        pool = None
        pool_apy_object = None

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/rewards-available",
        httpx_client,
    )
    rewards_for_account_available = api_result.return_value if api_result.ok else None

    if pool_apy_object or account_apy_object or rewards_for_account_available:
        rewards_filename = (
            f"/tmp/staking_rewards - {account_id} - {dt.datetime.now().strftime('%Y-%m')}.csv"
        )
    else:
        rewards_filename = None

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/tokens-available{'/alias' if alias_portion else ''}",
        httpx_client,
    )
    tokens_available = api_result.return_value if api_result.ok else None

    tokens_value_USD = 0
    if tokens_available:
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{account_id}/fungible-tokens/USD",
            httpx_client,
        )
        tokens_value_USD = api_result.return_value if api_result.ok else 0
    assert isinstance(tokens_value_USD, (int, float))

    plts_value_USD = 0
    if tokens_available:
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{account_id}/plt/USD",
            httpx_client,
        )
        plts_value_USD = api_result.return_value if api_result.ok else 0
    assert isinstance(plts_value_USD, (int, float))
    tokens_value_USD += plts_value_USD

    ccd_balance_USD = 0
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/balance/USD",
        httpx_client,
    )
    ccd_balance_USD = api_result.return_value if api_result.ok else 0

    # TODO
    cns_domains_list = None  # cns_domains_registered(account_id)

    cis2_token_ids = []
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/token-symbols-for-flow",
        httpx_client,
    )
    cis2_token_ids = api_result.return_value if api_result.ok else []

    plt_ids = []
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/plt-symbols-for-flow",
        httpx_client,
    )
    plt_ids = api_result.return_value if api_result.ok else []
    token_ids = cis2_token_ids + plt_ids  # type: ignore

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/deployed",
        httpx_client,
    )

    deployed_in_genesis_block = False
    genesis_block_slot_time = None
    if (api_result.ok) and (api_result.return_value is None):
        # this account was created in the genesis block!
        deployed_in_genesis_block = True
        tx_deployed = None
        api_result = api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/block/0",
            httpx_client,
        )
        genesis_block_slot_time = (
            CCD_BlockInfo(**api_result.return_value).slot_time  # type: ignore
            if api_result.ok
            else dt.datetime.now().astimezone(dt.UTC)
        )
    else:
        tx_deployed = CCD_BlockItemSummary(**api_result.return_value) if api_result.ok else None  # type: ignore

    request.state.api_calls["Account Info"] = (
        f"{request.app.api_url}/docs#/Account/get_account_info"
    )
    request.state.api_calls["Identity Providers"] = (
        f"{request.app.api_url}/docs#/Misc/get_identity_providers"
    )
    request.state.api_calls["Account Transactions"] = (
        f"{request.app.api_url}/docs#/Account/get_account_txs"
    )
    request.state.api_calls["Rewards Available"] = (
        f"{request.app.api_url}/docs#/Account/get_bool_account_rewards_available"
    )
    request.state.api_calls["APY Data"] = (
        f"{request.app.api_url}/docs#/Account/get_account_apy_data_v2"
    )
    request.state.api_calls["Tokens Available"] = (
        f"{request.app.api_url}/docs#/Account/get_account_tokens_available"
    )
    request.state.api_calls["Smart Wallet Details"] = (
        f"{request.app.api_url}/docs#/Smart%20Wallet/get_smart_wallet_details_from_public_key"
    )
    request.state.api_calls["CCD Balance in USD"] = (
        f"{request.app.api_url}/docs#/Account/get_account_balance_in_USD"
    )
    request.state.api_calls["Fungible Tokens Value in USD"] = (
        f"{request.app.api_url}/docs#/Account/get_account_fungible_tokens_value_in_USD"
    )
    request.state.api_calls["PLT Value in USD"] = (
        f"{request.app.api_url}/docs#/Account/get_account_plt_tokens_value_in_USD"
    )
    request.state.api_calls["Token Symbols for Flow"] = (
        f"{request.app.api_url}/docs#/Account/get_account_token_symbols_for_flow"
    )
    request.state.api_calls["PLT Symbols for Flow"] = (
        f"{request.app.api_url}/docs#/Account/get_account_plt_symbols_for_flow"
    )
    request.state.api_calls["Verified Fungible Tokens"] = (
        f"{request.app.api_url}/docs#/Account/get_account_fungible_tokens_verified"
    )
    request.state.api_calls["Verified Non-Fungible Tokens"] = (
        f"{request.app.api_url}/docs#/Account/get_account_non_fungible_tokens_verified"
    )
    request.state.api_calls["Unverified Tokens"] = (
        f"{request.app.api_url}/docs#/Account/get_account_tokens_unverified"
    )
    request.state.api_calls["Aliases in Use"] = (
        f"{request.app.api_url}/docs#/Account/get_aliases_in_use_for_account"
    )
    request.state.api_calls["Deployed Tx"] = (
        f"{request.app.api_url}/docs#/Account/get_account_deployment_tx"
    )
    request.state.api_calls["Transactions for Flow"] = (
        f"{request.app.api_url}/docs#/Account/get_account_transactions_for_flow_graph"
    )
    request.state.api_calls["Rewards for Flow"] = (
        f"{request.app.api_url}/docs#/Account/get_account_rewards_for_flow_graph"
    )
    request.state.api_calls["Pool Info"] = (
        f"{request.app.api_url}/docs#/Account/get_validator_pool_info"
    )
    request.state.api_calls["Node"] = (
        f"{request.app.api_url}/docs#/Account/get_account_validator_node"
    )
    request.state.api_calls["Earliest Win Time"] = (
        f"{request.app.api_url}/docs#/Account/get_validator_earliest_win_time"
    )
    request.state.api_calls["Current Payday Stats"] = (
        f"{request.app.api_url}/docs#/Account/get_validator_current_payday_stats"
    )
    request.state.api_calls["Validator/Account Staking Rewards"] = (
        f"{request.app.api_url}/docs#/Account/get_staking_rewards_object"
    )

    request.state.api_calls["Pool Delegators"] = (
        f"{request.app.api_url}/docs#/Account/get_account_pool_delegators"
    )
    request.state.api_calls["Validator Tally"] = (
        f"{request.app.api_url}/docs#/Account/get_validator_tally"
    )
    request.state.api_calls["Validator Transactions"] = (
        f"{request.app.api_url}/docs#/Account/get_account_validator_txs"
    )

    request.state.api_calls["Staking Rewards"] = (
        f"{request.app.api_url}/docs#/Account/get_staking_rewards_bucketed"
    )
    request.state.api_calls["Validator Performance"] = (
        f"{request.app.api_url}/docs#/Account/get_validator_performance"
    )
    request.state.api_calls["Block Info"] = (
        f"{request.app.api_url}/docs#/Block/get_block_at_height_from_grpc"
    )
    # TODO
    exchange_rates = {"CCD": {"rate": 1}}
    if not alias_portion:
        return request.app.templates.TemplateResponse(
            request,
            "account/account_account.html",
            {
                "env": request.app.env,
                "rewards_no_stake": (account_info.stake.baker is None)  # type: ignore
                and (account_info.stake.delegator is None)  # type: ignore
                and rewards_for_account_available,
                "rewards_for_account_available": rewards_for_account_available,
                "account_id": account_id,
                "account_index": account_index,
                "token_ids_for_flow": token_ids,
                "tx_deployed": tx_deployed,
                "deployed_in_genesis_block": deployed_in_genesis_block,
                "genesis_block_slot_time": genesis_block_slot_time,
                "request": request,
                "exchange_rates": exchange_rates,
                "tokens_value_USD": tokens_value_USD,
                "ccd_balance_USD": ccd_balance_USD,
                "pool_apy_object": pool_apy_object,
                "account_apy_object": account_apy_object,
                "account": account_info,
                "pool": pool,
                "account_is_validator": account_is_validator,
                "cns_domains_list": cns_domains_list,
                "identity": identity,
                "identity_providers": request.app.identity_providers_cache[net],
                "delegation": delegation,
                "delegation_target_address": delegation_target_address,
                "net": net,
                "user": user,
                "tags": tags,
                "account_link_found": account_link_found,
                "tokens_available": tokens_available,
                "year_month": dt.datetime.now().strftime("%Y-%m"),
                "rewards_filename": rewards_filename,
                "tx_type_translation_from_python": tx_type_translation_for_js(),
                "alias_portion": alias_portion,
            },
        )
    else:
        return request.app.templates.TemplateResponse(
            request,
            "account/account_account_alias.html",
            {
                "env": request.app.env,
                "rewards_no_stake": (account_info.stake.baker is None)  # type: ignore
                and (account_info.stake.delegator is None)  # type: ignore
                and rewards_for_account_available,
                "rewards_for_account_available": rewards_for_account_available,
                "account_id": account_id,
                "account_index": account_index,
                "token_ids_for_flow": token_ids,
                "tx_deployed": tx_deployed,
                "deployed_in_genesis_block": deployed_in_genesis_block,
                "genesis_block_slot_time": genesis_block_slot_time,
                "request": request,
                "exchange_rates": exchange_rates,
                "tokens_value_USD": tokens_value_USD,
                "ccd_balance_USD": ccd_balance_USD,
                "pool_apy_object": pool_apy_object,
                "account_apy_object": account_apy_object,
                "account": account_info,
                "pool": pool,
                "account_is_validator": account_is_validator,
                "cns_domains_list": cns_domains_list,
                "identity": identity,
                "identity_providers": request.app.identity_providers_cache[net],
                "delegation": delegation,
                "delegation_target_address": delegation_target_address,
                "net": net,
                "user": user,
                "tags": tags,
                "account_link_found": account_link_found,
                "tokens_available": tokens_available,
                "year_month": dt.datetime.now().strftime("%Y-%m"),
                "rewards_filename": rewards_filename,
                "tx_type_translation_from_python": tx_type_translation_for_js(),
                "alias_portion": alias_portion,
            },
        )


class SanKeyParams(BaseModel):
    theme: str
    gte: str | int
    start_date: str
    end_date: str
    token: str


@router.post(
    "/ajax_sankey/{net}/{account_id}",
    response_class=HTMLResponse,
)
async def request_sankey(
    request: Request,
    net: str,
    account_id: str,
    post_params: SanKeyParams,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    user: SiteUser | None = await get_user_detailsv2(request)
    theme = post_params.theme
    gte = post_params.gte
    start_date = post_params.start_date
    end_date = post_params.end_date
    token = post_params.token

    if net == "testnet":
        return "Not available on testnet."
    if isinstance(gte, str):
        gte = int(gte.replace(",", "").replace(".", ""))

    sankey = SanKey(account_id, gte, request.app, net, token)
    if token == "CCD":
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{account_id}/transactions-for-flow/{gte}/{start_date}/{end_date}",
            httpx_client,
        )
        txs_for_account = api_result.return_value if api_result.ok else []

        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/account/{account_id}/rewards-for-flow/{start_date}/{end_date}",
            httpx_client,
        )
        account_rewards_total = api_result.return_value if api_result.ok else 0

        sankey.add_txs_for_account(
            txs_for_account,
            account_rewards_total,  # , exchange_rates
        )
    else:
        # tokens
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/plt/{token}/info",
            httpx_client,
        )
        plt_info = api_result.return_value if api_result.ok else None
        if plt_info:
            # PLT
            api_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/account/{account_id}/plt-transactions-for-flow/{token}/{gte}/{start_date}/{end_date}",
                httpx_client,
            )
            txs_for_account = api_result.return_value if api_result.ok else []

            sankey.add_plt_txs_for_account(
                txs_for_account,
                plt_info["token_state"]["module_state"]["governance_account"]["account"],
            )
        else:
            # CIS-2
            api_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/token/{token}/info",
                httpx_client,
            )
            token_tag = MongoTypeTokensTag(**api_result.return_value) if api_result.ok else None  # type: ignore
            if not token_tag:
                return None

            token_id = (
                f"{token_tag.contracts[0]}-"
                if token != "CCDOGE"
                else f"{token_tag.contracts[0]}-01"
            )

            decimals = token_tag.decimals
            display_name = token_tag.display_name

            api_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/account/{account_id}/token-transactions-for-flow/{token_id}/{gte}/{start_date}/{end_date}",
                httpx_client,
            )
            txs_for_account = api_result.return_value if api_result.ok else []

            sankey.add_txs_for_account_for_token(txs_for_account, decimals, display_name)  # type: ignore

    account_ids_to_lookup = {
        x[:29]: from_address_to_index(x[:29], net, request.app)
        for x in sankey.labels.keys()
        if len(x) > 28
    }

    await sankey.cross_the_streams(user, tags, account_ids_to_lookup)

    node_count = len(sankey.tagged_labels)

    sankey_height = max(650, min(1400, node_count * 18))

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=25,
                    thickness=10,
                    line=dict(color="grey", width=1.0),
                    label=sankey.tagged_labels,
                    color=sankey.colors,
                ),
                link=dict(
                    source=sankey.source,
                    target=sankey.target,
                    value=sankey.value,
                    color=sankey.colors,
                ),
            )
        ],
    )
    fig.update_traces(node_hoverlabel_font_shadow="auto", selector=dict(type="sankey"))
    fig.update_layout(
        template=ccdexplorer_plotly_template(theme),
        title_text=f"Flow diagram for account for token {token}",
        font_size=10,
        height=sankey_height,
    )

    sankey_html = fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )

    return request.app.templates.TemplateResponse(
        request,
        "account/account_graph_table.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "account_id": account_id,
            "token": token,
            "sankey_html": sankey_html,
            "graph_dict": sankey.graph_dict,
            "tags": tags,
            "app": request.app,
        },
    )


class CombinedFlowParams(BaseModel):
    theme: str
    # Comma-separated list of account indexes (sent as a string to keep json-enc
    # serialisation clean; arrays get mangled by the extension).
    accounts: str | list[int] = ""
    gte: str | int = 0
    start_date: str = ""
    end_date: str = ""
    token: str = "CCD"

    def account_indexes(self) -> list[int]:
        if isinstance(self.accounts, list):
            return self.accounts
        return [int(x) for x in self.accounts.split(",") if x.strip()]


@router.post(
    "/ajax_combined_sankey/{net}",
    response_class=HTMLResponse,
)
async def request_combined_sankey(
    request: Request,
    net: str,
    post_params: CombinedFlowParams,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    """Render the cluster flow.

    Uses the same data source (``transactions-for-flow`` + ``rewards-for-flow``,
    i.e. ``balance_movement``) and the same netting machinery as the single-account
    Flow tab, so a cluster of one account is identical to that tab. Transfers
    between cluster members are excluded from the external flows and reported
    separately as netted internal transfers.
    """
    user: SiteUser | None = await get_user_detailsv2(request)
    theme = post_params.theme

    def _empty(message: str):
        return request.app.templates.TemplateResponse(
            request,
            "account/account_combined_table.html",
            {
                "env": request.app.env,
                "request": request,
                "user": user,
                "net": net,
                "error": message,
                "tags": tags,
                "app": request.app,
            },
        )

    if net == "testnet":
        return _empty("Not available on testnet.")
    accounts = post_params.account_indexes()
    if not accounts:
        return _empty("Add at least one account to the cluster.")

    gte = post_params.gte
    if isinstance(gte, str):
        gte = int(gte.replace(",", "").replace(".", "") or 0)
    start_date = post_params.start_date
    end_date = post_params.end_date
    token = post_params.token or "CCD"

    # Resolve the cluster's account indexes to their canonical addresses.
    api_result = await post_url_from_api(
        f"{request.app.api_url}/v2/{net}/accounts/get-addresses",
        httpx_client,
        accounts,
    )
    index_to_address = api_result.return_value if api_result.ok else None
    member_addresses = list(index_to_address.values()) if index_to_address else []
    if not member_addresses:
        return _empty("None of the provided accounts could be resolved.")
    member_canonicals = {a[:29] for a in member_addresses}

    # Single-account cluster: centre the sankey on that account (mirrors Flow).
    center_label = member_addresses[0] if len(member_addresses) == 1 else None
    sankey = ClusterSanKey(
        gte, request.app, net, member_canonicals, token=token, center_label=center_label
    )

    if token == "CCD":
        # Same per-account data the Flow tab uses (balance_movement + rewards).
        all_txs: list = []
        rewards_total = 0
        for address in member_addresses:
            tx_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/account/{address}/transactions-for-flow/{gte}/{start_date}/{end_date}",
                httpx_client,
            )
            if tx_result.ok and tx_result.return_value:
                all_txs.extend(tx_result.return_value)

            rewards_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/account/{address}/rewards-for-flow/{start_date}/{end_date}",
                httpx_client,
            )
            if rewards_result.ok and rewards_result.return_value:
                rewards_total += rewards_result.return_value

        sankey.add_txs_for_cluster(all_txs, rewards_total)
    else:
        # Token flow: PLT if the token has a protocol-level definition, else CIS-2.
        plt_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/plt/{token}/info", httpx_client
        )
        plt_info = plt_result.return_value if plt_result.ok else None

        if plt_info:
            governance_account = plt_info["token_state"]["module_state"]["governance_account"][
                "account"
            ]
            all_txs = []
            for address in member_addresses:
                tx_result = await get_url_from_api(
                    f"{request.app.api_url}/v2/{net}/account/{address}/plt-transactions-for-flow/{token}/{gte}/{start_date}/{end_date}",
                    httpx_client,
                )
                if tx_result.ok and tx_result.return_value:
                    all_txs.extend(tx_result.return_value)
            sankey.add_plt_txs_for_cluster(all_txs, governance_account)
        else:
            info_result = await get_url_from_api(
                f"{request.app.api_url}/v2/{net}/token/{token}/info", httpx_client
            )
            token_tag = (
                MongoTypeTokensTag(**info_result.return_value)
                if info_result.ok and info_result.return_value
                else None
            )
            if not token_tag:
                return _empty(f"Could not resolve token {token}.")

            token_id = (
                f"{token_tag.contracts[0]}-"
                if token != "CCDOGE"
                else f"{token_tag.contracts[0]}-01"
            )
            all_events: list = []
            for address in member_addresses:
                tx_result = await get_url_from_api(
                    f"{request.app.api_url}/v2/{net}/account/{address}/token-transactions-for-flow/{token_id}/{gte}/{start_date}/{end_date}",
                    httpx_client,
                )
                if tx_result.ok and tx_result.return_value:
                    all_events.extend(tx_result.return_value)
            sankey.add_txs_for_cluster_for_token(
                all_events, token_tag.decimals, token_tag.display_name
            )

    account_ids_to_lookup = {
        x[:29]: from_address_to_index(x[:29], net, request.app)
        for x in sankey.labels.keys()
        if len(x) > 28
    }
    await sankey.cross_the_streams(user, tags, account_ids_to_lookup)

    node_count = len(sankey.tagged_labels)
    sankey_height = max(650, min(1400, node_count * 18))

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=25,
                    thickness=10,
                    line=dict(color="grey", width=1.0),
                    label=sankey.tagged_labels,
                    color=sankey.colors,
                ),
                link=dict(
                    source=sankey.source,
                    target=sankey.target,
                    value=sankey.value,
                    color=sankey.colors,
                ),
            )
        ],
    )
    if len(accounts) == 1:
        # Mirror the Flow tab for a single account.
        title_text = f"Flow diagram for account {accounts[0]} for token {token}"
    else:
        title_text = (
            f"Flow diagram for cluster consisting of: {', '.join(str(a) for a in sorted(accounts))}"
        )
    fig.update_traces(node_hoverlabel_font_shadow="auto", selector=dict(type="sankey"))
    fig.update_layout(
        template=ccdexplorer_plotly_template(theme),
        title_text=title_text,
        font_size=10,
        height=sankey_height,
    )
    cmb_sankey_html = fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )

    amount_received = sankey.graph_dict.get("amount_received", 0.0)
    amount_sent = sankey.graph_dict.get("amount_sent", 0.0)
    net_transfer = amount_received - amount_sent

    return request.app.templates.TemplateResponse(
        request,
        "account/account_combined_table.html",
        {
            "env": request.app.env,
            "request": request,
            "user": user,
            "net": net,
            "token": token,
            "cmb_sankey_html": cmb_sankey_html,
            "graph_dict": sankey.graph_dict,
            "n_accounts": len(accounts),
            "internal_total": sankey.internal_total,
            "internal_count": sankey.internal_count,
            "net_transfer_abs": abs(net_transfer),
            "net_direction": "In" if net_transfer >= 0 else "Out",
            "tags": tags,
            "app": request.app,
        },
    )


@router.get(
    "/account_transactions/{net}/{account_id}/{total_rows}",
    response_class=HTMLResponse,
)
async def get_account_transactions_for_tabulator(
    request: Request,
    net: str,
    account_id: str,
    total_rows: int,
    page: int = Query(),
    size: int = Query(),
    sort_key: Optional[str] = Query("block_height"),
    direction: Optional[str] = Query("desc"),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    # recurring: Recurring = Depends(get_recurring),
    tags: dict = Depends(get_labeled_accounts),
):
    """
    Transactions for account.
    """

    user: SiteUser | None = await get_user_detailsv2(request)

    skip = (page - 1) * size
    last_page = math.ceil(total_rows / size)
    sort_key = "block_height" if sort_key == "transaction.block_info.height" else sort_key
    sort_key = "effect_type" if sort_key == "transaction.type.contents" else sort_key
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/transactions/{skip}/{size}/{sort_key}/{direction}",
        httpx_client,
    )
    tx_result = api_result.return_value if api_result.ok else None
    if not tx_result:
        error = f"Request error getting transactions for account at {account_id} on {net}."
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
    else:
        tb_made_up_txs = []
        tx_result_transactions = tx_result["transactions"]

        if len(tx_result_transactions) > 0:
            for transaction in tx_result_transactions:
                transaction = CCD_BlockItemSummary(**transaction)
                makeup_request = MakeUpRequest(
                    **{
                        "net": net,
                        "httpx_client": httpx_client,
                        "tags": tags,
                        "user": user,
                        "app": request.app,
                        "requesting_route": RequestingRoute.account,
                    }
                )

                classified_tx = await MakeUp(makeup_request=makeup_request).prepare_for_display(
                    transaction, "", False
                )

                type_additional_info, sender = await classified_tx.transform_for_tabulator()

                tb_made_up_txs.append(
                    create_dict_for_tabulator_display(
                        net, classified_tx, type_additional_info, sender
                    )
                )
        return JSONResponse(
            {
                "data": tb_made_up_txs,
                "last_page": max(1, last_page),
                "last_row": total_rows,
            }
        )


def collapse_tokens_from_aliases_fungible(tokens: list):
    collaped_tokens = {}
    if tokens:
        for token in tokens:
            if token["token_address"] not in collaped_tokens:
                collaped_tokens[token["token_address"]] = token
            else:
                collaped_tokens[token["token_address"]]["token_amount"] = str(
                    int(collaped_tokens[token["token_address"]]["token_amount"])
                    + int(token["token_amount"])
                )
                collaped_tokens[token["token_address"]]["token_value"] = (
                    collaped_tokens[token["token_address"]]["token_value"] + token["token_value"]
                )

                collaped_tokens[token["token_address"]]["token_value_USD"] = (
                    collaped_tokens[token["token_address"]]["token_value_USD"]
                    + token["token_value_USD"]
                )

    return collaped_tokens


def collapse_tokens_from_aliases_non_fungible(tokens: list):
    collaped_tokens = {}
    if tokens:
        for token in tokens:
            if token["token_address"] not in collaped_tokens:
                collaped_tokens[token["token_address"]] = token
            else:
                collaped_tokens[token["token_address"]]["token_amount"] = str(
                    int(collaped_tokens[token["token_address"]]["token_amount"])
                    + int(token["token_amount"])
                )

    return collaped_tokens


class AccountGraphParams(BaseModel):
    theme: str


@router.get(
    "/plots/{net}/{account_id}/ccd_balance_usd_value", response_class=Response
)  # note that is is actually the index, not the id
@router.get(
    "/plots/{net}/ccd_balance_usd_value/{account_id}/image.png", response_class=Response
)  # note that is is actually the index, not the id
@router.post(
    "/ajax_account_graph/{net}/{account_id}",
    response_class=HTMLResponse,
)
async def request_account_graph(
    request: Request,
    net: str,
    account_id: str | int,
    post_params: AccountGraphParams | None = Body(default=None),
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    if post_params is None:
        post_params = AccountGraphParams(theme="dark")

    theme = post_params.theme
    account_index = from_address_to_index(account_id, net, request.app)
    if net == "testnet":
        return "Not available on testnet."

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/graph/CCD",
        httpx_client,
    )
    graph_data = api_result.return_value if api_result.ok else []
    df = pd.DataFrame(graph_data)
    title = "CCD Balance (and USD value) "

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if df.empty:
        fig.update_layout(
            title=f"<b>{title}</b><br><sup>{account_index}</sup>",
            template=ccdexplorer_plotly_template(theme),
            height=350,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text="No data available",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(size=14),
                )
            ],
        )
        return fig.to_html(
            config={"responsive": True, "displayModeBar": False},
            full_html=False,
            include_plotlyjs=False,
        )

    fig.add_trace(
        go.Scatter(
            x=df["date"].to_list(),
            y=df["ccd_balance"].to_list(),
            name="Balance",
            marker=dict(color="#AE7CF7"),
            hovertemplate="%{y:,.0f} CCD",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"].to_list(),
            y=df["ccd_balance_in_USD"].to_list(),
            name="Value (in USD)",
            marker=dict(color="#549FF2"),
            fill="tozeroy",
            hovertemplate="%{y:,.0f} USD",
        ),
        secondary_y=True,
        # showgrid=False,
    )
    fig.update_yaxes(
        secondary_y=False,
        title_text="CCD Balance",
        showgrid=False,
        title_font=dict(color="#AE7CF7"),
    )
    fig.update_yaxes(
        autorangeoptions={"minallowed": 0},
        secondary_y=True,
        title_text="CCD Value (in USD)",
        showgrid=False,
        title_font=dict(color="#549FF2"),
    )
    fig.update_xaxes(type="date")
    fig.update_layout(
        hovermode="x unified",
        showlegend=False,
        title=f"<b>{title}</b><br><sup>{account_index}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=350,
    )
    return return_plot_response(fig, request, title)
    # return fig.to_html(
    #     config={"responsive": True, "displayModeBar": False},
    #     full_html=False,
    #     include_plotlyjs=False,
    # )


@router.get("/{net}/account/{account_address}/sent_latest_first", response_class=HTMLResponse)
async def get_account_tx_sent_latest_first(
    request: Request,
    net: str,
    account_address: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    request.state.api_calls = {}
    request.state.api_calls["Sent Transactions"] = (
        f"{request.app.api_url}/docs#/Account/get_account_txs_sent_latest_first"
    )
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_address}/transactions/sent/latest_first",
        httpx_client,
    )
    latest_first: dict | None = api_result.return_value if api_result.ok else {}
    if latest_first:
        if len(latest_first) > 0:
            html = request.app.templates.get_template(
                "account/account_latest_first_sent.html"
            ).render(
                {
                    "latest_first": latest_first,
                    "net": net,
                    "account_address": account_address,
                    "request": request,
                }
            )

            return html
        else:
            return ""
    else:
        return ""
