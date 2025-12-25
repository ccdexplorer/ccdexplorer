# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false

import datetime as dt
import math

import dateutil
import grpc
import httpx
from ccdexplorer.ccdexplorer_api.app.routers.v2.contract_v2 import (
    GetBalanceOfRequest,
    get_balance_of,
    get_module_name_from_contract_address,
)
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_blocks_per_day,
    get_exchange_rates,
    get_exchange_rates_historical,
    get_grpcclient,
    get_httpx_client,
    get_mongo_db,
    get_mongo_motor,
)
from ccdexplorer.ccdexplorer_api.app.utils import (
    PLTToken,
    TokenHolding,
    category_to_types,
    await_await,
    apply_docstring_router_wrappers,
)
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import (
    MongoImpactedAddress,
    MongoTypeBlockPerDay,
    MongoTypeLoggedEventV2,
    MongoTypePaydayV2,
)
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME, TX_REQUEST_LIMIT_DISPLAY
from fastapi.security.api_key import APIKeyHeader

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountInfo,
    CCD_BlockItemSummary,
    CCD_ContractAddress,
    CCD_PoolInfo,
    CCD_Token,
)
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
    MongoMotor,
)
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from grpc._channel import _InactiveRpcError
from pymongo import ASCENDING, DESCENDING
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

router = APIRouter(tags=["Account"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)
# bump


async def convert_account_plt_value_to_USD(
    tokens: list[CCD_Token], db_to_use: dict[Collections, AsyncCollection], exchange_rates
):
    tokens_value_USD = 0
    for token in tokens:
        token_value = int(token.token_account_state.balance.value) * (
            math.pow(10, -token.token_account_state.balance.decimals)
        )

        token_tag_info: dict | None = await db_to_use[Collections.plts_tags].find_one(
            {"_id": token.token_id}
        )
        get_price_from = token_tag_info.get("get_price_from") if token_tag_info else None
        if get_price_from in exchange_rates:
            token_value_USD = token_value * exchange_rates[get_price_from]["rate"]
        else:
            token_value_USD = 0
        tokens_value_USD += token_value_USD
    return tokens_value_USD


async def convert_account_fungible_tokens_value_to_USD(
    tokens_dict: dict[str, TokenHolding], db_to_use: AsyncDatabase, exchange_rates
):
    tokens_tags = {
        x["contracts"][0]: x
        for x in await db_to_use[Collections.tokens_tags]
        .find({"token_type": "fungible"})
        .to_list(length=None)
    }

    tokens_with_metadata: dict[str, TokenHolding] = {}
    for contract, d in tokens_dict.items():
        if contract in tokens_tags.keys():
            # it's a single use contract
            d.decimals = tokens_tags[contract]["decimals"]
            if not d.decimals:
                d.decimals = 0
            d.token_symbol = tokens_tags[contract].get("get_price_from")
            d.token_value = int(d.token_amount) * (math.pow(10, -d.decimals))

            if d.token_symbol in exchange_rates:
                d.token_value_USD = d.token_value * exchange_rates[d.token_symbol]["rate"]

            else:
                d.token_value_USD = 0
            tokens_with_metadata[contract] = d

    tokens_value_USD = sum(
        [
            x.token_value_USD if x.token_value_USD is not None else 0
            for x in tokens_with_metadata.values()
        ]
    )
    return tokens_value_USD


@router.get(
    "/{net}/account/{account_address}/tokens-available/{alias}", response_class=JSONResponse
)
@router.get("/{net}/account/{account_address}/tokens-available", response_class=JSONResponse)
async def get_account_tokens_available(
    request: Request,
    net: str,
    account_address: str,
    alias: str | None = None,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """
    Endpoint to determine if a given account holds tokens, as stored in MongoDB collection `tokens_links_v3`.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if alias:
        account_address_to_use = account_address
        compare = "account_address"
    else:
        account_address_to_use = account_address[:29]
        compare = "account_address_canonical"

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    result_list = list(
        db_to_use[Collections.tokens_links_v3].find({compare: account_address_to_use}).limit(1)
    )
    tokens = [TokenHolding(**x["token_holding"]) for x in result_list]

    # PLT
    result_list = list(
        db_to_use[Collections.plts_links].find({compare: account_address_to_use}).limit(1)
    )

    return len(tokens) > 0 or len(result_list) > 0


@router.get("/{net}/account/{account_address}/rewards-available", response_class=JSONResponse)
async def get_account_rewards_available(
    request: Request,
    net: str,
    account_address: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """
    Endpoint to determine if a given account has ever received Account Rewards.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    pipeline = [
        {"$match": {"impacted_address_canonical": account_address[:29]}},
        {"$match": {"effect_type": "Account Reward"}},
        {"$limit": 1},
    ]

    result_list = list(db_to_use[Collections.impacted_addresses].aggregate(pipeline))

    if result_list:
        return True
    else:
        return False


@router.get("/{net}/account/{account_address}/plt/USD", response_class=JSONResponse)
async def get_account_plt_tokens_value_in_USD(
    request: Request,
    net: str,
    account_address: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> float:
    """
    Endpoint to get sum of all PLT tokens in USD for a given account, as stored in account_info on the node`.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    tokens = grpcclient.get_account_info(
        "last_final", hex_address=account_address, net=NET(net)
    ).tokens
    # use grpc balance_of method
    if len(tokens) > 0:
        tokens_value_USD = await convert_account_plt_value_to_USD(
            tokens,
            db_to_use,
            exchange_rates,
        )
        return tokens_value_USD
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account ({account_address}) has no tokens on {net}",
        )


@router.get("/{net}/account/{account_address}/fungible-tokens/USD", response_class=JSONResponse)
async def get_account_fungible_tokens_value_in_USD(
    request: Request,
    net: str,
    account_address: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> float:
    """
    Endpoint to get sum of all fungible tokens in USD for a given account, as stored in MongoDB collection `tokens_links_v3`.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    # first get all contracts for fungible tokens
    fungible_contracts = {
        x["contracts"][0]: x
        for x in await db_to_use[Collections.tokens_tags]
        .find({"token_type": "fungible"}, {"_id": 1, "contracts": 1})
        .to_list(length=None)
    }
    pipeline = [
        {"$match": {"token_holding.contract": {"$in": list(fungible_contracts.keys())}}},
        {"$match": {"account_address_canonical": account_address[:29]}},
    ]
    result_list = await await_await(db_to_use, Collections.tokens_links_v3, pipeline)

    tokens = [TokenHolding(**x["token_holding"]) for x in result_list]

    # use grpc balance_of method
    for token in tokens:
        result = await db_to_use[Collections.tokens_tags].find_one(
            {"related_token_address": token.token_address}
        )
        if result:
            if "module_name" not in result:
                module_name = await get_module_name_from_contract_address(
                    db_to_use, CCD_ContractAddress.from_str(token.contract)
                )

            else:
                module_name = result["module_name"]

            contract = result["contracts"][0]
            request = GetBalanceOfRequest(
                net=net,
                contract_address=CCD_ContractAddress.from_str(contract),
                token_id=(
                    ""
                    if result["related_token_address"].replace(contract, "") == "-"
                    else result["related_token_address"].replace(f"{contract}-", "")
                ),
                module_name=module_name,
                addresses=[account_address],
                grpcclient=grpcclient,
                motor=mongomotor,
            )  # type: ignore
            token_amount_from_state = await get_balance_of(request)  # type: ignore
            token.token_amount = token_amount_from_state.get(account_address, 0)

    if len(tokens) > 0:
        tokens_value_USD = await convert_account_fungible_tokens_value_to_USD(
            {x.contract: x for x in tokens},
            db_to_use,
            exchange_rates,
        )
        return tokens_value_USD
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account ({account_address}) has no tokens on {net}",
        )


@router.get(
    "/{net}/account/{account_address}/token-symbols-for-flow",
    response_class=JSONResponse,
)
async def get_account_token_symbols_for_flow(
    request: Request,
    net: str,
    account_address: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[str]:
    """
    Endpoint to get all fungible CIS-2 tokens for a given account, even if the current balance is zero.


    """
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    # first get all contracts for fungible tokens
    fungible_contracts = {
        x["contracts"][0]: x
        for x in await await_await(
            db_to_use,
            Collections.tokens_tags,
            [
                {"$match": {"token_type": "fungible"}},
                {"$project": {"_id": 1, "contracts": 1}},
            ],
            length=None,
        )
    }

    pipeline = [
        {"$match": {"effect_type": {"$ne": "data_registered"}}},
        {"$match": {"contract": {"$exists": True}}},
        {
            "$match": {"impacted_address_canonical": {"$eq": account_address[:29]}},
        },
        {"$match": {"contract": {"$in": list(fungible_contracts.keys())}}},
        {
            "$match": {"event_type": {"$exists": True}},
        },
        {
            "$group": {
                "_id": "$contract",
            }
        },
        {
            "$project": {
                "_id": 0,
                "contract": "$_id",
            }
        },
    ]
    contracts = await await_await(db_to_use, Collections.impacted_addresses, pipeline)

    if len(contracts) > 0:
        contracts_for_account = [x["contract"] for x in contracts]

        return sorted(
            [
                value["_id"]
                for key, value in fungible_contracts.items()
                if key in contracts_for_account
            ]
        )
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account ({account_address}) has no tokens on {net}",
        )


@router.get(
    "/{net}/account/{account_address}/plt-symbols-for-flow",
    response_class=JSONResponse,
)
async def get_account_plt_symbols_for_flow(
    request: Request,
    net: str,
    account_address: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[str]:
    """
    Endpoint to get all Protocol-Level tokens for a given account, even if the current balance is zero.


    """
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        {"$match": {"plt_token_id": {"$exists": True}}},
        {
            "$match": {"impacted_address_canonical": {"$eq": account_address[:29]}},
        },
        {
            "$group": {
                "_id": "$plt_token_id",
            }
        },
        {
            "$project": {
                "_id": 0,
                "plt_token_id": "$_id",
            }
        },
    ]
    plts = await await_await(db_to_use, Collections.impacted_addresses, pipeline)

    return sorted([x["plt_token_id"] for x in plts])


@router.get(
    "/{net}/account/{account_address}/plt/{skip}/{limit}/{alias}",
    response_class=JSONResponse,
)
@router.get(
    "/{net}/account/{account_address}/plt/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_account_plt_tokens(
    request: Request,
    net: str,
    account_address: str,
    skip: int,
    limit: int,
    alias: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get PLT tokens for a given account, as stored in MongoDB collection `plts_links`.
    """

    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    if not alias:
        pipeline = [{"$match": {"account_address_canonical": account_address[:29]}}]
    else:
        pipeline = [{"$match": {"account_address": account_address}}]
    pipeline += [
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
        {
            "$project": {
                "data": 1,
                "total": {"$arrayElemAt": ["$metadata.total", 0]},
            }
        },
    ]
    result = await await_await(db_to_use, Collections.plts_links, pipeline)
    all_tokens = [x for x in result[0]["data"]]
    if "total" in result[0]:
        total_token_count = result[0]["total"]
    else:
        total_token_count = 0

    tokens = [PLTToken(**x) for x in all_tokens]

    # add verified information and metadata and USD value
    for index, token in enumerate(tokens):
        result = await db_to_use[Collections.plts_tags].find_one({"_id": token.token_id})
        token.tag_information = result
        if token.tag_information is None:
            raise HTTPException(
                status_code=404,
                detail=f"Can't find {token.token_id} in PLT tokens on {net}",
            )

        token.token_symbol = token.tag_information.get("get_price_from")
        token.decimals = token.tag_information.get("decimals", 0)
        token.token_value = int(token.balance) * (math.pow(10, -token.decimals))  # type: ignore

        if token.token_symbol in exchange_rates:
            token.token_value_USD = token.token_value * exchange_rates[token.token_symbol]["rate"]
            token.exchange_rate = exchange_rates[token.token_symbol]["rate"]
        else:
            token.token_value_USD = 0
            token.exchange_rate = 0

    return {"tokens": tokens, "total_token_count": total_token_count}


@router.get(
    "/{net}/account/{account_address}/fungible-tokens/{skip}/{limit}/verified/{alias}",
    response_class=JSONResponse,
)
@router.get(
    "/{net}/account/{account_address}/fungible-tokens/{skip}/{limit}/verified",
    response_class=JSONResponse,
)
async def get_account_fungible_tokens_verified(
    request: Request,
    net: str,
    account_address: str,
    skip: int,
    limit: int,
    alias: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get verified fungible tokens for a given account, as stored in MongoDB collection `tokens_links_v3`.
    """

    # if net == "testnet":
    #     raise HTTPException(
    #         status_code=404,
    #         detail=f"Fungible verified tokens are not tracked on {net}",
    #     )

    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    fungible_token_result = (
        await db_to_use[Collections.tokens_tags]
        .find({"token_type": "fungible"}, {"related_token_address": 1})
        .to_list(length=None)
    )

    fungible_token_addresses = [
        x["related_token_address"] for x in fungible_token_result if "related_token_address" in x
    ]

    if not alias:
        pipeline = [{"$match": {"account_address_canonical": account_address[:29]}}]
    else:
        pipeline = [{"$match": {"account_address": account_address}}]
    pipeline += [
        {"$match": {"token_holding.token_address": {"$in": fungible_token_addresses}}},
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
        {
            "$project": {
                "data": 1,
                "total": {"$arrayElemAt": ["$metadata.total", 0]},
            }
        },
    ]
    result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline)
    all_tokens = [x for x in result[0]["data"]]
    if "total" in result[0]:
        total_token_count = result[0]["total"]
    else:
        total_token_count = 0
    tokens = [
        {"account_address": x["account_address"], "token": TokenHolding(**x["token_holding"])}
        for x in all_tokens
    ]

    # add verified information and metadata and USD value
    # for index, token in enumerate(tokens):
    for d in tokens:
        account_address_for_token = d["account_address"]
        token = d["token"]
        result = await db_to_use[Collections.tokens_tags].find_one(
            {"related_token_address": token.token_address}
        )
        token.verified_information = result
        if result is None or ("module_name" not in result if result else True):
            module_name = await get_module_name_from_contract_address(
                db_to_use, CCD_ContractAddress.from_str(token.contract)
            )

        else:
            module_name = result["module_name"]

        contract = result["contracts"][0]
        request = GetBalanceOfRequest(
            net=net,
            contract_address=CCD_ContractAddress.from_str(contract),
            token_id=(
                ""
                if result["related_token_address"].replace(contract, "") == "-"
                else result["related_token_address"].replace(f"{contract}-", "")
            ),
            module_name=module_name,
            addresses=[account_address_for_token],
            grpcclient=grpcclient,
            motor=mongomotor,
        )
        token_amount_from_state = await get_balance_of(request)
        token.token_amount = token_amount_from_state.get(account_address_for_token, 0)

        token.token_symbol = token.verified_information.get("get_price_from")
        token.decimals = token.verified_information.get("decimals", 0)
        token.decimals = token.decimals if token.decimals else 0
        token.token_value = int(token.token_amount) * (math.pow(10, -token.decimals))
        if token.token_symbol:
            if token.token_symbol in exchange_rates:
                token.token_value_USD = (
                    token.token_value * exchange_rates[token.token_symbol]["rate"]
                )
            else:
                token.token_value_USD = 0
        else:
            token.token_value_USD = 0

        result = await db_to_use[Collections.tokens_token_addresses_v2].find_one(
            {"_id": token.token_address}
        )
        token.address_information = result
        if token.token_symbol in exchange_rates:
            token.address_information["exchange_rate"] = exchange_rates[token.token_symbol]["rate"]

    return {"tokens": [x["token"] for x in tokens], "total_token_count": total_token_count}


@router.get(
    "/{net}/account/{account_address}/non-fungible-tokens/{skip}/{limit}/verified/{alias}",
    response_class=JSONResponse,
)
@router.get(
    "/{net}/account/{account_address}/non-fungible-tokens/{skip}/{limit}/verified",
    response_class=JSONResponse,
)
async def get_account_non_fungible_tokens_verified(
    request: Request,
    net: str,
    account_address: str,
    skip: int,
    limit: int,
    alias: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get verified non fungible tokens for a given account, as stored in MongoDB collection `tokens_links_v3`.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    non_fungible_token_contracts = [
        x["contracts"]
        for x in await db_to_use[Collections.tokens_tags]
        .find({"token_type": "non-fungible"}, {"contracts": 1})
        .to_list(length=None)
    ]
    non_fungible_token_contracts = [item for row in non_fungible_token_contracts for item in row]
    if not alias:
        pipeline = [{"$match": {"account_address_canonical": account_address[:29]}}]
    else:
        pipeline = [{"$match": {"account_address": account_address}}]
    pipeline += [
        {"$match": {"token_holding.contract": {"$in": non_fungible_token_contracts}}},
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
        {
            "$project": {
                "data": 1,
                "total": {"$arrayElemAt": ["$metadata.total", 0]},
            }
        },
    ]
    result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline)
    all_tokens = [x for x in result[0]["data"]]
    if "total" in result[0]:
        total_token_count = result[0]["total"]
    else:
        total_token_count = 0
    tokens = [
        {"account_address": x["account_address"], "token": TokenHolding(**x["token_holding"])}
        for x in all_tokens
    ]

    # add verified information and metadata
    for d in tokens:
        account_address_for_token = d["account_address"]
        token = d["token"]
        token.account_address_for_token = account_address_for_token
        result = await db_to_use[Collections.tokens_tags].find_one(
            {"contracts": {"$in": [token.contract]}}
        )
        token.verified_information = result
        if result is None or "module_name" not in result:
            module_name = await get_module_name_from_contract_address(
                db_to_use, CCD_ContractAddress.from_str(token.contract)
            )

        else:
            module_name = result["module_name"]

        request = GetBalanceOfRequest(
            net=net,
            contract_address=CCD_ContractAddress.from_str(token.contract),
            token_id=token.token_id,
            module_name=module_name,
            addresses=[account_address_for_token],
            grpcclient=grpcclient,
            motor=mongomotor,
        )
        token_amount_from_state = await get_balance_of(request)
        token.token_amount = token_amount_from_state.get(
            account_address_for_token, token.token_amount
        )
        result = await db_to_use[Collections.tokens_token_addresses_v2].find_one(
            {"_id": token.token_address}
        )
        token.address_information = result

    return {"tokens": [x["token"] for x in tokens], "total_token_count": total_token_count}


@router.get(
    "/{net}/account/{account_address}/tokens/{skip}/{limit}/unverified/{alias}",
    response_class=JSONResponse,
)
@router.get(
    "/{net}/account/{account_address}/tokens/{skip}/{limit}/unverified",
    response_class=JSONResponse,
)
async def get_account_tokens_unverified(
    request: Request,
    net: str,
    account_address: str,
    skip: int,
    limit: int,
    alias: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get unverified tokens for a given account, as stored in MongoDB collection `tokens_links_v3`.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    verified_token_contracts = [
        x["contracts"]
        for x in await db_to_use[Collections.tokens_tags]
        .find({}, {"contracts": 1})
        .to_list(length=None)
    ]
    verified_token_contracts = [item for row in verified_token_contracts for item in row]
    if not alias:
        pipeline = [{"$match": {"account_address_canonical": account_address[:29]}}]
    else:
        pipeline = [{"$match": {"account_address": account_address}}]
    pipeline += [
        {"$match": {"token_holding.contract": {"$nin": verified_token_contracts}}},
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
        {
            "$project": {
                "data": 1,
                "total": {"$arrayElemAt": ["$metadata.total", 0]},
            }
        },
    ]
    result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline)
    all_tokens = [x for x in result[0]["data"]]
    if "total" in result[0]:
        total_token_count = result[0]["total"]
    else:
        total_token_count = 0
    tokens = [
        {"account_address": x["account_address"], "token": TokenHolding(**x["token_holding"])}
        for x in all_tokens
    ]

    # add metadata
    for d in tokens:
        account_address_for_token = d["account_address"]
        token = d["token"]
        result = await db_to_use[Collections.tokens_token_addresses_v2].find_one(
            {"_id": token.token_address}
        )
        token.address_information = result

        module_name = await get_module_name_from_contract_address(
            db_to_use, CCD_ContractAddress.from_str(token.contract)
        )

        request = GetBalanceOfRequest(
            net=net,
            contract_address=CCD_ContractAddress.from_str(token.contract),
            token_id=token.token_id,
            module_name=module_name,
            addresses=[account_address_for_token],
            grpcclient=grpcclient,
            motor=mongomotor,
        )
        token_amount_from_state = await get_balance_of(request)
        if token_amount_from_state != []:
            token.token_amount = token_amount_from_state.get(account_address_for_token, 0)
    return {"tokens": [x["token"] for x in tokens], "total_token_count": total_token_count}


@router.get(
    "/{net}/account/{account_address}/balance/block/{block}",
    response_class=JSONResponse,
)
async def get_account_balance_at_block(
    request: Request,
    net: str,
    account_address: str,
    block: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """
    Endpoint to get all CCD balance in microCCD for a given account at the given block.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_account_info(block, account_address, net=NET(net))
    except grpc._channel._InactiveRpcError:
        result = None

    if result:
        return result.amount
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account {account_address} or block {block:,.0f} not found on {net}",
        )


@router.get("/{net}/account/{account_address}/balance/USD", response_class=JSONResponse)
async def get_account_balance_in_USD(
    request: Request,
    net: str,
    account_address: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> float:
    """
    Endpoint to get all CCD balance in microCCD converted to USD for a given account at the last final block.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_account_info("last_final", account_address, net=NET(net))
    except grpc._channel._InactiveRpcError:
        result = None

    if result:
        return (result.amount / 1_000_000) * exchange_rates["CCD"]["rate"]
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account {account_address} not found on {net}",
        )


@router.get("/{net}/account/{account_address}/balance", response_class=JSONResponse)
async def get_account_balance(
    request: Request,
    net: str,
    account_address: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """
    Endpoint to get all CCD balance in microCCD for a given account at the last final block.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_account_info("last_final", account_address, net=NET(net))
    except grpc._channel._InactiveRpcError:
        result = None

    if result:
        return result.amount
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account {account_address} not found on {net}",
        )


@router.get("/{net}/account/{index_hash}/is-alias", response_class=JSONResponse)
async def account_is_alias(
    request: Request,
    net: str,
    index_hash: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    result = await db_to_use[Collections.all_account_addresses].find_one({"_id": index_hash[:29]})
    if not result:
        return False

    account_address = result["account_address"]
    return index_hash != account_address


@router.get("/{net}/account/{index_or_hash}/info", response_class=JSONResponse)
async def get_account_info(
    request: Request,
    net: str,
    index_or_hash: int | str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_AccountInfo:
    """
    Endpoint to get all account info for a given account at the last final block.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        # if this doesn't fail, it's type int.
        index_or_hash = int(index_or_hash)
    except ValueError:
        pass
    try:
        if isinstance(index_or_hash, int):
            try:
                result = grpcclient.get_account_info(
                    "last_final", account_index=index_or_hash, net=NET(net)
                )
            except grpc._channel._InactiveRpcError:
                result = None
        else:
            if len(index_or_hash) == 29:
                try:
                    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
                    result = await db_to_use[Collections.all_account_addresses].find_one(
                        {"_id": index_or_hash}
                    )
                    if result:
                        index_or_hash = result["account_address"]
                except Exception:
                    index_or_hash = ""

            try:
                result = grpcclient.get_account_info(
                    "last_final", hex_address=index_or_hash, net=NET(net)
                )
            except grpc._channel._InactiveRpcError:
                result = None
    except:  # noqa: E722
        raise HTTPException(
            status_code=404,
            detail=f"Requested account {index_or_hash} not found on {net}",
        )

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested account {index_or_hash} not found on {net}",
        )


@router.get("/{net}/account/{index}/earliest-win-time", response_class=JSONResponse)
async def get_validator_earliest_win_time(
    request: Request,
    net: str,
    index: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dt.datetime:
    """
    Endpoint to get earliest win time for an account that is an active validator.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_baker_earliest_win_time(baker_id=index, net=NET(net))
    except grpc._channel._InactiveRpcError:
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't get earliest win time for account {index} on {net}",
        )


def expectation(value, blocks_validated):
    if round(value, 0) == 1:
        plural = ""
    else:
        plural = "s"
    if value < 5:
        expectation_string = f"{blocks_validated:,.0f} / {value:,.2f} block{plural}"
    else:
        expectation_string = f"{blocks_validated:,.0f} / {value:,.0f} block{plural}"
    return expectation_string


@router.get("/{net}/account/{index}/current-payday-stats", response_class=JSONResponse)
async def get_validator_current_payday_stats(
    request: Request,
    net: str,
    index: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> str | None:
    """
    Endpoint to get current payday stats for an account that is an active validator.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    pipeline = [{"$sort": {"date": -1}}, {"$limit": 1}]
    mongo_result = await await_await(mongomotor.mainnet, Collections.paydays_v2, pipeline)
    if mongo_result:
        mongo_result = MongoTypePaydayV2(**mongo_result[0])
        if mongo_result:
            if mongo_result.height_for_last_block:
                paydays_last_blocks_validated = (
                    mongo_result.height_for_last_block - mongo_result.height_for_first_block + 1  # type: ignore
                )
            else:
                return "Payday calculation in progress...please wait."
        try:
            pool = grpcclient.get_pool_info_for_pool(index, "last_final", net=NET(net))
            if pool.current_payday_info:
                stats = expectation(
                    pool.current_payday_info.lottery_power * paydays_last_blocks_validated,
                    pool.current_payday_info.blocks_baked,
                )
            else:
                stats = "Not in current payday"
        except _InactiveRpcError:
            raise HTTPException(
                status_code=404,
                detail=f"Can't get earliest win time for account {index} on {net}",
            )

        if stats:
            return stats
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't get earliest win time for account {index} on {net}",
        )


@router.get("/{net}/account/{index}/pool-info", response_class=JSONResponse)
async def get_validator_pool_info(
    request: Request,
    net: str,
    index: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_PoolInfo:
    """
    Endpoint to get the current pool info for an account that is an active validator.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_pool_info_for_pool(
            pool_id=index, block_hash="last_final", net=NET(net)
        )
    except grpc._channel._InactiveRpcError:
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't get pool info for account {index} on {net}",
        )


@router.get("/{net}/account/{account_id}/staking-rewards-bucketed", response_class=JSONResponse)
async def get_staking_rewards_bucketed(
    request: Request,
    net: str,
    account_id: int | str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """
    Endpoint to get staking rewards info for a given account for graphing.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    pp = [
        {"$match": {"account_id": account_id}},
    ]
    try:
        result_pp = await await_await(db_to_use, Collections.paydays_v2_rewards, pp)
        return result_pp
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve staking rewards for account at {account_id} on {net} with error {error}.",
        )


@router.get(
    "/{net}/account/{account_id}/staking-rewards/{year_month}",
    response_class=JSONResponse,
)
async def get_staking_rewards_for_year_month(
    request: Request,
    net: str,
    account_id: int | str,
    year_month: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """
    Endpoint to get staking rewards info for a given account for rewards downloading.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    # from year_month make the first of the month and the first of the next month
    year_month = f"{year_month}-01"
    year_month_next = (
        dt.datetime.strptime(year_month, "%Y-%m-%d") + relativedelta(months=1)
    ).strftime("%Y-%m-%d")
    db_to_use = mongomotor.mainnet
    pp = [
        {
            "$match": {
                "account_id": account_id,
                "date": {
                    "$gte": year_month,
                    "$lt": year_month_next,
                },  # Range-based filtering instead of regex
            }
        },
        {"$project": {"_id": 0, "reward": 1, "date": 1}},
        {"$sort": {"date": 1}},
    ]

    try:
        result_pp = await await_await(db_to_use, Collections.paydays_v2_rewards, pp)

        dates_in_month = [x["date"] for x in result_pp]
        pp = [
            {"$match": {"date": {"$in": dates_in_month}}},
        ]
        paydays_result = await await_await(db_to_use, Collections.paydays, pp)

        paydays_dict = {x["date"]: x["_id"] for x in paydays_result}

        for item in result_pp:
            item["payday_block"] = paydays_dict.get(item["date"], None)

        return result_pp
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve staking rewards for account at {account_id} on {net} with error {error}.",
        )


@router.get("/{net}/account/{index}/validator-performance", response_class=JSONResponse)
async def get_validator_performance(
    request: Request,
    net: str,
    index: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """
    Endpoint to get validator performance for a given validator.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    try:
        result = (
            await db_to_use[Collections.paydays_v2_performance]
            .find({"baker_id": index})
            .sort("date", ASCENDING)
            .to_list(length=None)
        )
        return result
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve validator performance for validator {index} on {net} with error {error}.",
        )


@router.get("/{net}/account/{index}/validator-inactive", response_class=JSONResponse)
async def get_validator_inactive(
    request: Request,
    net: str,
    index: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get validator inactive actions for primed for and actual
    suspensions in the last 90 days.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    try:
        ninety_days_ago = dt.datetime.now().astimezone(dt.UTC) - dt.timedelta(days=90)
        pipeline = [
            {"$match": {"baker_id": index}},
            {"$match": {"date": {"$gte": ninety_days_ago.strftime("%Y-%m-%d")}}},
            {"$group": {"_id": "$action", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}},
        ]
        result = await await_await(db_to_use, Collections.validator_logs, pipeline)
        return {item["_id"]: item["count"] for item in result}
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve validator inactive actions for validator {index} on {net} with error {error}.",
        )


@router.get("/{net}/account/{account_id}/rewards/{skip}/{limit}", response_class=JSONResponse)
async def get_paginated_account_rewards(
    request: Request,
    net: str,
    account_id: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get paginated account rewards.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    pp = [
        {"$match": {"account_id": account_id}},
        {"$sort": {"date": DESCENDING}},
        {"$skip": skip},
        {"$limit": limit},
    ]
    total_rows = await db_to_use[Collections.paydays_v2_rewards].count_documents(
        {"account_id": account_id}
    )
    try:
        result_pp = await await_await(db_to_use, Collections.paydays_v2_rewards, pp)
        return {"data": result_pp, "total_rows": total_rows}
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve staking rewards for account at {account_id} on {net} with error {error}.",
        )


@router.get("/{net}/account/{account_id}/rewards-available", response_class=JSONResponse)
async def get_bool_account_rewards_available(
    request: Request,
    net: str,
    account_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """
    Endpoint to get determine if payday rewards are available for an account.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    try:
        result = await db_to_use[Collections.paydays_v2_rewards].find_one(
            {"account_id": account_id}
        )

        return result is not None

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't determine whether account {account_id} on {net}  has rewards with error {error}.",
        )


@router.get("/{net}/account/{index}/validator-tally/{skip}/{limit}", response_class=JSONResponse)
async def get_validator_tally(
    request: Request,
    net: str,
    index: int,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict | None:
    """
    Endpoint to get validator tally data.


    """

    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    # limit = limit if limit <= 50 else 50
    db_to_use = mongomotor.mainnet
    try:
        account_info = grpcclient.get_account_info(
            block_hash="last_final", account_index=index, net=NET(net)
        )

        if account_info.stake:
            if account_info.stake.baker:
                validator_id = account_info.stake.baker.baker_info.baker_id
            else:
                validator_id = None
        else:
            validator_id = None

        data = None
        if validator_id is not None:
            pipeline = [
                {
                    "$match": {"baker_id": str(validator_id)},
                },
                {"$sort": {"date": DESCENDING}},
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                        "data": [{"$skip": skip}, {"$limit": limit}],
                    }
                },
                {
                    "$project": {
                        "data": 1,
                        "total": {"$arrayElemAt": ["$metadata.total", 0]},
                    }
                },
            ]
            result = await await_await(db_to_use, Collections.paydays_v2_performance, pipeline)
            data = [
                {
                    "date": v["date"],
                    "actuals": v["pool_status"]["current_payday_info"]["blocks_baked"],
                    "lp": v["pool_status"]["current_payday_info"]["lottery_power"],
                    "expectation": v["expectation"],
                }
                for v in result[0]["data"]
            ]
            return {"data": data, "total_row_count": result[0]["total"]}
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve validator tally with error {error}.",
        )


@router.get("/{net}/account/{index}/pool/delegators/{skip}/{limit}", response_class=JSONResponse)
async def get_account_pool_delegators(
    request: Request,
    net: str,
    index: int,
    skip: int,
    limit: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict | None:
    """
    Endpoint to get all delegators to pool.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    try:
        account_info = grpcclient.get_account_info(
            block_hash="last_final", account_index=index, net=NET(net)
        )
        validator = account_info.stake.baker
        if validator:
            try:
                delegators_current_payday = [
                    x
                    for x in grpcclient.get_delegators_for_pool_in_reward_period(
                        pool_id=validator.baker_info.baker_id,
                        block_hash="last_final",
                        net=NET(net),
                    )
                ]
            except:  # noqa: E722
                delegators_current_payday = []

            try:
                delegators_in_block = [
                    x
                    for x in grpcclient.get_delegators_for_pool(
                        pool_id=validator.baker_info.baker_id,
                        block_hash="last_final",
                        net=NET(net),
                    )
                ]
            except:  # noqa: E722
                delegators_in_block = []

            delegators_current_payday_list = set([x.account for x in delegators_current_payday])
            delegators_in_block_list = set([x.account for x in delegators_in_block])

            new_delegators = delegators_in_block_list - delegators_current_payday_list

            delegators = sorted(delegators_current_payday, key=lambda x: x.stake, reverse=True)
            return {
                "delegators": delegators[skip : (skip + limit)],
                "delegators_in_block": delegators_in_block,
                "delegators_current_payday": delegators_current_payday,
                "new_delegators": new_delegators,
                "total_delegators": len(delegators_current_payday),
            }
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve delegators with error {error}.",
        )


@router.get("/{net}/account/{index_or_hash}/apy-data/{type}", response_class=JSONResponse)
async def get_account_apy_data_v2(
    request: Request,
    net: str,
    index_or_hash: str,
    type: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """
    Endpoint to get account APY data.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        if type in ["validator", "delegator", "passive_delegation"]:
            search_for = "validator_id"
        else:
            search_for = "account_id"
        pipeline = [
            {"$match": {search_for: str(index_or_hash)}},
            {"$match": {"type": type}},
            {"$match": {"days_in_average": {"$in": [30, 90, 180]}}},
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            "$count_of_days",
                            {"$multiply": ["$days_in_average", 0.9]},
                        ]
                    }
                }
            },
            {"$sort": {"date": -1}},
        ]
        result = await await_await(mongomotor.mainnet, Collections.paydays_v2_apy, pipeline)
        # Group results by days_in_average, values are lists of items
        # Build a lookup: {(date, days): apy}
        lookup = {(item["date"], item["days_in_average"]): item.get("apy") for item in result}

        # 2. Collect all distinct dates and sort them
        dates = sorted({item["date"] for item in result}, reverse=True)

        # 3. Build the list of dicts
        records = []
        for d in dates:
            records.append(
                {
                    "date": d,
                    "30d": lookup.get((d, 30)),
                    "90d": lookup.get((d, 90)),
                    "180d": lookup.get((d, 180)),
                }
            )
        return records

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve account APY data with error {error}.",
        )


@router.get("/{net}/account/{index}/node", response_class=JSONResponse)
async def get_account_validator_node(
    request: Request,
    net: str,
    index: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get account validator node.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet

    result = await db_to_use[Collections.dashboard_nodes].find_one({"consensusBakerId": str(index)})

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't find node for validator {index} on {net}.",
        )


@router.get(
    "/{net}/account/{index_or_hash}/staking-rewards-object/{request_type}",
    response_class=JSONResponse,
)
async def get_staking_rewards_object(
    request: Request,
    net: str,
    index_or_hash: int | str,
    request_type: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get latest APY info.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    if request_type in ["validator", "delegator", "passive_delegation"]:
        search_for = "validator_id"
    else:
        search_for = "account_id"
    pipeline = [
        {"$match": {search_for: str(index_or_hash)}},
        {"$match": {"type": request_type}},
        {"$sort": {"date": -1}},
        {"$limit": 4},
    ]
    apy_object = await await_await(db_to_use, Collections.paydays_v2_apy, pipeline)

    return_dict = {f"d{x['days_in_average']}": x for x in apy_object}
    return return_dict


@router.get(
    "/{net}/account/{account_id}/transactions-count-if-below-display-limit",
    response_class=JSONResponse,
)
async def get_indicator_for_account_tx_count(
    request: Request,
    net: str,
    account_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get count and indicator whether account transactions count is above display limit.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    top_list_member = await db_to_use[Collections.impacted_addresses_all_top_list].find_one(
        {"_id": account_id[:29]}
    )

    if not top_list_member:
        pipeline = [
            {"$match": {"impacted_address_canonical": account_id[:29]}},
            {  # this filters out account rewards, as they are special events
                "$match": {"tx_hash": {"$exists": True}},
            },
            {"$limit": TX_REQUEST_LIMIT_DISPLAY + 1},
            {"$group": {"_id": None, "count": {"$sum": 1}}},
        ]
        result = await await_await(db_to_use, Collections.impacted_addresses, pipeline)
        sequence_number = result[0]["count"]
    else:
        # Either an account with many txs or a contract
        sequence_number = top_list_member["count"]
    return {
        "tx_count": sequence_number,
        "count_above_display_limit": sequence_number > TX_REQUEST_LIMIT_DISPLAY,
    }


@router.get(
    "/{net}/account/{account_id}/transactions/sent/latest_first",
    response_class=JSONResponse,
)
async def get_account_txs_sent_latest_first(
    request: Request,
    net: str,
    account_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the last and first sent transaction for an account.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    first_tx = await await_await(
        db_to_use,
        Collections.impacted_addresses,
        [
            {
                "$match": {
                    "impacted_address_canonical": account_id[:29],
                    "balance_movement.transaction_fee": {"$exists": True},
                }
            },
            {"$sort": {"block_height": 1}},
            {"$limit": 1},
        ],
        1,
    )

    last_tx = await await_await(
        db_to_use,
        Collections.impacted_addresses,
        [
            {
                "$match": {
                    "impacted_address_canonical": account_id[:29],
                    "balance_movement.transaction_fee": {"$exists": True},
                }
            },
            {"$sort": {"block_height": -1}},
            {"$limit": 1},
        ],
        1,
    )

    if len(first_tx) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No transactions found for account {account_id} on {net}.",
        )

    first_tx = await db_to_use[Collections.transactions].find_one({"_id": first_tx[0]["tx_hash"]})
    last_tx = await db_to_use[Collections.transactions].find_one({"_id": last_tx[0]["tx_hash"]})
    return {
        "first_tx": CCD_BlockItemSummary(**first_tx).model_dump(exclude_none=True),  # type: ignore
        "last_tx": CCD_BlockItemSummary(**last_tx).model_dump(exclude_none=True),  # type: ignore
    }


@router.get(
    "/{net}/account/{account_id}/transactions/{skip}/{limit}/{sort_key}/{direction}/{filter_type}",
    response_class=JSONResponse,
)
async def get_account_txs_with_filter(
    request: Request,
    net: str,
    account_id: str,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    filter_type: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all account transactions with filtering. Endpoint is also used for contracts (
    as their impacts are stored in the same collection).
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    filter_list = []
    if filter_type != "all":
        if filter_type == "alias":
            filter_list = ["alias"]
        else:
            filter_list = category_to_types.get(filter_type, [])
    # if account sequence number > TX_REQUEST_LIMIT_DISPLAY, we don't get totals
    top_list_member = await db_to_use[Collections.impacted_addresses_all_top_list].find_one(
        {"_id": account_id[:29]}
    )

    # common filter
    base_filter = {
        "impacted_address_canonical": account_id[:29],
        "tx_hash": {"$exists": True},
    }
    if filter_list:
        if filter_type == "smart_contract":
            base_filter["$or"] = [
                {"effect_type": {"$in": filter_list}},
                {"event_type": {"$exists": True}},
            ]
        elif filter_type == "alias":
            print("ALIAS")
            base_filter = {
                "impacted_address": account_id,
                "tx_hash": {"$exists": True},
            }
        else:
            base_filter["effect_type"] = {"$in": filter_list}

    # count unique hashes
    if not top_list_member or (filter_list == ["alias"]):
        count_pipeline = [
            {"$match": base_filter},
            {"$group": {"_id": "$tx_hash"}},
            {"$count": "total"},
        ]

        count_result = await await_await(
            db_to_use, Collections.impacted_addresses, count_pipeline, 1
        )

        total_tx_count = count_result[0]["total"] if count_result else 0

    else:
        if not filter_list:
            total_tx_count = top_list_member["count"]
        else:
            total_tx_count = sum(
                [top_list_member["effects"].get(effect_type, 0) for effect_type in filter_list]
            )

    # 2) fetch page
    sort_field = sort_key or "tx_hash"
    sort_direction = 1 if direction == "asc" else -1

    pipeline = [
        {"$match": base_filter},
        {"$sort": {sort_field: sort_direction}},
        {"$project": {"_id": 0, "tx_hash": 1, sort_field: 1}},
        {"$skip": skip},
        {"$limit": limit * 3},
    ]

    all_txs_hashes = await await_await(
        db_to_use,
        Collections.impacted_addresses,
        pipeline,
        limit * 3,
        allowDiskUse=True,
        maxTimeMS=10_000,  # abort if > 10 s
    )
    all_txs_hashes = list(set([x["tx_hash"] for x in all_txs_hashes]))
    int_result = (
        await db_to_use[Collections.transactions]
        .find({"_id": {"$in": all_txs_hashes}})
        .sort("block_info.height", ASCENDING if direction == "asc" else DESCENDING)
        .to_list(limit)
    )
    tx_result = [CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in int_result]
    return {
        "transactions": tx_result,
        "total_tx_count": total_tx_count,
        "tx_request_limit_display": TX_REQUEST_LIMIT_DISPLAY,
    }


@router.get(
    "/{net}/account/{account_id}/transactions/{skip}/{limit}/{sort_key}/{direction}",
    response_class=JSONResponse,
)
async def get_account_txs(
    request: Request,
    net: str,
    account_id: str,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all account transactions. Endpoint is also used for contracts (
    as their impacts are stored in the same collection).
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    # if account sequence number > TX_REQUEST_LIMIT_DISPLAY, we don't get totals
    top_list_member = await db_to_use[Collections.impacted_addresses_all_top_list].find_one(
        {"_id": account_id[:29]}
    )

    if not top_list_member:
        pipeline = [
            {
                "$match": {"impacted_address_canonical": {"$eq": account_id[:29]}},
            },
            {  # this filters out account rewards, as they are special events
                "$match": {"tx_hash": {"$exists": True}},
            },
        ]
        if sort_key:
            pipeline.append({"$sort": {sort_key: 1 if direction == "asc" else -1}})
        pipeline.extend(
            [
                {"$project": {"_id": 0, "tx_hash": 1}},
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                        "data": [{"$skip": skip}, {"$limit": limit * 3}],
                    }
                },
                {
                    "$project": {
                        "data": 1,
                        "total": {"$arrayElemAt": ["$metadata.total", 0]},
                    }
                },
            ]
        )
        result = await await_await(db_to_use, Collections.impacted_addresses, pipeline, limit * 3)
        all_txs_hashes = list(set([x["tx_hash"] for x in result[0]["data"]]))
        if "total" in result[0]:
            total_tx_count = result[0]["total"]
        else:
            total_tx_count = 0

    else:
        #### Either an account with many txs or a contract
        pipeline = [
            {
                "$match": {"impacted_address_canonical": {"$eq": account_id[:29]}},
            },
            {
                "$match": {"tx_hash": {"$exists": True}},
            },
        ]
        if sort_key:
            pipeline.append({"$sort": {sort_key: 1 if direction == "asc" else -1}})
        pipeline.extend(
            [
                {"$skip": skip},
                {"$limit": limit * 3},
                {"$project": {"tx_hash": 1}},
            ]
        )
        result = await await_await(db_to_use, Collections.impacted_addresses, pipeline, limit * 3)
        all_txs_hashes = [x["tx_hash"] for x in result]
        total_tx_count = top_list_member["count"]

    int_result = (
        await db_to_use[Collections.transactions]
        .find({"_id": {"$in": all_txs_hashes}})
        .sort("block_info.height", ASCENDING if direction == "asc" else DESCENDING)
        .to_list(limit)
    )
    tx_result = [CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in int_result]
    return {
        "transactions": tx_result,
        # "total_tx_count": total_tx_count if sequence_number < 1000 else sequence_number,
        "total_tx_count": total_tx_count,
        # "count_type": count_type,
        "tx_request_limit_display": TX_REQUEST_LIMIT_DISPLAY,
    }


@router.get(
    "/{net}/account/{account_id}/validator-transactions/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_account_validator_txs(
    request: Request,
    net: str,
    account_id: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all account validator transactions.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if skip < 0:
        raise HTTPException(
            status_code=400,
            detail="Don't be silly. Skip must be greater than or equal to zero.",
        )

    if limit > request.app.REQUEST_LIMIT:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 100.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    try:
        pipeline = [
            {
                "$match": {"impacted_address_canonical": {"$eq": account_id[:29]}},
            },
            {  # this filters out account rewards, as they are special events
                "$match": {
                    "$or": [
                        {"effect_type": "baker_added"},
                        {"effect_type": "baker_removed"},
                        {"effect_type": "baker_stake_updated"},
                        {"effect_type": "baker_restake_earnings_updated"},
                        # {"effect_type": "baker_keys_updated"},
                        {"effect_type": "baker_configured"},
                    ]
                },
            },
            {"$sort": {"block_height": DESCENDING}},
            {"$project": {"_id": 0, "tx_hash": 1}},
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "data": [{"$skip": skip}, {"$limit": limit}],
                }
            },
            {
                "$project": {
                    "data": 1,
                    "total": {"$arrayElemAt": ["$metadata.total", 0]},
                }
            },
        ]
        result = await await_await(db_to_use, Collections.impacted_addresses, pipeline, limit)
        all_txs_hashes = [x["tx_hash"] for x in result[0]["data"]]
        if "total" in result[0]:
            total_tx_count = result[0]["total"]
        else:
            total_tx_count = 0

        int_result = (
            await db_to_use[Collections.transactions]
            .find({"_id": {"$in": all_txs_hashes}})
            .sort("block_info.height", DESCENDING)
            .to_list(limit)
        )
        tx_result = [CCD_BlockItemSummary(**x) for x in int_result]
        return {"transactions": tx_result, "total_tx_count": total_tx_count}
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve validator transactions for account at {account_id} on {net} with error {error}.",
        )


@router.get(
    "/{net}/account/{account_id}/transactions-for-flow/{gte}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_account_transactions_for_flow_graph(
    request: Request,
    net: str,
    account_id: str,
    gte: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    blocks_per_day: dict[str, MongoTypeBlockPerDay] = Depends(get_blocks_per_day),
    api_key: str = Security(API_KEY_HEADER),
) -> list[MongoImpactedAddress]:
    """
    Endpoint to get all txs for a given account that should be included in the flow graph for CCD.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    amended_start_date = f"{(dateutil.parser.parse(start_date) - dt.timedelta(days=1)):%Y-%m-%d}"
    start_block = blocks_per_day.get(amended_start_date)
    if start_block:
        start_block = start_block.height_for_first_block
    else:
        start_block = 0

    end_block = blocks_per_day.get(end_date)
    if end_block:
        end_block = end_block.height_for_last_block
    else:
        end_block = 1_000_000_000

    try:
        gte = int(gte.replace(",", "").replace(".", ""))
    except:  # noqa: E722
        error = True

    db_to_use = mongomotor.mainnet
    try:
        pipeline = [
            {
                "$match": {"included_in_flow": True},
            },
            {"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}},
            {
                "$match": {"impacted_address_canonical": {"$eq": account_id[:29]}},
            },
        ]
        txs_for_account = await await_await(
            db_to_use, Collections.impacted_addresses, pipeline, length=None
        )
        return txs_for_account

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't determine whether account {account_id} on {net}  has rewards with error {error}.",
        )


@router.get(
    "/{net}/account/{account_id}/plt-transactions-for-flow/{token_id}/{gte}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_account_plt_transactions_for_flow_graph(
    request: Request,
    net: str,
    account_id: str,
    token_id: str,
    gte: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    blocks_per_day: dict[str, MongoTypeBlockPerDay] = Depends(get_blocks_per_day),
    api_key: str = Security(API_KEY_HEADER),
) -> list[MongoImpactedAddress]:
    """
    Endpoint to get all plt txs for a given account that should be included in the flow graph for PLT.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    amended_start_date = f"{(dateutil.parser.parse(start_date) - dt.timedelta(days=1)):%Y-%m-%d}"
    start_block = blocks_per_day.get(amended_start_date)
    if start_block:
        start_block = start_block.height_for_first_block
    else:
        start_block = 0

    end_block = blocks_per_day.get(end_date)
    if end_block:
        end_block = end_block.height_for_last_block
    else:
        end_block = 1_000_000_000

    try:
        gte = int(gte.replace(",", "").replace(".", ""))
    except:  # noqa: E722
        error = True

    db_to_use = mongomotor.mainnet if net == "mainnet" else mongomotor.testnet
    try:
        pipeline = [
            {
                "$match": {"plt_token_id": token_id},
            },
            {
                "$match": {"included_in_flow": True},
            },
            {"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}},
            {
                "$match": {"impacted_address_canonical": {"$eq": account_id[:29]}},
            },
        ]
        txs_for_account = await await_await(db_to_use, Collections.impacted_addresses, pipeline)
        return txs_for_account

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't find PLT txs for account {account_id} on {net} with error {error}.",
        )


@router.get(
    "/{net}/account/{account_id}/token-transactions-for-flow/{token_id}/{gte}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_account_token_transactions_for_flow_graph(
    request: Request,
    net: str,
    account_id: str,
    token_id: str,
    gte: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    blocks_per_day: dict[str, MongoTypeBlockPerDay] = Depends(get_blocks_per_day),
    api_key: str = Security(API_KEY_HEADER),
) -> list[MongoTypeLoggedEventV2]:
    """
    Endpoint to get all token txs for a given account that should be included in the flow graph for a token.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    amended_start_date = f"{(dateutil.parser.parse(start_date) - dt.timedelta(days=1)):%Y-%m-%d}"
    start_block = blocks_per_day.get(amended_start_date)
    if start_block:
        start_block = start_block.height_for_first_block
    else:
        start_block = 0

    end_block = blocks_per_day.get(end_date)
    if end_block:
        end_block = end_block.height_for_last_block
    else:
        end_block = 1_000_000_000

    try:
        gte = int(gte.replace(",", "").replace(".", ""))
    except:  # noqa: E722
        error = True

    db_to_use = mongomotor.mainnet
    try:
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"to_address_canonical": account_id[:29]},
                        {"from_address_canonical": account_id[:29]},
                    ]
                }
            },
            {"$match": {"tx_info.block_height": {"$gt": start_block, "$lte": end_block}}},
            {"$match": {"event_info.token_address": token_id}},
        ]
        txs_for_account = [
            MongoTypeLoggedEventV2(**x)
            for x in await await_await(db_to_use, Collections.tokens_logged_events_v2, pipeline)
        ]
        return txs_for_account

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't determine whether account {account_id} on {net}  has token txs with error {error}.",
        )


@router.get(
    "/{net}/account/{account_id}/rewards-for-flow/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_account_rewards_for_flow_graph(
    request: Request,
    net: str,
    account_id: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    blocks_per_day: dict[str, MongoTypeBlockPerDay] = Depends(get_blocks_per_day),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """
    Endpoint to get all rewards for a given account that should be included in the flow graph for CCD.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    amended_start_date = f"{(dateutil.parser.parse(start_date) - dt.timedelta(days=1)):%Y-%m-%d}"
    start_block = blocks_per_day.get(amended_start_date)
    if start_block:
        start_block = start_block.height_for_first_block
    else:
        start_block = 0

    end_block = blocks_per_day.get(end_date)
    if end_block:
        end_block = end_block.height_for_last_block
    else:
        end_block = 1_000_000_000

    db_to_use = mongomotor.mainnet
    try:
        pipeline = [
            {
                "$match": {"impacted_address_canonical": {"$eq": account_id[:29]}},
            },
            {"$match": {"block_height": {"$gte": start_block, "$lte": end_block}}},
            {"$match": {"effect_type": "Account Reward"}},
            {
                "$group": {
                    "_id": "$impacted_address",
                    "sum_finalization_reward": {
                        "$sum": "$balance_movement.finalization_reward",
                    },
                    "sum_baker_reward": {
                        "$sum": "$balance_movement.baker_reward",
                    },
                    "sum_transaction_fee_reward": {
                        "$sum": "$balance_movement.transaction_fee_reward",
                    },
                },
            },
        ]
        rewards_for_account = await await_await(db_to_use, Collections.impacted_addresses, pipeline)
        if len(rewards_for_account) > 0:
            rewards_for_account = rewards_for_account[0]
        else:
            rewards_for_account = {
                "sum_transaction_fee_reward": 0,
                "sum_baker_reward": 0,
                "sum_finalization_reward": 0,
            }

        account_rewards_pre_payday = await db_to_use[
            Collections.impacted_addresses_pre_payday
        ].find_one({"impacted_address_canonical": {"$eq": account_id[:29]}})
        if account_rewards_pre_payday:
            account_rewards_total = (
                account_rewards_pre_payday["sum_transaction_fee_reward"]
                + account_rewards_pre_payday["sum_baker_reward"]
                + account_rewards_pre_payday["sum_finalization_reward"]
                + rewards_for_account["sum_transaction_fee_reward"]
                + rewards_for_account["sum_baker_reward"]
                + rewards_for_account["sum_finalization_reward"]
            )

        else:
            if len(rewards_for_account) > 0:
                account_rewards_total = (
                    rewards_for_account["sum_transaction_fee_reward"]
                    + rewards_for_account["sum_baker_reward"]
                    + rewards_for_account["sum_finalization_reward"]
                )
            else:
                account_rewards_total = 0

        return account_rewards_total

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't determine whether account {account_id} on {net}  has rewards with error {error}.",
        )


@router.get(
    "/{net}/account/{account_id}/deployed",
    response_class=JSONResponse,
)
async def get_account_deployment_tx(
    request: Request,
    net: str,
    account_id: str,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_BlockItemSummary | None:
    """
    Endpoint to get tx in which the account was deployed.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    pipeline = [
        {"$match": {"account_creation": {"$exists": True}}},
        {"$match": {"account_creation.address": account_id}},
    ]
    result = await await_await(db_to_use, Collections.transactions, pipeline, 1)

    if len(result) > 0:
        result = CCD_BlockItemSummary(**result[0])
        return result
    else:
        # account existed in genesis block
        return None


@router.get(
    "/{net}/account/{account_address}/aliases-in-use",
    response_class=JSONResponse,
)
async def get_aliases_in_use_for_account(
    request: Request,
    net: str,
    account_address: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """
    Endpoint to get all aliases that are in use for a specific account address.


    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    pipeline = [
        {
            "$match": {"effect_type": {"$ne": "data_registered"}},
        },
        {
            "$match": {"impacted_address_canonical": {"$eq": account_address[:29]}},
        },
        {
            "$group": {
                "_id": "$impacted_address",
            }
        },
        {
            "$project": {
                "_id": 1,
            }
        },
    ]
    result = await await_await(db_to_use, Collections.impacted_addresses, pipeline)

    aliases = [x for x in result if x["_id"] != account_address]
    return aliases


@router.get(
    "/{net}/account/{account_id}/graph/{coin}",
    response_class=JSONResponse,
)
async def get_account_graph(
    request: Request,
    net: str,
    account_id: str,
    coin: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    exchange_rates_historical: dict = Depends(get_exchange_rates_historical),
    blocks_per_day: dict[str, MongoTypeBlockPerDay] = Depends(get_blocks_per_day),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """
    Endpoint to get account graph for coin.
    """
    if net not in ["mainnet"]:
        raise HTTPException(
            status_code=404,
            detail="We only support mainnet for this graph.",
        )

    if coin != "CCD":
        raise HTTPException(
            status_code=404,
            detail="Only works for CCD.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    try:
        pipeline = [
            {"$match": {"account_address_canonical": account_id[:29]}},
            {"$match": {"type": "account_graph"}},
            {
                "$project": {
                    "_id": 0,
                    "date": 1,
                    "ccd_balance": 1,
                    "ccd_balance_in_USD": 1,
                }
            },
            {"$sort": {"date": ASCENDING}},
        ]
        result = await await_await(db_to_use, Collections.statistics, pipeline)

        return result
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve transactions for account at {account_id} on {net} with error {error}.",
        )


# bump
