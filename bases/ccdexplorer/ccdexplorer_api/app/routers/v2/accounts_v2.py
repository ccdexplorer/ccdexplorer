"""Routes for paginating, searching, and summarizing account-level data."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false

import json
import re
import asyncio

from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
import httpx
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_grpcclient,
    get_httpx_client,
    get_mongo_motor,
)
from ccdexplorer.domain.credential import Identity
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import MongoTypePaydaysPerformance, MongoTypePaydayV2
from ccdexplorer.domain.node import ConcordiumNodeFromDashboard
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountIndex,
    CCD_AccountPending,
    CCD_BlockItemSummary,
    CCD_IpInfo,
)
from ccdexplorer.mongodb import (
    Collections,
    MongoMotor,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from grpc._channel import _InactiveRpcError

router = APIRouter(tags=["Accounts"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)


@router.get("/{net}/accounts/newer/than/{since_index}", response_class=JSONResponse)
async def get_last_accounts_newer_than(
    request: Request,
    net: str,
    since_index: int,
    # tx_index: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """Return accounts created after the given index threshold.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        since_index: Minimum account index to include (exclusive).
        mongomotor: Mongo client dependency used to read ``all_account_addresses``.
        api_key: API key extracted from the request headers.

    Returns:
        Up to 1000 account records ordered by index descending.

    Raises:
        HTTPException: If the network is unsupported or MongoDB query fails.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    response = await httpx_client.get(f"{request.app.api_url}/v2/{net}/misc/identity-providers")
    identity_providers = {}
    for id in response.json():
        id = CCD_IpInfo(**id)
        identity_providers[str(id.identity)] = {
            "ip_identity": id.identity,
            "ip_description": id.description.name,
        }
    try:
        accounts = await await_await(
            db_to_use,
            Collections.all_account_addresses,
            [
                {"$match": {"account_index": {"$gt": since_index}}},
                {"$sort": {"account_index": -1}},
                {"$limit": 1000},
            ],
        )
        rr = []
        for x in accounts:
            account_info = grpcclient.get_account_info(
                "last_final", account_index=x["account_index"], net=NET(net)
            )
            response = await httpx_client.get(
                f"{request.app.api_url}/v2/{net}/account/{account_info.address}/deployed"
            )
            response.raise_for_status()
            deployment_tx = CCD_BlockItemSummary(**response.json())
            if account_info.stake:
                if account_info.stake.delegator:
                    staking = "Delegator"
                elif account_info.stake.baker:
                    staking = "Validator"
                else:
                    staking = None
            else:
                staking = None
            rr.append(
                {
                    "address": account_info.address,
                    "account_index": account_info.index,
                    "available_balance": account_info.available_balance,
                    "sequence_number": account_info.sequence_number,
                    "staking": staking,
                    "identity": identity_providers[
                        str(Identity(account_info).credentials[0]["ip_identity"])
                    ]["ip_description"],
                    "deployment_tx_slot_time": deployment_tx.block_info.slot_time,
                }
            )

        return rr or []

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving accounts on {net} newer than {since_index}. {error}",
        )


@router.get("/{net}/accounts/info/count", response_class=JSONResponse)
async def get_accounts_count_estimate(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """Return the approximate number of accounts maintained on the network.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to read ``all_account_addresses``.
        api_key: API key extracted from the request headers.

    Returns:
        Estimated document count derived from the highest account index plus one.

    Raises:
        HTTPException: If the network is unsupported or the query fails.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = await await_await(
            db_to_use,
            Collections.all_account_addresses,
            [
                {"$sort": {"account_index": -1}},
            ],
            1,
        )
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return int(result[0]["account_index"]) + 1
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving accounts count on {net}, {error}.",
        )


@router.post("/{net}/accounts/get-indexes", response_class=JSONResponse)
async def get_account_indexes(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Resolve canonical account ids to their on-chain account indexes.

    Args:
        request: FastAPI request containing a JSON array of ids.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to read ``all_account_addresses``.
        api_key: API key extracted from the request headers.

    Returns:
        Mapping from canonical account id to index.

    Raises:
        HTTPException: If the network is unsupported or the query fails.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    body = await request.body()
    if body and body != b"{}":
        account_ids = json.loads(body.decode("utf-8"))

    else:
        account_ids = []
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = await await_await(
            db_to_use,
            Collections.all_account_addresses,
            [{"$match": {"_id": {"$in": account_ids}}}],
        )
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return {x["_id"]: x["account_index"] for x in result}
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving accounts list on {net}, {error}.",
        )


@router.get("/{net}/accounts/search/{value}", response_class=JSONResponse)
async def search_accounts(
    request: Request,
    net: str,
    value: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """Return account documents for a provided list of canonical ids.

    Args:
        request: FastAPI request containing a JSON array of ids.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to query ``all_account_addresses``.
        api_key: API key extracted from the request headers.

    Returns:
        Matching account documents.

    Raises:
        HTTPException: If the network is unsupported or the query fails.
    """
    """Perform a case-insensitive search of accounts by hash, alias, or index.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        value: Partial string to match.
        mongomotor: Mongo client dependency used to read ``all_account_addresses``.
        api_key: API key extracted from the request headers.

    Returns:
        Account records that satisfy the regex search.

    Raises:
        HTTPException: If the network is unsupported.
    """
    search_str = str(value)
    regex = re.compile(search_str, re.IGNORECASE)
    search_str_canonical = str(value)[:29]
    regex_canonical = re.compile(search_str_canonical, re.IGNORECASE)
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        {"$addFields": {"account_index_str": {"$toString": "$account_index"}}},
        {
            "$match": {
                "$or": [
                    {"_id": {"$regex": regex_canonical}},
                    {"account_address": {"$regex": regex}},
                    {"account_index_str": {"$regex": regex}},
                ]
            }
        },
    ]
    result = await await_await(db_to_use, Collections.all_account_addresses, pipeline)
    return result


@router.post("/{net}/accounts/get-addresses", response_class=JSONResponse)
async def get_account_addresses(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Resolve account indexes to canonical account addresses.

    Args:
        request: FastAPI request containing a JSON array of indexes.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to read ``all_account_addresses``.
        api_key: API key extracted from the request headers.

    Returns:
        Mapping from account index to canonical id.

    Raises:
        HTTPException: If the network is unsupported or the lookup fails.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    body = await request.body()
    if body and body != b"{}":
        account_indexes = json.loads(body.decode("utf-8"))

    else:
        account_indexes = []
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = await await_await(
            db_to_use,
            Collections.all_account_addresses,
            [{"$match": {"account_index": {"$in": account_indexes}}}],
        )
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return {x["account_index"]: x["_id"] for x in result}
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving accounts list on {net}, {error}.",
        )


@router.get("/{net}/accounts/current-payday/info", response_class=JSONResponse)
async def get_current_payday_info(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """Return summary information for the current payday window.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to query payday collections.
        api_key: API key extracted from the request headers.

    Returns:
        List of payday info dictionaries ordered by date descending.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = (
            await db_to_use[Collections.paydays_v2_current_payday].find({}).to_list(length=None)
        )
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving current payday info, {error}.",
        )


@router.get("/{net}/accounts/last-payday-block/info", response_class=JSONResponse)
async def get_last_payday_info(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the last payday block info.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    result = await db_to_use[Collections.paydays_v2].find_one(sort=[("date", -1)])
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail="Error retrieving last payday block info.",
        )


@router.get("/{net}/accounts/last/{count}", response_class=JSONResponse)
async def get_last_accounts(
    request: Request,
    net: str,
    count: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """
    Endpoint to get the last X accounts. Maxes out at 50.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    count = min(50, max(count, 1))
    error = None
    try:
        result = [
            x["account_index"]
            for x in await db_to_use[Collections.all_account_addresses]
            .find({}, {"account_index": 1, "_id": 0})
            .sort({"account_index": -1})
            .to_list(count)
        ]

        accounts = []
        for account_index in result:
            account_info = grpcclient.get_account_info(
                "last_final", account_index=account_index, net=NET(net)
            )

            pipeline = [
                {"$match": {"account_creation": {"$exists": True}}},
                {"$match": {"account_creation.address": account_info.address}},
            ]
            result = await await_await(db_to_use, Collections.transactions, pipeline)

            if len(result) > 0:
                result = CCD_BlockItemSummary(**result[0])
            else:
                result = None
            accounts.append({"account_info": account_info, "deployment_tx": result})

    except Exception as error:  # noqa: F811
        print(error)
        result = None

    if result:
        return accounts
    else:
        error = None
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving last {count} accounts on {net}, {error}.",
        )


@router.get(
    "/{net}/accounts/business/{skip}/{limit}/{sort_key}/{direction}", response_class=JSONResponse
)
async def get_business_accounts(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the accounts registered through Global Finreg. Maxes out at 50.

    """
    if net not in ["mainnet"]:
        raise HTTPException(
            status_code=404,
            detail="This endpoint only supports mainnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    error = None
    if sort_key:
        sort_key = "nonce" if sort_key == "sequence_number" else sort_key
        sort_key = "account_index" if sort_key == "index" else sort_key
        # sort_key = "nonce" if sort_key == "available_balance" else sort_key
    try:
        pipeline = [{"$match": {"credentials.ip_identity": 3}}]
        if sort_key:
            pipeline.append({"$sort": {sort_key: 1 if direction == "asc" else -1}})
        pipeline.extend(
            [
                {
                    "$facet": {
                        "results": [
                            {"$skip": skip},
                            {"$limit": limit},
                        ],
                        "totalCount": [
                            {"$count": "count"},
                        ],
                    }
                },
                {"$limit": 1},
            ]
        )
        agg = await await_await(db_to_use, Collections.stable_address_info, pipeline)
        if agg:
            facet = agg[0]
            result = facet.get("results", [])
            total_rows = facet.get("totalCount", [{"count": 0}])[0]["count"]
        else:
            result = []
            total_rows = 0

    except Exception as error:  # noqa: F811
        print(error)
        result = None

    if result:
        enriched_results = []
        for account in result:
            account_info = grpcclient.get_account_info(
                "last_final", account_index=account["account_index"], net=NET(net)
            )
            enriched_results.append({"account_info": account_info})

        return {"results": enriched_results, "total_rows": total_rows}
    else:
        error = None
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving business accounts on {net}, {error}.",
        )


@router.get("/{net}/accounts/nodes-validators", response_class=JSONResponse)
async def get_nodes_and_validators(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get nodes and validators.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    all_nodes = await db_to_use[Collections.dashboard_nodes].find({}).to_list(length=None)
    all_nodes_by_node_id = {x["nodeId"]: x for x in all_nodes}

    if net == "mainnet":
        all_validators = [
            x
            for x in await db_to_use[Collections.paydays_v2_current_payday]
            .find({})
            .to_list(length=None)
        ]

        all_validators_by_validator_id = {x["baker_id"]: x for x in all_validators}

        validator_nodes_by_validator_id = {
            x["consensusBakerId"]: {
                "node": ConcordiumNodeFromDashboard(**x),
                "validator": all_validators_by_validator_id[str(x["consensusBakerId"])],
            }
            for x in all_nodes
            if x["consensusBakerId"] is not None
            if str(x["consensusBakerId"]) in all_validators_by_validator_id.keys()
        }

        validator_nodes_by_account_id = {
            all_validators_by_validator_id[str(x["consensusBakerId"])]["pool_status"]["address"]: {
                "node": ConcordiumNodeFromDashboard(**x),
                "validator": all_validators_by_validator_id[str(x["consensusBakerId"])],
            }
            for x in all_nodes
            if x["consensusBakerId"] is not None
            if str(x["consensusBakerId"]) in all_validators_by_validator_id.keys()
        }

        non_validator_nodes_by_node_id = {
            x["nodeId"]: {"node": ConcordiumNodeFromDashboard(**x), "validator": None}
            for x in all_nodes
            if x["consensusBakerId"] is None
        }

        non_reporting_validators_by_validator_id = {
            x["baker_id"]: {
                "node": None,
                "validator": all_validators_by_validator_id[str(x["baker_id"])],
            }
            for x in all_validators
            if x["baker_id"] not in validator_nodes_by_validator_id.keys()
        }

        non_reporting_validators_by_account_id = {
            all_validators_by_validator_id[x["baker_id"]]["pool_status"]["address"]: {
                "node": None,
                "validator": all_validators_by_validator_id[str(x["baker_id"])],
            }
            for x in all_validators
            if x["baker_id"] not in validator_nodes_by_validator_id.keys()
        }

    result_dict = {"all_nodes_by_node_id": all_nodes_by_node_id}
    if net == "mainnet":
        result_dict.update(
            {
                "all_validators_by_validator_id": all_validators_by_validator_id,
                "validator_nodes_by_account_id": validator_nodes_by_account_id,
                "non_validator_nodes_by_node_id": non_validator_nodes_by_node_id,
                "non_reporting_validators_by_validator_id": non_reporting_validators_by_validator_id,
                "non_reporting_validators_by_account_id": non_reporting_validators_by_account_id,
            }
        )
    return result_dict


@router.get("/{net}/accounts/validators/primed-suspended", response_class=JSONResponse)
async def get_validator_primed_suspended_information(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get validator primed suspended information.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        {"$match": {"action": "suspended"}},
        {
            "$group": {
                "_id": "$baker_id",
                "dates": {"$push": "$date"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "baker_id": "$_id",
                "dates": {"$sortArray": {"input": "$dates", "sortBy": -1}},
            }
        },
    ]
    suspended_validators_source = await await_await(db_to_use, Collections.validator_logs, pipeline)

    pipeline = [
        {"$match": {"action": "primed"}},
        {
            "$group": {
                "_id": "$baker_id",
                "dates": {"$push": "$date"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "baker_id": "$_id",
                "dates": {"$sortArray": {"input": "$dates", "sortBy": -1}},
            }
        },
    ]

    primed_validators_source = await await_await(db_to_use, Collections.validator_logs, pipeline)

    suspended_validators = []
    for v in suspended_validators_source:
        ai = grpcclient.get_account_info("last_final", account_index=v["baker_id"], net=NET(net))
        await asyncio.sleep(0.01)
        if ai.stake:
            if ai.stake.baker:
                if ai.stake.baker.is_suspended:
                    suspended_validators.append(
                        {
                            "baker_id": v["baker_id"],
                            "dates": v["dates"],
                        }
                    )

    suspended_validators = sorted(suspended_validators, key=lambda x: x["baker_id"])
    suspended_validators = {v["baker_id"]: v["dates"] for v in suspended_validators}
    primed_validators = {v["baker_id"]: v["dates"] for v in primed_validators_source}
    return {
        "suspended_validators": suspended_validators,
        "primed_validators": primed_validators,
    }


@router.get("/{net}/accounts/paydays/pools/{status}", response_class=JSONResponse)
async def get_payday_pools(
    request: Request,
    net: str,
    status: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get payday pools.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    #### get dashboard nodes to get node name
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    all_nodes = await await_await(db_to_use, Collections.dashboard_nodes, [])
    pools_for_status: dict[str, list] = {}
    if net == "mainnet":
        all_validators = [
            x for x in await await_await(db_to_use, Collections.paydays_v2_current_payday, [])
        ]

        all_validators_by_validator_id = {x["baker_id"]: x for x in all_validators}

        validator_nodes_by_validator_id = {
            x["consensusBakerId"]: {
                "node": ConcordiumNodeFromDashboard(**x),
                "validator": all_validators_by_validator_id[str(x["consensusBakerId"])],
            }
            for x in all_nodes
            if x["consensusBakerId"] is not None
            if str(x["consensusBakerId"]) in all_validators_by_validator_id.keys()
        }
        suspended_validators = {}
        for validator_id in all_validators_by_validator_id.keys():
            try:
                pool = grpcclient.get_pool_info_for_pool(int(validator_id), "last_final")
            except _InactiveRpcError:
                pool = None
            if pool:
                if pool.pool_info:
                    if pool.pool_info.open_status not in pools_for_status.keys():
                        pools_for_status[pool.pool_info.open_status] = []
                    pools_for_status[pool.pool_info.open_status].append(int(validator_id))
                if pool.is_suspended:
                    suspended_validators[validator_id] = True

    last_payday = MongoTypePaydayV2(
        **await db_to_use[Collections.paydays_v2].find_one(sort=[("date", -1)])  # type: ignore
    )
    pools_for_status = pools_for_status.get(status, [])  # type: ignore

    result = await mongomotor.mainnet[Collections.paydays_v2_current_payday].find().to_list(100_000)
    last_payday_performance = {
        x["baker_id"]: MongoTypePaydaysPerformance(**x)
        for x in result
        if ((str(x["baker_id"]).isnumeric()) and (int(x["baker_id"]) in pools_for_status))
    }

    pipeline = [
        {
            "$match": {
                "validator_id": {"$exists": True},
                "days_in_average": {"$gt": 1},
                "date": last_payday.date,
                "type": "delegator",
            }
        },
    ]
    result = await await_await(mongomotor.mainnet, Collections.paydays_v2_apy, pipeline, 100_000)

    last_payday_apy_objects = {f"{x['validator_id']}-{x['days_in_average']}": x for x in result}

    dd = {}
    for baker_id in last_payday_performance.keys():
        node_name = None
        if baker_id in validator_nodes_by_validator_id:
            if "node" in validator_nodes_by_validator_id[baker_id]:
                node_name = validator_nodes_by_validator_id[baker_id].get("node").nodeName

        delegated_percentage = (
            (
                last_payday_performance[baker_id].pool_status.delegated_capital
                / last_payday_performance[baker_id].pool_status.delegated_capital_cap
            )  # type: ignore
            * 100
            if last_payday_performance[baker_id].pool_status.delegated_capital_cap > 0  # type: ignore
            else 0
        )

        delegated_percentage_remaining = 100 - delegated_percentage
        pie = (
            f"<style> .pie_{baker_id} {{\n"
            f"width: 20px;\nheight: 20px;\n"
            f"background-image: conic-gradient(#AE7CF7 0%, #AE7CF7 {delegated_percentage}%, #70B785 0%, #70B785 {delegated_percentage_remaining}%);\n"
            f" border-radius: 50%\n"
            f"}}\n</style>\n"
        )
        assert last_payday_performance[baker_id].pool_status.pool_info is not None
        d = {
            "baker_id": baker_id,
            "node_name": node_name,
            "is_suspended": baker_id in suspended_validators,
            "block_commission_rate": last_payday_performance[
                baker_id
            ].pool_status.pool_info.commission_rates.baking,
            "tx_commission_rate": last_payday_performance[
                baker_id
            ].pool_status.pool_info.commission_rates.transaction,
            "expectation": last_payday_performance[baker_id].expectation,
            "lottery_power": last_payday_performance[
                baker_id
            ].pool_status.current_payday_info.lottery_power,
            "url": last_payday_performance[baker_id].pool_status.pool_info.url,
            "effective_stake": last_payday_performance[
                baker_id
            ].pool_status.current_payday_info.effective_stake,
            "delegated_capital": last_payday_performance[baker_id].pool_status.delegated_capital,
            "delegated_capital_cap": last_payday_performance[
                baker_id
            ].pool_status.delegated_capital_cap,
            "baker_equity_capital": last_payday_performance[
                baker_id
            ].pool_status.current_payday_info.baker_equity_capital,
            "delegated_percentage": delegated_percentage,
            "delegated_percentage_remaining": delegated_percentage_remaining,
            "pie": pie,
            "d30": (
                last_payday_apy_objects.get(
                    f"{baker_id}-30",
                    {"apy": 0.0, "sum_of_rewards": 0, "count_of_days": 0},
                )
            ),
            "d90": (
                last_payday_apy_objects.get(
                    f"{baker_id}-90",
                    {"apy": 0.0, "sum_of_rewards": 0, "count_of_days": 0},
                )
            ),
            "d180": (
                last_payday_apy_objects.get(
                    f"{baker_id}-180",
                    {"apy": 0.0, "sum_of_rewards": 0, "count_of_days": 0},
                )
            ),
        }
        dd[baker_id] = d

    return dd


@router.get(
    "/{net}/accounts/paydays/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paydays(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get paydays.
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
    total_rows = await db_to_use[Collections.paydays_v2].count_documents({})
    result = (
        await db_to_use[Collections.paydays_v2]
        .find(sort=[("date", -1)])
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )
    result = {
        "result": result,
        "total_rows": total_rows,
    }
    return result


@router.get(
    "/{net}/accounts/paydays/passive-delegation",
    response_class=JSONResponse,
)
async def get_payday_passive_info(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get payday passive information.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    passive_delegation_info = grpcclient.get_passive_delegation_info("last_final")
    return {"passive_delegation_info": passive_delegation_info}


@router.get(
    "/{net}/accounts/paydays/passive-delegators/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_payday_passive_delegators(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get payday passive delegators.
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

    delegators_current_payday = [
        x for x in grpcclient.get_delegators_for_passive_delegation_in_reward_period("last_final")
    ]
    delegators_in_block = [
        x for x in grpcclient.get_delegators_for_passive_delegation("last_final")
    ]

    delegators_current_payday_list = set([x.account for x in delegators_current_payday])

    delegators_in_block_list = set([x.account for x in delegators_in_block])

    delegators_current_payday_dict = {x.account: x for x in delegators_current_payday}
    delegators_in_block_dict = {x.account: x for x in delegators_in_block}

    new_delegators = delegators_in_block_list - delegators_current_payday_list

    # delegators_in_block_list = list(delegators_in_block_list)
    new_delegators_dict = {x: delegators_in_block_dict[x] for x in new_delegators}

    delegators = sorted(delegators_current_payday, key=lambda x: x.stake, reverse=True)
    return {
        "delegators": delegators[skip : (skip + limit)],
        "delegators_current_payday_dict": delegators_current_payday_dict,
        "delegators_in_block_dict": delegators_in_block_dict,
        "new_delegators_dict": new_delegators_dict,
        "total_rows": len(delegators),
    }


@router.get(
    "/{net}/accounts/scheduled-release/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_scheduled_release_accounts(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all accounts that have scheduled releases,
    with the timestamp of the first pending scheduled release
    for that account. (Note, this only identifies accounts by index,
    and only indicates the first pending release for each account.)
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    scheduled_release_accounts_org = grpcclient.get_scheduled_release_accounts(
        "last_final", NET(net)
    )
    scheduled_release_accounts = scheduled_release_accounts_org[skip : (skip + limit)]

    return_list = []
    for account_release in scheduled_release_accounts:
        account_info = grpcclient.get_account_info(
            "last_final",
            account_index=account_release.account_index,
            net=NET(net),
        )
        return_list.append(
            {
                "account_index": account_release.account_index,
                "account_amount": account_info.amount,
                "account_schedule": account_info.schedule,  # type: ignore
                # if not isinstance(x, tuple)
            }
        )

    return {
        "total_rows": len(scheduled_release_accounts_org),
        "accounts": return_list,
    }


@router.get(
    "/{net}/accounts/scheduled-release",
    response_class=JSONResponse,
)
async def get_scheduled_release_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_AccountPending]:
    """
    Endpoint to get all accounts that have scheduled releases,
    with the timestamp of the first pending scheduled release
    for that account. (Note, this only identifies accounts by index,
    and only indicates the first pending release for each account.)
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    scheduled_release_accounts = grpcclient.get_scheduled_release_accounts("last_final", NET(net))

    return scheduled_release_accounts


@router.get(
    "/{net}/accounts/cooldown",
    response_class=JSONResponse,
)
async def get_cooldown_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """
    Endpoint to get all accounts that have stake in cooldown,
    with the timestamp of the first pending cooldown expiry for
    each account. (Note, this only identifies accounts by index,
    and only indicates the first pending cooldown for each account.)
    Prior to protocol version 7, the resulting stream will always be empty.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    cooldown_accounts = grpcclient.get_cooldown_accounts("last_final", NET(net))
    return_list = []
    for account_pending in cooldown_accounts:
        return_list.append(
            {
                "account_index": account_pending.account_index,
                "account_cooldowns": [
                    x.model_dump()
                    for x in grpcclient.get_account_info(
                        "last_final",
                        account_index=account_pending.account_index,
                        net=NET(net),
                    ).cooldowns  # type: ignore
                ],
            }
        )

    return return_list


@router.get(
    "/{net}/accounts/pre-cooldown",
    response_class=JSONResponse,
)
async def get_pre_cooldown_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_AccountIndex]:
    """
    Endpoint to get all accounts that have stake in pre-cooldown.
    (This only identifies accounts by index.) Prior to protocol version 7,
    the resulting stream will always be empty.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    pre_cooldown_accounts = grpcclient.get_pre_cooldown_accounts("last_final", NET(net))

    return pre_cooldown_accounts


@router.get(
    "/{net}/accounts/pre-pre-cooldown",
    response_class=JSONResponse,
)
async def get_pre_pre_cooldown_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_AccountIndex]:
    """
    Endpoint to get all accounts that have stake in pre-pre-cooldown.
    (This only identifies accounts by index.) Prior to protocol version 7,
    the resulting stream will always be empty.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    pre_pre_cooldown_accounts = grpcclient.get_pre_pre_cooldown_accounts("last_final", NET(net))

    return pre_pre_cooldown_accounts


@router.get(
    "/{net}/accounts/paginated/skip/{skip}/limit/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_accounts(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to page through the `accounts` collection using skip/limit.
    """
    # validate network
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Unsupported network. Choose 'mainnet' or 'testnet'.",
        )

    db = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    limit = min(limit, 100)
    response = await httpx_client.get(f"{request.app.api_url}/v2/{net}/misc/identity-providers")
    identity_providers = {}
    for id in response.json():
        id = CCD_IpInfo(**id)
        identity_providers[str(id.identity)] = {
            "ip_identity": id.identity,
            "ip_description": id.description.name,
        }
    try:
        # total documents for client-side page computations
        total_docs = await db[Collections.all_account_addresses].estimated_document_count()

        # fetch the requested slice, sorted by height desc
        cursor = (
            db[Collections.all_account_addresses]
            .find({})
            .sort("account_index", -1)
            .skip(skip)
            .limit(limit)
        )
        accounts = await cursor.to_list(length=limit)
        rr = []
        for x in accounts:
            account_info = grpcclient.get_account_info(
                "last_final", account_index=x["account_index"], net=NET(net)
            )
            response = await httpx_client.get(
                f"{request.app.api_url}/v2/{net}/account/{account_info.address}/deployed"
            )
            response.raise_for_status()
            deployment_tx = CCD_BlockItemSummary(**response.json())
            if account_info.stake:
                if account_info.stake.delegator:
                    staking = "Delegator"
                elif account_info.stake.baker:
                    staking = "Validator"
                else:
                    staking = None
            else:
                staking = None
            rr.append(
                {
                    "address": account_info.address,
                    "account_index": account_info.index,
                    "available_balance": account_info.available_balance,
                    "sequence_number": account_info.sequence_number,
                    "staking": staking,
                    # {{identity_providers[identity.credentials[0].ip_identity|string].ip_description}}
                    "identity": identity_providers[
                        str(Identity(account_info).credentials[0]["ip_identity"])
                    ]["ip_description"],
                    "deployment_tx_slot_time": deployment_tx.block_info.slot_time,
                }
            )

        return {
            "total_rows": total_docs,
            "accounts": rr,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving accounts: {e}",
        )


@router.get(
    "/{net}/accounts/credential-summary",
    response_class=JSONResponse,
)
async def get_credential_summary(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_AccountIndex]:
    """
    Endpoint to get a summary of credential information for all accounts
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        # One row per credential
        {"$unwind": "$credentials"},
        {
            "$facet": {
                # 1. Counts over policy_attributes key/value pairs
                "key_value_counts": [
                    {
                        "$match": {
                            "credentials.policy_attributes": {
                                "$exists": True,
                                "$ne": [],
                            }
                        }
                    },
                    {"$unwind": "$credentials.policy_attributes"},
                    {
                        "$group": {
                            "_id": {
                                "key": "$credentials.policy_attributes.key",
                                "value": "$credentials.policy_attributes.value",
                            },
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"count": -1, "_id.key": 1, "_id.value": 1}},
                ],
                # 2. Counts over ip_identity (per credential)
                "ip_identity_counts": [
                    {
                        "$group": {
                            "_id": "$credentials.ip_identity",
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ],
                # 3. Counts over commitment_attributes (per attribute string)
                "commitment_attribute_counts": [
                    {
                        "$match": {
                            "credentials.commitment_attributes": {
                                "$exists": True,
                                "$ne": [],
                            }
                        }
                    },
                    {"$unwind": "$credentials.commitment_attributes"},
                    {
                        "$group": {
                            "_id": "$credentials.commitment_attributes",
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"count": -1, "_id": 1}},
                ],
            }
        },
    ]
    result = await await_await(db_to_use, Collections.stable_address_info, pipeline)

    raw_kv = result[0]["key_value_counts"]
    raw_ip = result[0]["ip_identity_counts"]
    raw_commit = result[0]["commitment_attribute_counts"]

    # Convert to Python dicts
    kv_counts = {(d["_id"]["key"], d["_id"]["value"]): d["count"] for d in raw_kv}

    ip_counts = {d["_id"]: d["count"] for d in raw_ip}
    commitment_counts = {d["_id"]: d["count"] for d in raw_commit}
    if len(result) > 0:
        result = CCD_BlockItemSummary(**result[0])
    else:
        result = None

    return pre_pre_cooldown_accounts
