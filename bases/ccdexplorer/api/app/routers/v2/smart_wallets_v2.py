# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.api.app.utils import await_await
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from fastapi import APIRouter, Depends, Request, Security, HTTPException
from fastapi.responses import JSONResponse
from pymongo.collection import Collection
from pymongo import DESCENDING
from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.api.app.state_getters import get_mongo_db, get_mongo_motor

router = APIRouter(tags=["Smart Wallets"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get(
    # /v2/mainnet/smart-wallets/transactions/newer/than/34174755
    "/{net}/smart-wallets/transactions/newer/than/{since}",
    response_class=JSONResponse,
)
async def get_last_smart_wallet_transactions_newer_than(
    request: Request,
    net: str,
    since: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the last X smart wallet transactions that are newer than block `since`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    return await _get_paginated_smart_wallets_txs_non_route(
        net=net,
        skip=0,
        limit=33,
        mongomotor=mongomotor,
        block_since=since,
    )


@router.get("/{net}/smart-wallets/overview/all", response_class=JSONResponse)
async def get_all_smart_wallet_contracts_info(
    request: Request,
    net: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Fetches all unique smart wallet contract addresses from the specified MongoDB collection.

    Args:
        request (Request): The request object.
        net (str): The network type, either "testnet" or "mainnet".
        mongodb (MongoDB, optional): The MongoDB dependency, defaults to the result of get_mongo_db.
        api_key (str, optional): The API key for security, defaults to the result of API_KEY_HEADER.

    Returns:
        list[str]: A list of unique smart wallet contract addresses.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    distinct_wallet_addresses = list(
        db_to_use[Collections.cis5_public_keys_contracts].distinct("wallet_contract_address")
    )
    distinct_wallet_addresses_result = list(
        db_to_use[Collections.instances].find({"_id": {"$in": distinct_wallet_addresses}})
    )
    wallets_dict = {}

    for x in distinct_wallet_addresses_result:
        wallets_dict[x["_id"]] = {
            "name": x["v1"]["name"][5:].replace("_", " ").capitalize(),
            "source_module": x["source_module"],
        }
        pipeline = [
            {"$match": {"wallet_contract_address": x["_id"]}},
            {"$count": "count"},
        ]
        # pipeline = [
        #     {"$match": {"wallet_contract_address": x["_id"]}},
        #     {
        #         "$match": {
        #             "$expr": {"$eq": [{"$strLenCP": "$address_or_public_key"}, 64]}
        #         }
        #     },
        #     {"$group": {"_id": "$address_or_public_key"}},
        #     {"$count": "distinct_count"},
        # ]

        count = list(db_to_use[Collections.cis5_public_keys_info].aggregate(pipeline))
        wallets_dict[x["_id"]].update({"count_of_unique_addresses": count[0]["count"]})
        pipeline = [
            {"$match": {"event_info.standard": "CIS-5"}},
            {"$match": {"event_info.contract": x["_id"]}},
            {"$sort": {"tx_info.block_height": -1}},
            {"$limit": 100},
        ]

        result = list(db_to_use[Collections.tokens_logged_events_v2].aggregate(pipeline))
        active_addresses = {}
        for log in result:
            if "to_address_canonical" in log:
                if len(log["to_address_canonical"]) == 64:
                    if (log["to_address_canonical"] not in active_addresses) and (
                        log["from_address_canonical"] not in active_addresses
                    ):
                        active_addresses[log["to_address_canonical"]] = {
                            "public_key": log["to_address_canonical"],
                            "last_active_date": log["tx_info"]["date"],
                            "last_active_block": log["tx_info"]["block_height"],
                        }

            if "from_address_canonical" in log:
                if (log["to_address_canonical"] not in active_addresses) and (
                    log["from_address_canonical"] not in active_addresses
                ):
                    if len(log["from_address_canonical"]) == 64:
                        active_addresses[log["from_address_canonical"]] = {
                            "public_key": log["from_address_canonical"],
                            "last_active_date": log["tx_info"]["date"],
                            "last_active_block": log["tx_info"]["block_height"],
                        }
        # Sort and limit active_addresses
        sorted_addresses = dict(
            sorted(
                active_addresses.items(),
                key=lambda x: x[1]["last_active_block"],
                reverse=True,
            )[:10]
        )
        wallets_dict[x["_id"]].update({"active_addresses": sorted_addresses})
    return wallets_dict


@router.get("/{net}/smart-wallets/overview", response_class=JSONResponse)
async def get_all_smart_wallet_contracts(
    request: Request,
    net: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Fetches all unique smart wallet contract addresses from the specified MongoDB collection.

    Args:
        request (Request): The request object.
        net (str): The network type, either "testnet" or "mainnet".
        mongodb (MongoDB, optional): The MongoDB dependency, defaults to the result of get_mongo_db.
        api_key (str, optional): The API key for security, defaults to the result of API_KEY_HEADER.

    Returns:
        list[str]: A list of unique smart wallet contract addresses.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    distinct_wallet_addresses = list(
        db_to_use[Collections.cis5_public_keys_contracts].distinct("wallet_contract_address")
    )
    distinct_wallet_addresses_result = list(
        db_to_use[Collections.instances].find({"_id": {"$in": distinct_wallet_addresses}})
    )
    wallets_dict = {}

    for x in distinct_wallet_addresses_result:
        wallets_dict[x["_id"]] = {
            "name": x["v1"]["name"][5:].replace("_", " ").capitalize(),
            "source_module": x["source_module"],
        }

    return wallets_dict


def get_block_ranges_from_start_and_end_dates(
    start_date: str, end_date: str, db_to_use: dict[Collections, Collection]
) -> str:
    start_date_result = db_to_use[Collections.blocks_per_day].find_one({"date": start_date})
    if start_date_result:
        height_for_first_block_start_date = start_date_result["height_for_first_block"]
    else:
        height_for_first_block_start_date = 0

    end_date_result = db_to_use[Collections.blocks_per_day].find_one({"date": end_date})
    if end_date_result:
        height_for_last_block_end_date = end_date_result["height_for_last_block"]
    else:
        height_for_last_block_end_date = 1_000_000_000

    return height_for_first_block_start_date, height_for_last_block_end_date


@router.get(
    "/{net}/smart-wallets/public-key-creation/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_smart_wallet_public_key_creations_per_day(
    request: Request,
    net: str,
    start_date: str,
    end_date: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    height_for_first_block_start_date, height_for_last_block_end_date = (
        get_block_ranges_from_start_and_end_dates(start_date, end_date, db_to_use)
    )
    pipeline = [
        {
            "$match": {
                "deployment_block_height": {
                    "$gte": height_for_first_block_start_date,
                    "$lte": height_for_last_block_end_date,
                }
            }
        },
        {"$group": {"_id": "$date", "count": {"$count": {}}}},
    ]
    result = db_to_use[Collections.cis5_public_keys_info].aggregate(pipeline)

    return result


async def _get_paginated_smart_wallets_txs_non_route(
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor,
    block_since: int | None = None,
) -> dict:
    """
    Internal function to get paginated smart wallets transactions.
    """
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    distinct_wallet_addresses = (
        await db_to_use[Collections.cis5_public_keys_contracts].distinct("wallet_contract_address")
        # .to_list(length=None)
    )
    if block_since:
        pipeline = [{"$match": {"block_height": {"$gt": block_since}}}]
    else:
        pipeline = []

    pipeline.extend(
        [
            {
                "$match": {"impacted_address_canonical": {"$in": distinct_wallet_addresses}},
            },
            {  # this filters out account rewards, as they are special events
                "$match": {"tx_hash": {"$exists": True}},
            },
            {"$sort": {"block_height": DESCENDING}},
            {"$skip": skip},
            {"$limit": limit * 3},  # multiple addresses can be impacted by the same tx
            {"$project": {"tx_hash": 1, "impacted_address_canonical": 1, "_id": 0}},
        ]
    )
    result = await await_await(db_to_use, Collections.impacted_addresses, pipeline, limit * 3)
    dd = {x["tx_hash"]: x["impacted_address_canonical"] for x in result}
    all_txs_hashes = [x["tx_hash"] for x in result]

    int_result = (
        await db_to_use[Collections.transactions]
        .find({"_id": {"$in": all_txs_hashes}})
        .sort("block_info.height", DESCENDING)
        .to_list(limit)
    )
    tx_result = [CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in int_result]
    tx_from_dict = {
        x["hash"]: {
            "wallet_contract_address": dd[x["hash"]],
            "tx": x,
        }
        for x in tx_result
    }

    # try to get a public_key per transaction
    pipeline = [
        {"$match": {"tx_info.tx_hash": {"$in": list(tx_from_dict.keys())}}},
        {"$sort": {"event_info.effect_index": -1}},  # Sort by effect_index in descending order
    ]
    logged_events = await await_await(db_to_use, Collections.tokens_logged_events_v2, pipeline)

    # Group events by tx_hash
    public_keys_by_tx: dict = {}
    for event in logged_events:
        tx_hash = event["tx_info"]["tx_hash"]
        if tx_hash not in public_keys_by_tx:
            public_keys_by_tx[tx_hash] = []
        if "from_address_canonical" in event:
            public_keys_by_tx[tx_hash].append(event["from_address_canonical"])
        if "to_address_canonical" in event:
            public_keys_by_tx[tx_hash].append(event["to_address_canonical"])

    for tx_hash in tx_from_dict.keys():
        for event in public_keys_by_tx.get(tx_hash, []):
            highest_count_public_key = max(
                (key for key in public_keys_by_tx[tx_hash] if len(key) == 64),
                key=public_keys_by_tx[tx_hash].count,
                default=None,
            )
            if highest_count_public_key:
                tx_from_dict[tx_hash].update({"public_key": highest_count_public_key})

    return tx_from_dict


@router.get(
    "/{net}/smart-wallets/transactions/paginated/skip/{skip}/limit/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_smart_wallets_txs(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get paginated smart wallets transactions.
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

    return await _get_paginated_smart_wallets_txs_non_route(
        net=net,
        skip=skip,
        limit=limit,
        mongomotor=mongomotor,
    )
