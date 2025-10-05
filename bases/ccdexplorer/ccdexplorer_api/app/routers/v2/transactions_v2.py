# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER
from ccdexplorer.domain.mongo import MongoTypeBlockPerDay
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from ccdexplorer.mongodb import (
    MongoMotor,
    Collections,
)
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
)
import json
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_mongo_motor,
    get_blocks_per_day,
    get_memos,
)
from ccdexplorer.ccdexplorer_api.app.utils import await_await, tx_type_translation, TypeContents
from collections import defaultdict

# bump

router = APIRouter(tags=["Transactions"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


def tx_type_translator(tx_type_contents: str, request_type: str) -> str | None:
    result: TypeContents | None = tx_type_translation.get(tx_type_contents)
    if result:
        result.category.value
    else:
        return None


def reverse_tx_type_translation(tx_type_translation: dict) -> dict:
    category_to_types = defaultdict(list)

    for tx_type, contents in tx_type_translation.items():
        category_to_types[contents.category.value].append(tx_type)

    return dict(category_to_types)


reversed_type_contents_dict = reverse_tx_type_translation(tx_type_translation)


@router.get("/{net}/transaction_types", response_class=JSONResponse)
async def get_transaction_types(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """
    Endpoint to get the transaction types as stored in MongoDB collection `tx_types_count`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        return await db_to_use[Collections.tx_types_count].find({}).to_list(length=None)

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving transaction types on {net}. {error}.",
        )


@router.get("/{net}/transactions/last/{count}", response_class=JSONResponse)
async def get_last_transactions(
    request: Request,
    net: str,
    count: int,
    skip: int | None = None,
    filter: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """
    Endpoint to get the last X transactions as stored in MongoDB collection `transactions`. Maxes out at 50.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    count = min(50, max(count, 1))
    error = None

    if filter:
        filter = reversed_type_contents_dict.get(filter, None)
        if not filter:
            raise HTTPException(
                status_code=404,
                detail="Invalid filter provided. Please use one of the following: "
                + ", ".join(reversed_type_contents_dict.keys()),
            )
        filter_dict = {"type.contents": {"$in": filter}}
    else:
        filter_dict = {}
    try:
        pipeline = [
            {"$match": filter_dict} if filter_dict else {"$match": {}},
            {"$sort": {"block_info.height": -1}},
            {"$skip": skip} if skip else {"$skip": 0},
            {"$limit": count},
        ]

        result = await await_await(db_to_use, Collections.transactions, pipeline)

    except Exception as error:
        print(error)
        result = None

    if result:
        last_txs = [CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in result]
        return last_txs
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving last {count} transactions on {net}, {error}.",  # type: ignore
        )


@router.get("/{net}/transactions/newer/than/{since}", response_class=JSONResponse)
async def get_last_blocks_newer_than(
    request: Request,
    net: str,
    since: int,
    # tx_index: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_BlockItemSummary]:
    """
    Endpoint to get the last X transactions that are newer than `since` as stored in MongoDB collection `transactions`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = (
            await db_to_use[Collections.transactions]
            .find({"block_info.height": {"$gt": since}})
            .sort({"block_info.height": -1, "index": -1})
            .to_list(length=min(since, 1000))
        )
        return result
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving transactions on {net} newer than {since}. {error}",
        )


@router.get("/{net}/transactions/{count}/{skip}/{filter}", response_class=JSONResponse)
async def get_transactions_with_filter(
    request: Request,
    net: str,
    count: int,
    skip: int | None = None,
    filter: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the last X transactions as stored in MongoDB collection `transactions`. Maxes out at 50.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    count = min(500, max(count, 1))

    total_for_type = 0
    if filter:
        filter_dict = {"type.contents": filter}
        result = await db_to_use[Collections.tx_types_count].find_one({"_id": filter})
        if not result:
            total_for_type = 0
        else:
            total_for_type = result.get("count", 0)
    else:
        filter_dict = {}

    try:
        pipeline = [
            {"$match": filter_dict} if filter_dict else {"$match": {}},
            {"$sort": {"block_info.height": -1}},
            {"$skip": skip} if skip else {"$skip": 0},
            {"$limit": count},
        ]

        result = await await_await(db_to_use, Collections.transactions, pipeline)

        return {
            "transactions": [
                CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in result
            ],
            "total_for_type": total_for_type,
            "tx_type": filter,
        }
    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving last {count} transactions on {net}, {error}.",
        )


@router.get("/{net}/transactions/info/tps", response_class=JSONResponse)
async def get_transactions_tps(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
) -> dict:
    """
    Endpoint to get the transactions TPS as stored in MongoDB collection `pre_render`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    if net != "mainnet":
        raise HTTPException(
            status_code=404,
            detail="Transactions TPS information only available for mainnet.",
        )

    db_to_use = mongomotor.mainnet
    try:
        result = await db_to_use[Collections.pre_render].find_one({"_id": "tps_table"})
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving last transactions tps, {error}.",
        )


@router.get("/{net}/transactions/info/count", response_class=JSONResponse)
async def get_transactions_count_estimate(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
) -> int:
    """
    Endpoint to get the transactions estimated count.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = await db_to_use[Collections.transactions].estimated_document_count()
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving transactions count on {net}, {error}.",
        )


### search


@router.post("/{net}/transactions/search/data/{skip}/{limit}", response_class=JSONResponse)
async def get_transactions_from_data_registered(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """
    Endpoint to get the search transactions from data registered as stored in MongoDB collection `transactions`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    limit = min(50, max(limit, 1))
    error = None
    body = await request.body()
    if body:
        hex_dict = json.loads(body.decode("utf-8"))
        hex = hex_dict.get("hex")
    else:
        error = "No data registered search term provided."
    if error:
        raise HTTPException(
            status_code=400,
            detail=error,
        )

    pipeline = [
        {"$match": {"account_transaction.effects.data_registered": hex}},
        {"$sort": {"block_info.height": -1}},
        {"$skip": skip} if skip else {"$skip": 0},
        {"$limit": limit},
    ]

    result = await await_await(db_to_use, Collections.transactions, pipeline, limit)

    if result:
        txs = [CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in result]
        return txs
    else:
        return []


async def get_tx_ids_for_memo(memo_to_search: str, memos: dict):
    keys_with_sub_string = []
    for x in memos.keys():
        memo = x
        try:
            if memo_to_search in memo:
                keys_with_sub_string.append(memo)
        except Exception as e:
            print(e)
            pass

    hashes_with_sub_string = [memos[x] for x in keys_with_sub_string]

    list_of_tx_hashes_with_memo_predicate = [
        item for sublist in hashes_with_sub_string for item in sublist
    ]
    return list_of_tx_hashes_with_memo_predicate


@router.post("/{net}/transactions/search/transfers/{skip}/{limit}", response_class=JSONResponse)
async def get_transactions_from_transfers(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    memos: dict[str, list] = Depends(get_memos),
    blocks_per_day: dict[str, MongoTypeBlockPerDay] = Depends(get_blocks_per_day),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the search transactions from transfers as stored in MongoDB collection `transactions`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    limit = min(500, max(limit, 1))
    error = None
    body = await request.body()
    if body and body != b"{}":
        body_dict = json.loads(body.decode("utf-8"))
        gte = int(body_dict.get("gte"))
        lte = int(body_dict.get("lte"))
        start_date = body_dict.get("start_date")
        end_date = body_dict.get("end_date")
        memo = body_dict.get("memo")
        sort_key = "amount_ccd" if body_dict.get("sort")[0]["field"] == "amount" else "block_height"
        sort_direction = -1 if body_dict.get("sort")[0]["dir"] == "desc" else 1

    else:
        error = "No transfers info provided."
    if error:
        raise HTTPException(
            status_code=400,
            detail=error,
        )

    if memo:
        list_of_tx_hashes_with_memo_predicate = await get_tx_ids_for_memo(memo, memos)
    else:
        list_of_tx_hashes_with_memo_predicate = []

    start_block = blocks_per_day.get(start_date)
    if start_block:
        start_block = start_block.height_for_first_block
    else:
        start_block = 0

    end_block = blocks_per_day.get(end_date)
    if end_block:
        end_block = end_block.height_for_last_block
    else:
        end_block = 1_000_000_000
    pipeline = [
        {"$set": {"amount_ccd": {"$divide": ["$amount", 1000000]}}},
        {"$match": {"amount_ccd": {"$gte": gte}}},
        {"$match": {"amount_ccd": {"$lte": lte}}},
    ]

    if memo:
        pipeline.extend([{"$match": {"_id": {"$in": list_of_tx_hashes_with_memo_predicate}}}])

    pipeline.append({"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}})
    # ────── INSERT TOTAL COUNT HERE ──────
    # Clone the pipeline (so we don’t ruin the one we’ll paginate)
    count_pipeline = pipeline.copy()
    # Count how many docs match all those criteria
    count_pipeline.append({"$count": "total"})

    count_result = await await_await(
        db_to_use, Collections.involved_accounts_transfer, count_pipeline, 1
    )

    total_txs = count_result[0]["total"] if count_result else 0
    # ─────────────────────────────────────
    pipeline.extend(
        [
            {"$sort": {sort_key: sort_direction}},
            {"$skip": skip} if skip else {"$skip": 0},
            {"$project": {"_id": 1}},
        ]
    )

    result = await await_await(db_to_use, Collections.involved_accounts_transfer, pipeline, limit)

    if result:
        sort_key = (
            "amount_ccd" if body_dict.get("sort")[0]["field"] == "amount" else "block_info.height"
        )
        # get the transactions from the ids
        ids = [x["_id"] for x in result]
        pipeline = [
            {"$match": {"_id": {"$in": ids}}},
            {
                "$set": {
                    "amount_ccd": {
                        "$cond": {
                            "if": {
                                "$isArray": "$account_transaction.effects.transferred_with_schedule.amount"
                            },
                            "then": {
                                "$sum": {
                                    "$map": {
                                        "input": "$account_transaction.effects.transferred_with_schedule.amount",
                                        "as": "event",
                                        "in": {"$divide": ["$$event.amount", 1000000]},
                                    }
                                }
                            },
                            "else": {
                                "$divide": [
                                    "$account_transaction.effects.account_transfer.amount",
                                    1000000,
                                ]
                            },
                        }
                    }
                }
            },
            {"$sort": {sort_key: sort_direction}},
        ]
        result = await await_await(db_to_use, Collections.transactions, pipeline)
        txs = [
            CCD_BlockItemSummary(**x).model_dump(exclude_none=True)
            for x in result  # [0]["data"]
        ]
        return {"transactions": txs, "total_txs": total_txs}
    else:
        return {"transactions": [], "total_txs": 0}


@router.get(
    "/{net}/transactions/paginated/skip/{skip}/limit/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_transactions(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to page through the `transactions` collection using skip/limit.
    """
    # validate network
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Unsupported network. Choose 'mainnet' or 'testnet'.",
        )

    db = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    try:
        # total documents for client-side page computations
        total_docs = await db[Collections.transactions].estimated_document_count()

        # fetch the requested slice, sorted by height desc
        cursor = (
            db[Collections.transactions]
            .find({})
            .sort("block_info.height", -1)
            .skip(skip)
            .limit(limit)
        )
        transactions = await cursor.to_list(length=limit)

    except Exception as e:
        # log e if you like, then:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving transactions: {e}",
        )

    return {
        "total_rows": total_docs,
        "transactions": transactions,
    }
