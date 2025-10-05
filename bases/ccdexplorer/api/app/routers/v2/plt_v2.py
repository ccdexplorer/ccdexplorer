# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.api.app.utils import await_await
import grpc
from ccdexplorer.mongodb import Collections, MongoMotor
from ccdexplorer.domain.generic import NET
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.api.app.state_getters import get_mongo_motor, get_grpcclient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_TokenInfo,
    CCD_BlockItemSummary,
)
from pymongo import ASCENDING, DESCENDING

router = APIRouter(tags=["Protocol-Level Token"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get("/{net}/plt/{token_id}/info", response_class=JSONResponse)
async def get_plt_token_info(
    request: Request,
    net: str,
    token_id: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_token_info("last_final", token_id=token_id, net=NET(net))
        # result = CCD_TokenInfo(**result)  # type: ignore
        result.tag_information = await db_to_use[Collections.plts_tags].find_one(  # type: ignore
            {"_id": token_id}
        )

    except grpc._channel._InactiveRpcError:  # type: ignore
        result = None

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Token {token_id} not found on {net}.",
        )
    else:
        return result.model_dump(exclude_none=True)


@router.get("/{net}/plt/{token_id}/info-at-block/{block_hash}", response_class=JSONResponse)
async def get_plt_token_info_at_block(
    request: Request,
    net: str,
    token_id: str,
    block_hash: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_TokenInfo:
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_token_info(block_hash, token_id=token_id, net=NET(net))
    except grpc._channel._InactiveRpcError:  # type: ignore
        result = None

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Token {token_id} not found on {net}.",
        )
    else:
        return result


@router.get(
    "/{net}/plt/{token_id}/holders/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_token_current_holders(
    request: Request,
    net: str,
    token_id: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get current token holders for PLT token.
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
        total_count = await db_to_use[Collections.plts_links].count_documents(
            {"token_id": token_id}
        )
        pipeline = [
            {"$match": {"token_id": token_id}},
            {"$addFields": {"balance_num": {"$toLong": "$balance"}}},
            {"$sort": {"balance_num": -1}},
            {"$skip": skip},
            {"$limit": limit},
        ]
        current_holders = await await_await(db_to_use, Collections.plts_links, pipeline, limit)

        return {
            "data": current_holders,
            "total_row_count": total_count,
        }

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve current holders for PLT token {token_id} on {net}. {error}",
        )


@router.get(
    "/{net}/plt/{token_id}/transactions/{skip}/{limit}/{sort_key}/{direction}",
    response_class=JSONResponse,
)
async def get_paginated_plt_transactions(
    request: Request,
    net: str,
    token_id: str,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all plt transactions.
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

    base_filter = {"plt_token_id": token_id}
    # count unique hashes
    count_pipeline = [
        {"$match": base_filter},
        {"$group": {"_id": "$tx_hash"}},
        {"$count": "total"},
    ]

    count_result = await await_await(db_to_use, Collections.impacted_addresses, count_pipeline, 1)

    total_tx_count = count_result[0]["total"] if count_result else 0

    # fetch page
    sort_field = sort_key or "block_height"
    sort_direction = 1 if direction == "asc" else -1

    pipeline = [
        {"$match": base_filter},
        {"$sort": {sort_field: sort_direction}},
        {"$project": {"_id": 0, "tx_hash": 1}},
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
    pipeline = [
        {"$match": {"_id": {"$in": [x["tx_hash"] for x in all_txs_hashes]}}},
        {"$sort": {"block_info.height": 1 if direction == "asc" else -1}},
        {"$limit": limit},
    ]
    int_result = await await_await(db_to_use, Collections.transactions, pipeline)
    tx_result = [CCD_BlockItemSummary(**x).model_dump(exclude_none=True) for x in int_result]
    return {"transactions": tx_result, "total_tx_count": total_tx_count}
