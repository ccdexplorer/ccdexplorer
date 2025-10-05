# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from ccdexplorer.mongodb import (
    Collections,
    MongoMotor,
)
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType  # noqa
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse

from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.api.app.state_getters import get_mongo_motor

router = APIRouter(tags=["Blocks"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get("/{net}/blocks/last/{limit}", response_class=JSONResponse)
async def get_last_blocks(
    request: Request,
    net: str,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_BlockInfo]:
    """
    Endpoint to get the last X blocks as stored in MongoDB collection `blocks`. Maxes out at 50.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    limit = min(50, max(limit, 1))
    error = None
    try:
        result = await db_to_use[Collections.blocks].find({}).sort({"height": -1}).to_list(limit)

    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving last {limit} blocks on {net}, {error}.",
        )


@router.get(
    "/{net}/blocks/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_blocks(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to page through the `blocks` collection using skip/limit.
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
        total_docs = await db[Collections.blocks].estimated_document_count()

        # fetch the requested slice, sorted by height desc
        cursor = db[Collections.blocks].find({}).sort("height", -1).skip(skip).limit(limit)
        blocks = await cursor.to_list(length=limit)

    except Exception as e:
        # log e if you like, then:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving blocks: {e}",
        )

    return {
        "total_rows": total_docs,
        "blocks": blocks,
    }


@router.get("/{net}/blocks/newer/than/{since}", response_class=JSONResponse)
async def get_last_blocks_newer_than(
    request: Request,
    net: str,
    since: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_BlockInfo]:
    """
    Endpoint to get the last X blocks that are newer than `since` as stored in MongoDB collection `blocks`.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = (
            await db_to_use[Collections.blocks]
            .find({"height": {"$gt": since}})
            .sort({"height": -1})
            .to_list(length=min(since, 1000))
        )

    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving blocks on {net} newer than {since}.",
        )
