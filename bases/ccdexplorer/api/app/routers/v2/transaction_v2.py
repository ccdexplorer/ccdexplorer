# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from pymongo import ASCENDING
from ccdexplorer.mongodb import (
    MongoDB,
    Collections,
)
from ccdexplorer.domain.mongo import MongoTypeLoggedEventV2
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.api.app.state_getters import get_mongo_db


router = APIRouter(tags=["Transaction"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get("/{net}/transaction/{tx_hash}/logged-events", response_class=JSONResponse)
async def get_transaction_logged_events(
    request: Request,
    net: str,
    tx_hash: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Get logged events for a transaction from the MongoDB `tokens_logged_events_v2` collection.

    Parameters:
    - net (str): Network type, either "testnet" or "mainnet".
    - tx_hash (str): The transaction hash to look up.
    - mongodb (MongoDB, optional): MongoDB dependency, defaults to `get_mongo_db`.

    Returns:
    - list[MongoTypeLoggedEventV2]: A list of logged events for the specified transaction.

    Raises:
    - HTTPException: If the transaction hash is not found in the specified network.

    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    pipeline = [
        {"$match": {"tx_info.tx_hash": tx_hash}},
    ]
    result = list(db_to_use[Collections.tokens_logged_events_v2].aggregate(pipeline))

    if len(result) > 0:
        result = {
            (
                x["event_info"]["effect_index"],
                x["event_info"]["event_index"],
            ): MongoTypeLoggedEventV2(**x)
            for x in result
        }

        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested transaction hash ({tx_hash}) not found on {net}",
        )


@router.get("/{net}/transaction/{tx_hash}", response_class=JSONResponse)
async def get_transaction(
    request: Request,
    net: str,
    tx_hash: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_BlockItemSummary:
    """
    Retrieve a transaction from the MongoDB `transactions` collection.

    Parameters:
    - net (str): Network type, either "testnet" or "mainnet".
    - tx_hash (str): The transaction hash to look up.
    - mongodb (MongoDB, optional): MongoDB dependency, defaults to `get_mongo_db`.

    Returns:
    - CCD_BlockItemSummary: The transaction summary from the MongoDB collection.

    Raises:
    - HTTPException: If the transaction hash is not found in the specified network.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    result = db_to_use[Collections.transactions].find_one(tx_hash)
    if result:
        result = CCD_BlockItemSummary(**result)
        return result  # .model_dump_json(exclude="_id", exclude_none=True)
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested transaction hash ({tx_hash}) not found on {net}",
        )
