"""FastAPI routes for transaction-level queries in the v2 API."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from pymongo import ASCENDING
from ccdexplorer.mongodb import (
    MongoDB,
    Collections,
)
from ccdexplorer.domain.mongo import MongoTypeLoggedEventV2
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_db
from ccdexplorer.ccdexplorer_api.app.utils import apply_docstring_router_wrappers


router = APIRouter(tags=["Transaction"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)


@router.get("/{net}/transaction/{tx_hash}/logged-events", response_class=JSONResponse)
async def get_transaction_logged_events(
    request: Request,
    net: str,
    tx_hash: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Fetch the logged events emitted by a specific transaction.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        tx_hash: Hash of the transaction to inspect.
        mongodb: Mongo client dependency used to query ``tokens_logged_events_v2``.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary keyed by ``(effect_index, event_index)`` mapping to logged event models.

    Raises:
        HTTPException: If the network is unsupported or the transaction hash is unknown.
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
    """Return the raw transaction summary from MongoDB.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        tx_hash: Hash of the transaction to fetch.
        mongodb: Mongo client dependency used to query the ``transactions`` collection.
        api_key: API key extracted from the request headers.

    Returns:
        The transaction serialized as ``CCD_BlockItemSummary``.

    Raises:
        HTTPException: If the network is unsupported or the transaction hash is unknown.
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
