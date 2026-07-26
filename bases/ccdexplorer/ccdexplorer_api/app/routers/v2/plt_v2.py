"""Routes for Protocol-Level Token (PLT) data such as holders and activity."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
import datetime as dt
import grpc
from ccdexplorer.mongodb import Collections, MongoMotor
from ccdexplorer.domain.generic import NET
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor, get_grpcclient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_TokenInfo,
    CCD_BlockItemSummary,
    CCD_LockId,
)
from pymongo import ASCENDING, DESCENDING

router = APIRouter(tags=["Protocol-Level Token"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)


@router.get("/{net}/plt/{token_id}/info", response_class=JSONResponse)
async def get_plt_token_info(
    request: Request,
    net: str,
    token_id: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return live token metadata for a PLT token.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        token_id: Identifier of the PLT token.
        grpcclient: gRPC client dependency used to query token info.
        mongomotor: Mongo client dependency used to enrich with tag info.
        api_key: API key extracted from the request headers.

    Returns:
        Token info merged with optional metadata stored in MongoDB.

    Raises:
        HTTPException: If the network is unsupported or the token is missing.
    """
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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
    """Return historical PLT token info from a specific block hash.

    Currently not in use.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        token_id: Identifier of the PLT token.
        block_hash: Block hash to query.
        grpcclient: gRPC client dependency used to fetch token info.
        mongomotor: Mongo client dependency (unused but kept for parity).
        api_key: API key extracted from the request headers.

    Returns:
        ``CCD_TokenInfo`` at the requested block.

    Raises:
        HTTPException: If the network is unsupported or the token is missing.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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
    """List PLT token holders ordered by balance.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        token_id: Identifier of the PLT token.
        skip: Number of holders to skip.
        limit: Maximum number of holders to return.
        mongomotor: Mongo client dependency used to query ``plts_links``.
        grpcclient: gRPC client dependency (unused but kept for parity).
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary with holder entries and the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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
    "/{net}/plt/locks/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_locks(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    status: str | None = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """List all PLT locks system-wide, most recently updated first.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        skip: Number of locks to skip.
        limit: Maximum number of locks to return.
        status: Optional filter, one of ``open``, ``expired``, ``cancelled``. ``expired`` is
            derived (an "open" lock whose expiry has passed) since the indexer only ever
            stores ``open``/``cancelled`` based on destroy events.
        mongomotor: Mongo client dependency used to query ``plts_locks``.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary with lock entries and the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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

    now = dt.datetime.now(dt.timezone.utc)
    if status == "open":
        match = {"status": "open", "expiry": {"$gte": now}}
    elif status == "expired":
        match = {"status": "open", "expiry": {"$lt": now}}
    elif status == "cancelled":
        match = {"status": "cancelled"}
    else:
        match = {}

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        total_count = await db_to_use[Collections.plts_locks].count_documents(match)
        pipeline = [
            {"$match": match},
            {"$sort": {"last_updated_block_height": DESCENDING}},
            {"$skip": skip},
            {"$limit": limit},
        ]
        locks = await await_await(db_to_use, Collections.plts_locks, pipeline, limit)

        return {
            "data": locks,
            "total_row_count": total_count,
        }

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve locks on {net}. {error}",
        )


@router.get(
    "/{net}/plt/{token_id}/locks/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_token_locks(
    request: Request,
    net: str,
    token_id: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """List PLT locks that support this token, most recently updated first.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        token_id: Identifier of the PLT token.
        skip: Number of locks to skip.
        limit: Maximum number of locks to return.
        mongomotor: Mongo client dependency used to query ``plts_locks``.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary with lock entries and the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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
        total_count = await db_to_use[Collections.plts_locks].count_documents(
            {"token_ids": token_id}
        )
        pipeline = [
            {"$match": {"token_ids": token_id}},
            {"$sort": {"last_updated_block_height": DESCENDING}},
            {"$skip": skip},
            {"$limit": limit},
        ]
        locks = await await_await(db_to_use, Collections.plts_locks, pipeline, limit)

        return {
            "data": locks,
            "total_row_count": total_count,
        }

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve locks for PLT token {token_id} on {net}. {error}",
        )


@router.get("/{net}/plt/lock/{account_index}/{sequence_number}/{creation_order}", response_class=JSONResponse)
async def get_lock_detail(
    request: Request,
    net: str,
    account_index: int,
    sequence_number: int,
    creation_order: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return current lock state, merged with indexed status.

    Lock state (recipients/controller/funds) is read live from the node, since it can change
    on every touching transaction. ``status`` only exists in the indexed ``plts_locks``
    document. The lock's transaction history is served separately (paginated) by
    ``get_paginated_lock_transactions``.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        account_index: The account index that created the lock.
        sequence_number: The sequence number of the creating transaction.
        creation_order: The 0-based creation order of the lock within that transaction.
        grpcclient: gRPC client dependency used to fetch live lock info.
        mongomotor: Mongo client dependency used to fetch indexed lock metadata.
        api_key: API key extracted from the request headers.

    Returns:
        Decoded lock state merged with ``status`` and ``touching_txs``.

    Raises:
        HTTPException: If the network is unsupported or the lock is not found.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    lock_id = CCD_LockId(
        account_index=account_index,
        sequence_number=sequence_number,
        creation_order=creation_order,
    )
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    mongo_doc = await db_to_use[Collections.plts_locks].find_one({"_id": lock_id.to_str()})

    try:
        lock_info = grpcclient.get_lock_info("last_final", lock_id, net=NET(net))
        decoded = grpcclient.decode_lock_info(lock_info.lock_info).model_dump(exclude_none=True)
    except grpc._channel._InactiveRpcError:  # type: ignore
        decoded = None

    if not decoded and not mongo_doc:
        raise HTTPException(
            status_code=404,
            detail=f"Lock {lock_id.to_str()} not found on {net}.",
        )

    return {
        **(decoded or {}),
        "status": mongo_doc.get("status") if mongo_doc else "unknown",
    }


@router.get(
    "/{net}/plt/lock/{account_index}/{sequence_number}/{creation_order}/transactions/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_lock_transactions(
    request: Request,
    net: str,
    account_index: int,
    sequence_number: int,
    creation_order: int,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """List transactions that have touched a lock, most recent first.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        account_index: The account index that created the lock.
        sequence_number: The sequence number of the creating transaction.
        creation_order: The 0-based creation order of the lock within that transaction.
        skip: Number of transactions to skip.
        limit: Maximum number of transactions to return.
        mongomotor: Mongo client dependency used to query ``plts_locks``.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary with transaction entries and the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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

    lock_id_str = CCD_LockId(
        account_index=account_index,
        sequence_number=sequence_number,
        creation_order=creation_order,
    ).to_str()
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        count_pipeline = [
            {"$match": {"_id": lock_id_str}},
            {"$project": {"total": {"$size": {"$ifNull": ["$touching_txs", []]}}}},
        ]
        count_result = await await_await(db_to_use, Collections.plts_locks, count_pipeline, 1)
        total_count = count_result[0]["total"] if count_result else 0

        pipeline = [
            {"$match": {"_id": lock_id_str}},
            {"$project": {"touching_txs": 1}},
            {"$unwind": "$touching_txs"},
            {"$sort": {"touching_txs.block_height": DESCENDING}},
            {"$skip": skip},
            {"$limit": limit},
            {"$replaceRoot": {"newRoot": "$touching_txs"}},
        ]
        touching_txs = await await_await(db_to_use, Collections.plts_locks, pipeline, limit)

        return {
            "data": touching_txs,
            "total_row_count": total_count,
        }

    except Exception as error:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve transactions for lock {lock_id_str} on {net}. {error}",
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
    """Return PLT token transfer activity for a specific token id.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        token_id: Identifier of the PLT token.
        skip: Number of records to skip.
        limit: Maximum number of transactions to return.
        sort_key: Field inside transaction docs used for sorting.
        direction: Sort order, ``asc`` or ``desc``.
        mongomotor: Mongo client dependency used to query impacts and transactions.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary containing the serialized transactions plus the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=422,
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
