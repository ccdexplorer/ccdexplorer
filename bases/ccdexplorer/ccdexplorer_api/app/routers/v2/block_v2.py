"""FastAPI endpoints that expose block and payday data for version 2 of the API."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import MongoTypePoolReward, MongoTypeAccountReward
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    CCD_FinalizedBlockInfo,
    CCD_BlockSpecialEvent,
    CCD_ChainParameters,
    CCD_BlockItemSummary,
)
from ccdexplorer.mongodb import (
    MongoMotor,
    Collections,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from ccdexplorer.env import API_KEY_HEADER, TX_REQUEST_LIMIT_DISPLAY
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
import grpc
from ccdexplorer.ccdexplorer_api.app.state_getters import get_grpcclient, get_mongo_motor

router = APIRouter(tags=["Block"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)
apply_docstring_router_wrappers(router)


@router.get("/{net}/block/{height_or_hash}", response_class=JSONResponse)
async def get_block_at_height_from_grpc(
    request: Request,
    net: str,
    height_or_hash: int | str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_BlockInfo:
    """Retrieve a block directly from the node via gRPC.

    Args:
        request: FastAPI request used to read app limits (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height_or_hash: Either a block height or a block hash.
        grpcclient: Shared gRPC client dependency.
        api_key: API key extracted from the request headers.

    Returns:
        The block info as returned by the node.

    Raises:
        HTTPException: If the network is unsupported or the block cannot be found.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        height_or_hash = int(height_or_hash)
    except ValueError:
        pass
    try:
        result = grpcclient.get_block_info(height_or_hash, NET(net))
    except grpc._channel._InactiveRpcError:
        result = None
    except ValueError:
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested block at {height_or_hash} not found on {net}",
        )


@router.get(
    "/{net}/block/{height}/transactions/{skip}/{limit}/{sort_key}/{direction}",
    response_class=JSONResponse,
)
async def get_block_txs(
    request: Request,
    net: str,
    height: int,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """List transactions belonging to a particular block.

    Args:
        request: FastAPI request used to access per-request configuration limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height: Height of the block whose transactions are requested.
        skip: Number of transactions to skip for pagination.
        limit: Maximum number of transactions to return.
        sort_key: Field inside a transaction summary used for sorting.
        direction: Either ``asc`` or ``desc`` to influence the sort order.
        mongomotor: Mongo client dependency injected by FastAPI.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary containing the transactions plus metadata such as total count.

    Raises:
        HTTPException: If parameters are invalid, the block does not exist, or the
            transactions cannot be retrieved.
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
    block = await db_to_use[Collections.blocks].find_one({"height": height})
    if block:
        block = CCD_BlockInfo(**block)
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't find block at {height} on {net}",
        )
    try:
        pipeline = [{"$match": {"block_info.height": height}}]
        if sort_key:
            pipeline.append({"$sort": {sort_key: 1 if direction == "asc" else -1}})
        pipeline.extend([{"$skip": skip}, {"$limit": limit}])
        result = await await_await(db_to_use, Collections.transactions, pipeline, limit)
        # result = (
        #     await db_to_use[Collections.transactions]
        #     .find({"block_info.height": height})
        #     .sort({"index": 1})
        #     .skip(skip)
        #     .limit(limit)
        #     .to_list(limit)
        # )
        error = None
    except Exception as error:
        print(error)
        result = None

    if result is not None:
        tx_result = [CCD_BlockItemSummary(**x) for x in result]
        return {
            "transactions": tx_result,
            # "total_tx_count": total_tx_count if sequence_number < 1000 else sequence_number,
            "total_tx_count": block.transaction_count,
            "count_type": "ready_to_display",
            "tx_request_limit_display": TX_REQUEST_LIMIT_DISPLAY,
        }
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve transactions for block at {height} on {net}",
        )


@router.get(
    "/{net}/block/{height_or_hash}/payday",
    response_class=JSONResponse,
)
async def get_block_payday_true_false(
    request: Request,
    net: str,
    height_or_hash: int | str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Determine whether a block distributes payday rewards.

    Args:
        request: FastAPI request used to read app limits (unused but kept for parity).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height_or_hash: Either a block height or hash used to look up payday metadata.
        mongomotor: Mongo client dependency injected by FastAPI.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary indicating whether the block is a payday and, if so, how many
        account and pool rewards exist.

    Raises:
        HTTPException: If the network is unsupported or an error occurs while
            querying MongoDB.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        # if this doesn't fail, it's type int.
        height_or_hash = int(height_or_hash)
    except ValueError:
        pass
    db_to_use = mongomotor.mainnet
    try:
        if isinstance(height_or_hash, int):
            payday_result = await db_to_use[Collections.paydays_v2].find_one(
                {"height_for_last_block": height_or_hash - 1}
            )
        else:
            payday_result = await db_to_use[Collections.paydays_v2].find_one(
                {"_id": height_or_hash}
            )
        # if True, this block has payday rewards
        if payday_result:
            result_pool_rewards = await await_await(
                db_to_use,
                Collections.paydays_v2_rewards,
                [
                    {"$match": {"date": payday_result["date"]}},
                    {"$match": {"pool_owner": {"$exists": True}}},
                    {"$count": "count_of_pool_rewards"},
                ],
            )
            result_account_rewards = await await_await(
                db_to_use,
                Collections.paydays_v2_rewards,
                [
                    {"$match": {"date": payday_result["date"]}},
                    {"$match": {"account_id": {"$exists": True}}},
                    {"$count": "count_of_account_rewards"},
                ],
            )
            return {
                "is_payday": True,
                "count_of_account_rewards": result_account_rewards[0]["count_of_account_rewards"],
                "count_of_pool_rewards": result_pool_rewards[0]["count_of_pool_rewards"],
            }
        else:
            return {"is_payday": False}
    except Exception as error:
        print(error)
        raise HTTPException(
            status_code=404,
            detail=f"Can't determine whether block at {height_or_hash} on {net} is a payday.",
        )


@router.get(
    "/{net}/block/{height}/payday/pool-rewards/{skip}/{limit}/{sort_key}/{direction}",
    response_class=JSONResponse,
)
async def get_block_payday_pool_rewards(
    request: Request,
    net: str,
    height: int,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[MongoTypePoolReward]:
    """Return the pool rewards distributed for a payday block.

    Args:
        request: FastAPI request used to access per-request configuration limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height: Height of the block that closes the payday sequence.
        skip: Number of pool rewards to skip for pagination.
        limit: Maximum number of rewards to return.
        sort_key: Field used to sort the results.
        direction: Either ``asc`` or ``desc`` to select the sort direction.
        mongomotor: Mongo client dependency injected by FastAPI.
        api_key: API key extracted from the request headers.

    Returns:
        A list of pool reward models constrained by the pagination arguments.

    Raises:
        HTTPException: If parameters are invalid, the network is unsupported, or
            rewards cannot be retrieved.
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

    db_to_use = mongomotor.mainnet
    try:
        payday_result = await db_to_use[Collections.paydays_v2].find_one(
            {"height_for_last_block": height - 1}
        )
        # if True, this block has payday rewards
        if payday_result:
            pipeline = [
                {"$match": {"date": payday_result["date"]}},
                {"$match": {"pool_owner": {"$exists": True}}},
            ]
            if sort_key:
                pipeline.append({"$sort": {sort_key: 1 if direction == "asc" else -1}})
            pipeline.append(
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                        "data": [
                            {"$skip": int(skip)},
                            {"$limit": int(limit)},
                        ],
                    }
                },
            )
            result = await await_await(db_to_use, Collections.paydays_v2_rewards, pipeline)

        reward_result = [MongoTypePoolReward(**x) for x in result[0]["data"]]

        error = None
    except Exception as error:
        print(error)
        reward_result = None

    if reward_result is not None:
        return reward_result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve pool rewards for block at {height} on {net}",
        )


@router.get(
    "/{net}/block/{height}/payday/account-rewards/{skip}/{limit}/{sort_key}/{direction}",
    response_class=JSONResponse,
)
async def get_block_payday_account_rewards(
    request: Request,
    net: str,
    height: int,
    skip: int,
    limit: int,
    sort_key: str,
    direction: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[MongoTypeAccountReward]:
    """Return the account rewards distributed for a payday block.

    Args:
        request: FastAPI request used to access per-request configuration limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height: Height of the block that closes the payday sequence.
        skip: Number of account rewards to skip for pagination.
        limit: Maximum number of rewards to return.
        sort_key: Field used to sort the results.
        direction: Either ``asc`` or ``desc`` to select the sort direction.
        mongomotor: Mongo client dependency injected by FastAPI.
        api_key: API key extracted from the request headers.

    Returns:
        A list of account reward models constrained by the pagination arguments.

    Raises:
        HTTPException: If parameters are invalid, the network is unsupported, or
            rewards cannot be retrieved.
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

    db_to_use = mongomotor.mainnet
    try:
        payday_result = await db_to_use[Collections.paydays_v2].find_one(
            {"height_for_last_block": height - 1}
        )
        # if True, this block has payday rewards
        if payday_result:
            pipeline = [
                {"$match": {"date": payday_result["date"]}},
                {"$match": {"account_id": {"$exists": True}}},
            ]
            if sort_key:
                pipeline.append({"$sort": {sort_key: 1 if direction == "asc" else -1}})
            pipeline.append(
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                        "data": [
                            {"$skip": int(skip)},
                            {"$limit": int(limit)},
                        ],
                    }
                }
            )

            result = await await_await(db_to_use, Collections.paydays_v2_rewards, pipeline)

        reward_result = [MongoTypeAccountReward(**x) for x in result[0]["data"]]

        error = None
    except Exception as error:
        print(error)
        reward_result = None

    if reward_result is not None:
        return reward_result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve account rewards for block at {height} on {net}",
        )


@router.get("/{net}/block/{height}/special-events", response_class=JSONResponse)
async def get_block_special_events(
    request: Request,
    net: str,
    height: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_BlockSpecialEvent]:
    """Fetch special events emitted by the node for a given block.

    Args:
        request: FastAPI request used to read app limits (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height: Height of the block whose special events are requested.
        grpcclient: Shared gRPC client dependency.
        api_key: API key extracted from the request headers.

    Returns:
        A list of special event descriptions.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    special_events = grpcclient.get_block_special_events(height, net=NET(net))

    return special_events
    # else:
    #     raise HTTPException(
    #         status_code=404,
    #         detail=f"Can't retrieve special events for block at {height} on {net}",
    #     )


@router.get("/{net}/block/{height}/chain-parameters", response_class=JSONResponse)
async def get_block_chain_parameters(
    request: Request,
    net: str,
    height: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_ChainParameters:
    """Return the chain parameters that applied at the given block height.

    Args:
        request: FastAPI request used to read app limits (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        height: Height of the block whose chain parameters are requested.
        grpcclient: Shared gRPC client dependency.
        api_key: API key extracted from the request headers.

    Returns:
        The chain parameters deserialized from the gRPC response.

    Raises:
        HTTPException: If the network is unsupported or the parameters cannot be found.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    chain_parameters = grpcclient.get_block_chain_parameters(height, net=NET(net))

    if chain_parameters:
        return chain_parameters
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve chain parameters for block at {height} on {net}",
        )


@router.get("/{net}/block/height/finalized", response_class=JSONResponse)
async def get_last_finalized_block(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_FinalizedBlockInfo:
    """Retrieve the latest finalized block from the node.

    Args:
        request: FastAPI request used to read app limits (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        grpcclient: Shared gRPC client dependency.
        api_key: API key extracted from the request headers.

    Returns:
        The most recently finalized block info returned by the node.

    Raises:
        HTTPException: If the network is unsupported or the data cannot be obtained.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    result = grpcclient.get_finalized_blocks()
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve last finalized block for {net}",
        )
