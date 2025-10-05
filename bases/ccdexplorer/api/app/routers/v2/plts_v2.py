# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import grpc
from ccdexplorer.mongodb import Collections, MongoMotor
from ccdexplorer.domain.generic import NET
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.api.app.state_getters import get_mongo_motor, get_grpcclient
from ccdexplorer.grpc_client.CCD_Types import CCD_TokenId

router = APIRouter(tags=["Protocol-Level Tokens"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get("/{net}/plt/list-token-ids", response_class=JSONResponse)
async def get_all_plt_tokens_from_node(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_TokenId]:
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_token_list("last_final", net=NET(net))
    except grpc._channel._InactiveRpcError | _MultiThreadedRendezvous:  # type: ignore
        result = None

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Can't list all tokens on {net}.",
        )
    else:
        return result


@router.get("/{net}/plts/overview", response_class=JSONResponse)
async def get_all_plt_tokens(
    request: Request,
    net: str,
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

    result = await db_to_use[Collections.plts_tags].find({}).to_list(length=None)
    result = {x["_id"]: x for x in result}
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Can't list tokens on {net}.",
        )
    else:
        return result


@router.get("/{net}/plt/overview-at-block/{block_hash}", response_class=JSONResponse)
async def get_all_plt_tokens_at_block(
    request: Request,
    net: str,
    block_hash: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_TokenId]:
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        result = grpcclient.get_token_list(block_hash, net=NET(net))
    except grpc._channel._InactiveRpcError:  # type: ignore
        result = None

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Can't list all tokens on {net}.",
        )
    else:
        return result
