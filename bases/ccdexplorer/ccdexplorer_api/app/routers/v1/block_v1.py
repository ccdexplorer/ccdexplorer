from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    CCD_FinalizedBlockInfo,
)
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from ccdexplorer.ccdexplorer_api.app.state_getters import get_grpcclient

router = APIRouter(tags=["Block"], prefix="/v1")


@router.get("/{net}/block/{height_or_hash}", response_class=JSONResponse)
async def get_block_at_height_from_grpc(
    request: Request,
    net: str,
    height_or_hash: int | str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
) -> CCD_BlockInfo:
    """
    Endpoint to get blockInfo from the node.
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
    result = grpcclient.get_block_info(height_or_hash, NET(net))
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested block at {height_or_hash} not found on {net}",
        )


@router.get("/{net}/block/height/finalized", response_class=JSONResponse)
async def get_last_finalized_block(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
) -> CCD_FinalizedBlockInfo:
    """
    Endpoint to get the last block from the node.
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
