"""Routes for listing and aggregating Protocol-Level Tokens (PLTs)."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.ccdexplorer_api.app.utils import apply_docstring_router_wrappers
import dateutil
import grpc
from grpc._channel import _MultiThreadedRendezvous
from ccdexplorer.mongodb import Collections, MongoMotor
from ccdexplorer.domain.generic import NET
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from fastapi.security.api_key import APIKeyHeader
import pandas as pd
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_httpx_client,
    get_mongo_motor,
    get_grpcclient,
)
from ccdexplorer.grpc_client.CCD_Types import CCD_TokenId
import httpx
from calendar import monthrange

router = APIRouter(tags=["Protocol-Level Tokens"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)


@router.get("/{net}/plt/list-token-ids", response_class=JSONResponse)
async def get_all_plt_tokens_from_node(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_TokenId]:
    """Return the list of PLT token ids that existed at a given block hash.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        block_hash: Block hash to query.
        grpcclient: gRPC client dependency used to call the node.
        mongomotor: Mongo client dependency (unused but kept for parity).
        api_key: API key extracted from the request headers.

    Returns:
        Token ids reported by the node for that block.

    Raises:
        HTTPException: If the network is unsupported or the node call fails.
    """
    """Return the list of PLT token IDs directly from the node.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        grpcclient: gRPC client dependency used to call the node.
        mongomotor: Mongo client dependency (unused but kept for parity).
        api_key: API key extracted from the request headers.

    Returns:
        A list of ``CCD_TokenId`` entries.

    Raises:
        HTTPException: If the network is unsupported or the node call fails.
    """
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
    """Return the cached metadata for all PLT tokens.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        grpcclient: gRPC client dependency (unused but kept for parity).
        mongomotor: Mongo client dependency used to read ``plts_tags``.
        api_key: API key extracted from the request headers.

    Returns:
        Dictionary keyed by token id containing the stored metadata.

    Raises:
        HTTPException: If the network is unsupported or no data is available.
    """
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


@router.get("/{net}/plt/statistics-overview/{year}/{month}/{day}", response_class=JSONResponse)
@router.get("/{net}/plt/statistics-overview/{year}/{month}", response_class=JSONResponse)
async def get_all_plt_statistics_per_day_or_month(
    request: Request,
    net: str,
    year: int,
    month: int,
    day: int | None = None,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    api_key: str = Security(API_KEY_HEADER),
):
    """Aggregate PLT statistics for a given day or month.

    Args:
        request: FastAPI request context providing the internal API URL.
        net: Network identifier (only ``mainnet`` supported).
        year: Year component of the range.
        month: Month component of the range.
        day: Optional day; if omitted, the full month is summarized.
        grpcclient: gRPC client dependency (unused but kept for parity).
        httpx_client: Shared HTTP client used to query the misc statistics endpoint.
        api_key: API key extracted from the request headers.

    Returns:
        A summary dictionary containing aggregate transaction counts and USD volumes.

    Raises:
        HTTPException: If the network is unsupported, the date is invalid, or no data exists.
    """
    if net != "mainnet":
        raise HTTPException(
            status_code=404,
            detail="Only available on mainnet.",
        )
    analysis = "statistics_plt"
    if day is not None:
        try:
            _ = dateutil.parser.parse(f"{year:04d}-{month:02d}-{day:02d}")
            start_date = f"{year:04d}-{month:02d}-{day:02d}"
            end_date = f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            raise HTTPException(
                status_code=404,
                detail=f"Date {year:04d}-{month:02d}-{day:02d} is invalid.",
            )

    else:
        start_date = f"{year:04d}-{month:02d}-01"
        # find valid end date for month
        last_day = monthrange(year, month)[1]
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"

    response = await httpx_client.get(
        f"{request.app.api_url}/v2/mainnet/misc/statistics/{analysis}/{start_date}/{end_date}"
    )
    all_data = response.json()

    if not all_data:
        raise HTTPException(
            status_code=404,
            detail=f"No data for {start_date}" + (f" to {end_date}" if day is None else ""),
        )
    df = pd.json_normalize(all_data)
    count_tx_cols = [x for x in df.columns if "count_txs" in x]
    transfer_cols = [x for x in df.columns if "USD.transfer" in x]
    total_supply_cols = [x for x in df.columns if "USD.total_supply" in x]
    df["total_count_txs"] = df[count_tx_cols].sum(axis=1).astype(int)
    df["total_transfer_in_USD"] = df[transfer_cols].sum(axis=1)
    df["total_supply_in_USD"] = df[total_supply_cols].sum(axis=1)
    if day is not None:
        summary = {"date": start_date}
    else:
        summary = {
            "month": f"{year:04d}-{month:02d}",
            "start_date": start_date,
            "end_date": end_date,
        }
    summary = {
        **summary,
        "total_count_txs": int(df["total_count_txs"].sum()),
        "total_transfer_in_USD": float(df["total_transfer_in_USD"].sum()),
        "avg_total_supply_in_USD": float(df["total_supply_in_USD"].mean()),
    }

    return summary
