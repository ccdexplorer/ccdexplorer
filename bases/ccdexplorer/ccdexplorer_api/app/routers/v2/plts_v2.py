# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import dateutil
import grpc
from grpc._channel import _MultiThreadedRendezvous
from ccdexplorer.mongodb import Collections, MongoMotor
from ccdexplorer.domain.generic import NET
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.env import API_KEY_HEADER
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
