"""Routes exposing cached market data sourced from third parties."""

from ccdexplorer.ccdexplorer_api.app.utils import apply_docstring_router_wrappers
from ccdexplorer.mongodb import (
    Collections,
    MongoMotor,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor

router = APIRouter(tags=["Markets"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)


@router.get(
    "/markets/info",
    response_class=JSONResponse,
)
async def get_markets_info(
    request: Request,
    api_key: str = Security(API_KEY_HEADER),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
) -> dict:
    """Return cached Concordium market metrics obtained from CoinMarketCap.

    Args:
        request: FastAPI request context (unused but required).
        api_key: API key extracted from the request headers.
        mongomotor: Mongo client dependency used to read helper documents.

    Returns:
        The ``coinmarketcap_data`` helper record.

    Raises:
        HTTPException: If the market data cannot be found or retrieved.
    """
    db_to_use = mongomotor.mainnet
    try:
        result = await db_to_use[Collections.helpers].find_one({"_id": "coinmarketcap_data"})
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail="Error retrieving markets information for CCD.",
        )
