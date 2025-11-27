"""Routes exposing aggregated token metadata across the Concordium networks."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from ccdexplorer.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
)
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor, get_exchange_rates
import math
from typing import Optional
from pydantic import BaseModel
import re

router = APIRouter(tags=["Tokens"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)
apply_docstring_router_wrappers(router)


class FungibleToken(BaseModel):
    decimals: Optional[int] = None
    token_symbol: Optional[str] = None
    token_value: Optional[float] = None
    token_value_USD: Optional[float] = None
    verified_information: Optional[dict] = None
    address_information: Optional[dict] = None


class NonFungibleToken(BaseModel):
    verified_information: Optional[dict] = None


@router.get("/{net}/tokens/info/count", response_class=JSONResponse)
async def get_tokens_count_estimate(
    request: Request,
    net: str,
    mongomotor: MongoDB = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """Return the approximate number of known token addresses.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to access token collections.
        api_key: API key extracted from the request headers.

    Returns:
        Estimated document count for ``tokens_token_addresses_v2``.

    Raises:
        HTTPException: If the network is unsupported or the estimate cannot be retrieved.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        result = await db_to_use[Collections.tokens_token_addresses_v2].estimated_document_count()
        error = None
    except Exception as error:
        print(error)
        result = None

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error retrieving tokens count on {net}, {error}.",
        )


@router.get(
    "/{net}/tokens/fungible-tokens/verified",
    response_class=JSONResponse,
)
async def get_fungible_tokens_verified(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """List verified fungible CIS-2 tokens together with USD estimates.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to access token metadata.
        exchange_rates: Injected exchange-rate cache keyed by ticker.
        api_key: API key extracted from the request headers.

    Returns:
        A list of ``FungibleToken`` models enriched with metadata and fiat valuations.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    pipeline = [
        {
            "$match": {
                "token_type": "fungible",
                "$or": [{"hidden": {"$exists": False}}, {"hidden": False}],
            }
        }
    ]
    fungible_tokens = await await_await(db_to_use, Collections.tokens_tags, pipeline)

    # add verified information and metadata and USD value
    fungible_result = []
    for token in fungible_tokens:
        fungible_token = FungibleToken()
        result = await db_to_use[Collections.tokens_token_addresses_v2].find_one(
            {"_id": token["related_token_address"]}
        )
        fungible_token.address_information = result
        fungible_token.verified_information = token

        fungible_token.token_symbol = fungible_token.verified_information.get("get_price_from")
        fungible_token.token_value_USD = 0
        if fungible_token.token_symbol:
            fungible_token.decimals = fungible_token.verified_information["decimals"]
            if fungible_token.address_information:
                fungible_token.token_value = int(
                    fungible_token.address_information.get("token_amount")
                ) * (math.pow(10, -fungible_token.decimals))

                if fungible_token.token_symbol in exchange_rates:
                    fungible_token.token_value_USD = (
                        fungible_token.token_value
                        * exchange_rates[fungible_token.token_symbol]["rate"]
                    )
                else:
                    fungible_token.token_value_USD = 0
        fungible_result.append(fungible_token)

    return fungible_result


@router.get(
    "/{net}/tokens/non-fungible-tokens/verified",
    response_class=JSONResponse,
)
async def get_non_fungible_tokens_verified(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    """List verified NFT collections for the requested network.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongomotor: Mongo client dependency used to access token metadata.
        exchange_rates: Injected exchange-rate cache (unused but kept for parity).
        api_key: API key extracted from the request headers.

    Returns:
        A list of ``NonFungibleToken`` models containing the verified metadata.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    pipeline = [
        {
            "$match": {
                "token_type": "non-fungible",
                "$or": [{"hidden": {"$exists": False}}, {"hidden": False}],
            }
        }
    ]
    non_fungible_tokens = await await_await(db_to_use, Collections.tokens_tags, pipeline)

    non_fungible_result = []
    for token in non_fungible_tokens:
        non_fungible_token = NonFungibleToken()
        non_fungible_token.verified_information = token

        non_fungible_result.append(non_fungible_token)

    return non_fungible_result


@router.get("/{net}/tokens/search/{value}", response_class=JSONResponse)
async def search_tokens(
    request: Request,
    net: str,
    value: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """Perform a case-insensitive search over verified token display names.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        value: Partial display name to match.
        mongomotor: Mongo client dependency used to access token metadata.
        api_key: API key extracted from the request headers.

    Returns:
        Tokens whose display name matches the pattern.

    Raises:
        HTTPException: If the network is unsupported.
    """
    search_str = str(value).lower()
    regex = re.compile(search_str)

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        {"$match": {"$or": [{"hidden": False}, {"hidden": {"$exists": False}}]}},
        {"$addFields": {"display_name_lc": {"$toLower": "$display_name"}}},
        {"$match": {"display_name_lc": {"$regex": regex}}},
    ]
    result = await await_await(db_to_use, Collections.tokens_tags, pipeline)
    return result
