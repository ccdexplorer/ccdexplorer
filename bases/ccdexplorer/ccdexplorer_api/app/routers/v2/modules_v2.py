"""Routes exposing module summaries and search utilities."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import re
from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
from ccdexplorer.mongodb import Collections, MongoMotor
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse

from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor

router = APIRouter(tags=["Modules"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)
apply_docstring_router_wrappers(router)


@router.get("/{net}/modules/overview", response_class=JSONResponse)
async def get_overview_of_all_modules(
    request: Request,
    net: str,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return the latest monthly overview statistics for every module.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongodb: Mongo client dependency used to read the ``statistics`` collection.
        api_key: API key extracted from the request headers.

    Returns:
        Dictionary keyed by ``year_month`` containing module overview rows.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    modules_overview = (
        await db_to_use[Collections.statistics]
        .find({"type": "statistics_modules_overview"})
        .sort({"date": -1})
        .to_list(length=None)
    )

    return {x["year_month"]: x for x in modules_overview}


@router.get("/{net}/modules/search/{value}", response_class=JSONResponse)
async def search_modules(
    request: Request,
    net: str,
    value: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    """Perform a case-insensitive search across module ids and names.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        value: Search pattern to match.
        mongomotor: Mongo client dependency used to query ``modules``.
        api_key: API key extracted from the request headers.

    Returns:
        Up to ten modules matching the search string.

    Raises:
        HTTPException: If the network is unsupported.
    """
    search_str = str(value)
    regex = re.compile(search_str, re.IGNORECASE)
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        {
            "$match": {
                "$or": [
                    {"_id": {"$regex": regex}},
                    {"module_name": {"$regex": regex}},
                ]
            }
        },
    ]
    result = await await_await(db_to_use, Collections.modules, pipeline, 10)
    return result


# @router.get("/{net}/modules/{year}/{month}", response_class=JSONResponse)
# async def get_all_modules(
#     request: Request,
#     net: str,
#     year: int,
#     month: int,
#     mongomotor: MongoMotor = Depends(get_mongo_motor),
#     api_key: str = Security(API_KEY_HEADER),
# ) -> list[CCD_BlockItemSummary]:
#     """
#     Endpoint to get all modules on net.

#     """

#     db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
#     error = None
#     try:
#         start_date = dt.datetime(year, month, 1)
#         end_date = dt.datetime(year + (month // 12), (month % 12) + 1, 1)

#         # # If it's December, the next month will be January of the next year
#         # if month == 12:
#         #     end_date = dt.datetime(year + 1, 1, 1)
#         # else:
#         #     end_date = dt.datetime(year, month + 1, 1)

#         # Query to match "module_deployed" and filter by `slot_time` in the specified month
#         pipeline = [
#             # Match documents where "type.contents" is "module_deployed"
#             {
#                 "$match": {
#                     "$expr": {
#                         "$and": [
#                             {"$eq": ["$type.contents", "module_deployed"]},
#                             {
#                                 "$eq": [
#                                     {"$year": {"$toDate": "$block_info.slot_time"}},
#                                     year,
#                                 ]
#                             },
#                             {
#                                 "$eq": [
#                                     {"$month": {"$toDate": "$block_info.slot_time"}},
#                                     month,
#                                 ]
#                             },
#                         ]
#                     }
#                 }
#             },
#             {"$sort": {"block_info.slot_time": -1}},
#         ]
#         result = [
#             CCD_BlockItemSummary(**x)
#             for x in await db_to_use[Collections.transactions]
#             .aggregate(pipeline)
#             .to_list(length=None)
#         ]
#     except Exception as error:
#         print(error)
#         result = None

#     if result:
#         return result
#     else:
#         raise HTTPException(
#             status_code=404,
#             detail=f"Error retrieving modules on {net}, {error}.",
#         )
