"""Routes exposing module summaries and search utilities."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from markdown_it.rules_core import block
import datetime as dt
import re
from ccdexplorer.domain.generic import NET
from ccdexplorer.ccdexplorer_api.app.state_getters import get_grpcclient, get_mongo_motor
from ccdexplorer.ccdexplorer_api.app.utils import apply_docstring_router_wrappers, await_await
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoMotor, net_db
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader

router = APIRouter(tags=["Modules"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)

# The modules overview used to be served from a `statistics_modules_overview`
# document that ms_modules/subscriber/module.py rebuilt from scratch, but only
# whenever a *new module* was deployed -- so it drifted whenever an instance
# was created, or upgraded to point at a different module, in between
# deployments. All three pieces the page needs (module metadata, which month
# each module belongs to, and per-module instance counts) are small, indexed,
# fast live queries (~500ms cold for all three combined -- see the modules /
# module_deployed-transactions / instances collections directly), so we query
# them live instead and cache each piece for as long as it stays fresh:
# module identity/deployment-month barely ever changes (only grows on a new
# deployment), instance counts change far more often.
MODULE_DEPLOYMENTS_CACHE_SECONDS = 5 * 60
MODULE_INSTANCE_COUNTS_CACHE_SECONDS = 15


async def get_modules_with_deployment_dates(request: Request, net: str, db_to_use: dict) -> dict:
    """{module_ref: {...module fields..., "init_date": datetime}} for every
    module that has a matching module_deployed transaction."""
    now = dt.datetime.now().astimezone(dt.timezone.utc)
    last_requested = request.app.module_deployments_last_requested.get(net)
    cached = request.app.module_deployments.get(net)
    if (
        cached is not None
        and last_requested is not None
        and (now - last_requested).total_seconds() < MODULE_DEPLOYMENTS_CACHE_SECONDS
    ):
        return cached

    modules_dict = {}
    async for module in db_to_use[Collections.modules].find({}):
        module["id"] = module.pop("_id")  # match the shape templates already expect
        modules_dict[module["id"]] = module

    deploys = await await_await(
        db_to_use, Collections.transactions, [{"$match": {"type.contents": "module_deployed"}}]
    )
    for tx in deploys:
        module_ref = tx.get("account_transaction", {}).get("effects", {}).get("module_deployed")
        module = modules_dict.get(module_ref)
        if module:
            module["init_date"] = tx["block_info"]["slot_time"]

    result = {ref: m for ref, m in modules_dict.items() if "init_date" in m}
    request.app.module_deployments[net] = result
    request.app.module_deployments_last_requested[net] = now
    return result


async def get_live_module_instance_counts(request: Request, net: str, db_to_use: dict) -> dict:
    now = dt.datetime.now().astimezone(dt.timezone.utc)
    last_requested = request.app.module_instance_counts_last_requested.get(net)
    cached = request.app.module_instance_counts.get(net)
    if (
        cached is not None
        and last_requested is not None
        and (now - last_requested).total_seconds() < MODULE_INSTANCE_COUNTS_CACHE_SECONDS
    ):
        return cached

    pipeline = [{"$group": {"_id": "$source_module", "count": {"$sum": 1}}}]
    result = await await_await(db_to_use, Collections.instances, pipeline)
    counts = {x["_id"]: x["count"] for x in result if x["_id"]}

    request.app.module_instance_counts[net] = counts
    request.app.module_instance_counts_last_requested[net] = now
    return counts


@router.get("/{net}/modules/overview", response_class=JSONResponse)
async def get_overview_of_all_modules(
    request: Request,
    net: str,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return the latest monthly overview statistics for every module, live
    (module identity/deployment-month cached 5 minutes, instance counts
    cached 15 seconds -- see get_modules_with_deployment_dates and
    get_live_module_instance_counts).

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        mongodb: Mongo client dependency used to query modules/transactions/instances.
        api_key: API key extracted from the request headers.

    Returns:
        Dictionary keyed by ``year_month`` containing module overview rows.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = net_db(mongodb, net)

    modules_by_ref = await get_modules_with_deployment_dates(request, net, db_to_use)
    live_instance_counts = await get_live_module_instance_counts(request, net, db_to_use)

    by_month: dict[str, list[dict]] = {}
    for module_ref, module in modules_by_ref.items():
        module = dict(module)  # don't mutate the cached dict
        module["instances_count"] = live_instance_counts.get(module_ref, 0)
        init_date = module["init_date"]
        year_month = f"{init_date.year}-{init_date.month:02}"
        by_month.setdefault(year_month, []).append(module)

    for modules in by_month.values():
        modules.sort(key=lambda m: m["init_date"], reverse=True)

    return {
        year_month: {"year_month": year_month, "net": net, "modules": modules}
        for year_month, modules in sorted(by_month.items(), reverse=True)
    }


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
    db_to_use = net_db(mongomotor, net)

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


@router.get(
    "/{net}/modules/list/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_modules_list(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Page through modules.

    Currently not in use.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        skip: Number of modules to skip.
        limit: Maximum number of modules to return.
        mongodb: Mongo client dependency used to query ``modules``.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary with the list of module ids and the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
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

    result: list[str] = grpcclient.get_module_list(block_hash="last_final", net=NET(net))

    return {"modules": result[skip : skip + limit], "modules_count": len(result)}


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
