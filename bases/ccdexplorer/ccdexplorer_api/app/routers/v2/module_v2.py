"""Routes exposing module metadata, schemas, and usage statistics."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import base64
import json
import re
from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.grpc_client.types_pb2 import VersionedModuleSource
from ccdexplorer.mongodb import (
    Collections,
    MongoMotor,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse

from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.ccdexplorer_api.app.state_getters import get_grpcclient, get_mongo_motor

router = APIRouter(tags=["Module"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)
apply_docstring_router_wrappers(router)


@router.get(
    "/{net}/module/{module_ref}/deployed",
    response_class=JSONResponse,
)
async def get_module_deployment_tx(
    request: Request,
    net: str,
    module_ref: str,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_BlockItemSummary:
    """Return the transaction that deployed the specified module reference.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        module_ref: Module reference (hash).
        mongodb: Mongo client dependency used to query the ``transactions`` collection.
        api_key: API key extracted from the request headers.

    Returns:
        ``CCD_BlockItemSummary`` describing the deployment transaction.

    Raises:
        HTTPException: If the network is unsupported or the module is unknown.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    result = await db_to_use[Collections.transactions].find_one(
        {"account_transaction.effects.module_deployed": module_ref}
    )
    if result:
        result = CCD_BlockItemSummary(**result)
        return result


@router.get(
    "/{net}/module/{module_ref}/schema",
    response_class=JSONResponse,
)
async def get_module_schema(
    request: Request,
    net: str,
    module_ref: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """Return the raw module source in base64 encoding along with its version.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        module_ref: Module reference (hash).
        grpcclient: gRPC client dependency used to fetch module sources.
        api_key: API key extracted from the request headers.

    Returns:
        JSON document with the encoded schema and the version tag.

    Raises:
        HTTPException: If the network is unsupported or the source cannot be found.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    ms: VersionedModuleSource = grpcclient.get_module_source_original_classes(
        module_ref, "last_final", net=NET(net)
    )
    version = "v1" if ms.v1 else "v0"
    module_source = ms.v1.value if ms.v1 else ms.v0.value
    return JSONResponse(
        {
            "module_source": json.dumps(base64.encodebytes(module_source).decode()),
            "version": version,
        }
    )


@router.get(
    "/{net}/module/{module_ref}/instances/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_module_instances(
    request: Request,
    net: str,
    module_ref: str,
    skip: int,
    limit: int,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Page through contract instances derived from a specific module reference.

    Args:
        request: FastAPI request context providing pagination limits.
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        module_ref: Module reference (hash).
        skip: Number of instances to skip.
        limit: Maximum number of instances to return.
        mongodb: Mongo client dependency used to query ``instances``.
        api_key: API key extracted from the request headers.

    Returns:
        A dictionary with the list of instance addresses and the total count.

    Raises:
        HTTPException: If the network is unsupported or pagination invalid.
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

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    pipeline = [
        {"$match": {"source_module": module_ref}},
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
        {
            "$project": {
                "data": 1,
                "total": {"$arrayElemAt": ["$metadata.total", 0]},
            }
        },
    ]
    result = await await_await(db_to_use, Collections.instances, pipeline)
    module_instances = [x["_id"] for x in result[0]["data"]]
    if "total" in result[0]:
        instances_count = result[0]["total"]
    else:
        instances_count = 0

    return {"module_instances": module_instances, "instances_count": instances_count}


@router.get(
    "/{net}/module/{module_ref}/usage",
    response_class=JSONResponse,
)
async def get_module_usage(
    request: Request,
    net: str,
    module_ref: str,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """Return daily activity counts for all instances referencing the module.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        module_ref: Module reference (hash).
        mongodb: Mongo client dependency used to query impacts and instances.
        api_key: API key extracted from the request headers.

    Returns:
        A list of day/count pairs describing usage.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    module_instances_result = (
        await db_to_use[Collections.instances]
        .find({"source_module": module_ref})
        .to_list(length=None)
    )
    module_instances = [x["_id"] for x in module_instances_result]
    pipeline = [
        {"$match": {"impacted_address_canonical": {"$in": module_instances}}},
        {"$group": {"_id": "$date", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    result = await await_await(db_to_use, Collections.impacted_addresses, pipeline)

    return result


@router.get(
    "/{net}/module/{module_ref}",
    response_class=JSONResponse,
)
async def get_module(
    request: Request,
    net: str,
    module_ref: str,
    mongodb: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """Return the stored module metadata document.

    Args:
        request: FastAPI request context (unused but required).
        net: Network identifier, must be ``mainnet`` or ``testnet``.
        module_ref: Module reference (hash).
        mongodb: Mongo client dependency used to read the ``modules`` collection.
        api_key: API key extracted from the request headers.

    Returns:
        JSON document for the module reference.

    Raises:
        HTTPException: If the network is unsupported.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    result = await db_to_use[Collections.modules].find_one({"_id": module_ref})

    return result
