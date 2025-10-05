# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import re

from ccdexplorer.api.app.utils import await_await
from ccdexplorer.mongodb import Collections, MongoMotor
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ContractAddress,
    CCD_BlockItemSummary,
)
from ccdexplorer.env import API_KEY_HEADER, API_URL
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.api.app.state_getters import get_mongo_motor, get_httpx_client
import httpx

router = APIRouter(tags=["Contracts"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get("/{net}/contracts/search/{value}", response_class=JSONResponse)
async def search_contracts(
    request: Request,
    net: str,
    value: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    search_str = str(value)
    regex = re.compile(search_str, re.IGNORECASE)
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    pipeline = [
        {
            "$match": {
                "$or": [
                    {"v0.name": {"$regex": regex}},
                    {"v1.name": {"$regex": regex}},
                ]
            }
        }
    ]
    result = await await_await(db_to_use, Collections.instances, pipeline)
    return result


@router.get(
    "/{net}/contracts/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_paginated_contracts(
    request: Request,
    net: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to page through the `instances` collection using skip/limit.
    """
    # validate network
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Unsupported network. Choose 'mainnet' or 'testnet'.",
        )

    db = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    try:
        # total documents for client-side page computations
        total_docs = await db[Collections.instances].estimated_document_count()

        # fetch the requested slice, sorted by height desc
        instances = await await_await(
            db,
            Collections.instances,
            [
                # pull the digits before the comma: "<9900,0>" -> "9900"
                {
                    "$addFields": {
                        "_sort_index": {
                            "$toInt": {
                                "$let": {
                                    "vars": {
                                        "m": {
                                            "$regexFind": {
                                                "input": "$_id",
                                                "regex": r"^<(\d+),",
                                            }
                                        }
                                    },
                                    "in": {"$arrayElemAt": ["$$m.captures", 0]},
                                }
                            }
                        }
                    }
                },
                {"$sort": {"_sort_index": -1}},  # or 1 for ascending
                {"$skip": skip},
                {"$limit": limit},
                {"$project": {"_sort_index": 0}},
            ],
            limit,
            allowDiskUse=True,
        )

        for instance in instances:
            contract_as_class = CCD_ContractAddress.from_str(instance["_id"])
            response = await httpx_client.get(
                f"{request.app.api_url}/v2/{net}/contract/{contract_as_class.index}/{contract_as_class.subindex}/deployed"
            )
            tx_classified = CCD_BlockItemSummary(**response.json()).model_dump(exclude_none=True)
            instance["tx_deployed"] = tx_classified

            response = await httpx_client.get(
                f"{request.app.api_url}/v2/{net}/module/{instance['source_module']}"
            )
            module_info = response.json()
            instance["module_info"] = module_info
    except Exception as e:
        # log e if you like, then:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving blocks: {e}",
        )

    return {
        "total_rows": total_docs,
        "instances": instances,
    }
