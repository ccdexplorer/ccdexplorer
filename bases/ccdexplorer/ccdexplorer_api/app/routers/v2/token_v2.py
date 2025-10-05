# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from ccdexplorer.ccdexplorer_api.app.utils import await_await
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse, RedirectResponse
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.cis import CIS
from ccdexplorer.domain.mongo import MongoTypeTokensTag, MongoTypeTokenAddress
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
from ccdexplorer.domain.generic import NET
from ccdexplorer.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
)
from redis.asyncio import Redis
from pydantic import BaseModel
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_mongo_db,
    get_grpcclient,
    get_mongo_motor,
)
from json import dumps, loads
from typing import Optional
from ccdexplorer.ccdexplorer_api.app.routers.v2.contract_v2 import (
    get_balance_of,
    GetBalanceOfRequest,
    get_module_name_from_contract_address,
)

# from ccdexplorer.ccdexplorer_api.app.utils import TokenHolding
from datetime import date, datetime
import json


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class TokenHolding(BaseModel):
    token_address: str
    contract: str
    token_id: str
    token_amount: str | int


router = APIRouter(tags=["Token"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


def get_owner_history_for_provenance(
    grpcclient: GRPCClient,
    tokenID: str,
    contract_address: CCD_ContractAddress,
    net: NET,
):
    entrypoint = "provenance_tag_nft.view_owner_history"
    ci = CIS(
        grpcclient,
        contract_address.index,
        contract_address.subindex,
        entrypoint,
        net,
    )
    parameter_bytes = ci.viewOwnerHistoryRequest(tokenID)

    ii = grpcclient.invoke_instance(
        "last_final",
        contract_address.index,
        contract_address.subindex,
        entrypoint,
        parameter_bytes,
        net,
    )

    result = ii.success.return_value
    return ci.viewOwnerHistoryResponse(result)


@router.get("/{net}/token/tag/{tag}/info", response_class=JSONResponse)
@router.get(
    "/{net}/token/tag/{tag}/token-id/{token_id}/info",
    response_class=JSONResponse,
)
async def get_token_based_on_token_id(
    request: Request,
    net: str,
    tag: str,
    token_id: Optional[str] = None,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get a token based on tag and token_id.
    """

    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    tag_result = await db_to_use[Collections.tokens_tags].find_one({"_id": tag})

    if tag_result:
        if not token_id:
            if "related_token_address" in tag_result:
                pipeline = [{"$match": {"_id": tag_result["related_token_address"]}}]
            else:
                pipeline = [{"$match": {"contract": tag_result["contracts"][0]}}]

            pipeline.append({"$limit": 1})
        else:
            pipeline = [
                {"$match": {"contract": {"$in": tag_result["contracts"]}}},
                {"$match": {"token_id": token_id}},
                {"$limit": 1},
            ]
        result = await await_await(db_to_use, Collections.tokens_token_addresses_v2, pipeline, 1)
        if len(result) > 0:
            the_token = result[0]
            # add verified information
            instance_address = f"{the_token['contract']}"
            result = await db_to_use[Collections.tokens_tags].find_one(
                {"contracts": {"$in": [instance_address]}}
            )
            the_token.update({"verified_information": result})
            # get mint event from the logged events collection
            pipeline = [
                {
                    "$match": {
                        "$and": [
                            {"event_info.token_address": the_token["_id"]},
                            {"event_info.event_type": "CIS-2.mint_event"},
                        ]
                    }
                },
                {"$sort": {"tx_info.block_height": 1}},
                {"$limit": 1},
            ]
            mint_event_logged_event = await await_await(
                db_to_use, Collections.tokens_logged_events_v2, pipeline, 1
            )
            if mint_event_logged_event:
                the_token.update({"mint_tx_hash": mint_event_logged_event[0]["tx_info"]["tx_hash"]})

            pipeline = [
                {"$match": {"token_holding.token_address": the_token["_id"]}},
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                    }
                },
                {
                    "$project": {
                        "total": {"$arrayElemAt": ["$metadata.total", 0]},
                    }
                },
            ]
            result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline, 1)
            if "total" in result[0]:
                current_holders_count = result[0]["total"]
            else:
                current_holders_count = 0
            the_token.update({"current_holders_count": current_holders_count})

            return the_token
        raise HTTPException(
            status_code=404,
            detail=f"Requested smart contract tag information for '{tag}' and {token_id} not found on {net}.",
        )
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested smart contract tag information for '{tag}' not found on {net}.",
        )


@router.get(
    "/{net}/token/{contract_index}/{contract_subindex}/{token_id}/info-for-public-keys",
    response_class=JSONResponse,
)
async def get_info_for_token_address_for_public_keys(
    request: Request,
    net: str,
    contract_index: int,
    contract_subindex: int,
    token_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get information for a given token address for public keys.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    contract_address = CCD_ContractAddress.from_index(contract_index, contract_subindex)
    token_id = "" if token_id == "_" else token_id
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    token_address = f"<{contract_index},{contract_subindex}>-{token_id}"

    fungible = False
    unverified = True
    fungible_result = None
    non_fungible_result = None
    # now try to get additional information from tokens_tags

    if token_id == "":
        # search for fungible token
        fungible_result = await db_to_use[Collections.tokens_tags].find_one(
            {"related_token_address": token_address}
        )
    else:
        non_fungible_result = await db_to_use[Collections.tokens_tags].find_one(
            {"contracts": {"$in": [contract_address.to_str()]}}
        )

    fungible = fungible_result is not None
    unverified = (fungible_result is None) and (non_fungible_result is None)

    this_token = {
        "token_address": token_address,
        "contract": contract_address.to_str(),
        # "public_key": public_key,
    }
    if fungible:
        this_token.update({"verified_information": fungible_result})
    else:
        this_token.update({"verified_information": non_fungible_result})

    result = await db_to_use[Collections.tokens_token_addresses_v2].find_one({"_id": token_address})
    this_token.update({"address_information": result})

    return loads(dumps(this_token, default=json_serial))


@router.get(
    "/{net}/token/{contract_index}/{contract_subindex}/{token_id}/info",
    response_class=JSONResponse,
)
async def get_info_for_token_address(
    request: Request,
    net: str,
    contract_index: int,
    contract_subindex: int,
    token_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get information for a given token address. For Provenance Tags specifically, the `owner_history`
    property is added if available.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    token_id = "" if token_id == "_" else token_id
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    token_address = f"<{contract_index},{contract_subindex}>-{token_id}"
    token_from_collection = await db_to_use[Collections.tokens_token_addresses_v2].find_one(
        {"_id": token_address}
    )

    if token_from_collection:
        # tag information if available
        instance_address = f"<{contract_index},{contract_subindex}>"
        result = await db_to_use[Collections.tokens_tags].find_one(
            {"contracts": {"$in": [instance_address]}}
        )
        token_from_collection.update({"verified_information": result})

        # get mint event from the logged events collection
        mint_event_logged_event = await db_to_use[Collections.tokens_logged_events_v2].find_one(
            {
                "$and": [
                    {"event_info.token_address": token_address},
                    {"event_info.event_type": "CIS-2.mint_event"},
                ]
            }
        )
        if not mint_event_logged_event:
            raise HTTPException(
                status_code=500,
                detail=f"mint_event for requested token_id {token_id} from contract <{contract_index},{contract_subindex}> is not found on {net}.",
            )
        token_from_collection.update(
            {"mint_tx_hash": mint_event_logged_event["tx_info"]["tx_hash"]}
        )

        pipeline = [
            {"$match": {"token_holding.token_address": token_address}},
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                }
            },
            {
                "$project": {
                    "total": {"$arrayElemAt": ["$metadata.total", 0]},
                }
            },
        ]
        result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline, 1)
        if "total" in result[0]:
            current_holders_count = result[0]["total"]
        else:
            current_holders_count = 0
        token_from_collection.update({"current_holders_count": current_holders_count})
        # # get current owner from the token_links collection
        # current_owner_link = (
        #     await db_to_use[Collections.tokens_links_v3]
        #     .find({"token_holding.token_address": token_address})
        #     .to_list(length=None)
        # )

        # current_owners = []
        # for link in current_owner_link:
        #     current_owners.append(
        #         {
        #             "address": link["account_address"],
        #             "balance": int(link["token_holding"]["token_amount"]),
        #         }
        #     )

        # token_from_collection.update({"current_owners": current_owners})

        # Provenance Tags Owner History
        provenance_tag_stored = await db_to_use[Collections.tokens_tags].find_one(
            {"_id": "provenance-tags"}
        )
        if provenance_tag_stored:
            owner_history_list = get_owner_history_for_provenance(
                grpcclient,
                token_id,
                CCD_ContractAddress.from_index(contract_index, contract_subindex),
                NET(net),
            )
            if owner_history_list:
                token_from_collection.update({"owner_history": owner_history_list})

        if "hidden" in token_from_collection:
            del token_from_collection["hidden"]
        if "token_holders" in token_from_collection:
            del token_from_collection["token_holders"]

        return loads(dumps(token_from_collection, default=json_serial))
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested token_id {token_id} from contract <{contract_index},{contract_subindex}> is not found on {net}.",
        )


@router.get(
    "/{net}/token/{contract_index}/{contract_subindex}/{token_id}/holders/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_token_current_holders(
    request: Request,
    net: str,
    contract_index: int,
    contract_subindex: int,
    token_id: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get current token holders for token.
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

    token_id = "" if token_id == "_" else token_id
    token_address = f"<{contract_index},{contract_subindex}>-{token_id}"
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    try:
        pipeline = [
            {"$match": {"token_holding.token_address": token_address}},
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "data": [
                        {"$skip": 0},
                    ],
                }
            },
            {
                "$project": {
                    "data": 1,
                    "total": {"$arrayElemAt": ["$metadata.total", 0]},
                }
            },
        ]
        result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline, limit)
        current_holders = [x for x in result[0]["data"]]
        if "total" in result[0]:
            total_count = result[0]["total"]
        else:
            total_count = 0

    except Exception as error:
        print(error)
        result = None

    if result is not None:
        addresses = [x["account_address"] for x in current_holders]
        contract = CCD_ContractAddress.from_index(contract_index, contract_subindex)
        module_name = await get_module_name_from_contract_address(db_to_use, contract)
        request = GetBalanceOfRequest(
            net=net,
            contract_address=contract,
            token_id=token_id,
            module_name=module_name,
            addresses=addresses,
            grpcclient=grpcclient,
            motor=mongomotor,
        )
        token_amounts_from_state = await get_balance_of(request)

        for holder in current_holders:
            token_holding = TokenHolding(**holder["token_holding"])

            token_holding.token_amount = token_amounts_from_state.get(holder["account_address"], 0)
            holder["token_holding"] = token_holding

        consolidated = {}

        for entry in current_holders:
            address_canonical = entry["account_address_canonical"]
            token_holding = entry["token_holding"]
            token_amount = int(token_holding.token_amount)

            if address_canonical in consolidated:
                consolidated[address_canonical]["token_holding"].token_amount += token_amount
            else:
                # Make a deep copy to ensure no mutation of the original entry
                consolidated[address_canonical] = entry
                consolidated[address_canonical]["token_holding"].token_amount = int(
                    consolidated[address_canonical]["token_holding"].token_amount
                )

        # Convert the consolidated dictionary back to a list
        de_duped_holders = list(consolidated.values())
        sorted_data = sorted(
            de_duped_holders,
            reverse=True,
            key=lambda x: int(x["token_holding"].token_amount),
        )

        return {
            "current_holders": sorted_data[skip : (skip + limit)],
            "total_count": total_count,
        }
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Can't retrieve current holders for token at {token_address} on {net}",
        )


@router.get(
    "/{net}/token/{contract_index}/{contract_subindex}/{token_id}/cis-2-compliant",
    response_class=JSONResponse,
)
async def get_token_cis_2_compliance(
    request: Request,
    net: str,
    contract_index: int,
    contract_subindex: int,
    token_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """
    Endpoint to get determination if all holder have zero of positive balance.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    token_id = "" if token_id == "_" else token_id
    token_address = f"<{contract_index},{contract_subindex}>-{token_id}"
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    compliant_contract = True
    try:
        pipeline = [
            {"$match": {"token_holding.token_address": token_address}},
            {"$match": {"token_holding.token_amount": {"$regex": "-"}}},
            {"$limit": 1},
        ]
        result = await await_await(db_to_use, Collections.tokens_links_v3, pipeline, 1)
        compliant_contract = len(result) == 0

    except Exception as _:
        pass

    return compliant_contract


@router.get(
    "/{net}/token/{tag}/info",
    response_class=JSONResponse,
)
async def get_info_for_token_tag(
    request: Request,
    net: str,
    tag: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> MongoTypeTokensTag:
    """
    Endpoint to get information for a given token tag.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    result = db_to_use[Collections.tokens_tags].find_one({"_id": tag})
    if result:
        token_tag = MongoTypeTokensTag(**result)
    if result:
        return token_tag
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested token_tag {tag} is not found on {net}.",
        )


@router.post(
    "/{net}/token/{contract_index}/{contract_subindex}/refresh",
    response_class=RedirectResponse,
)
async def add_token_address_without_token_id_to_metadata_refresh_queue(
    request: Request,
    net: str,
    contract_index: int,
    contract_subindex: int,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> RedirectResponse:
    """
    Endpoint to queue a token for a refresh of the metadata from the token metadataUrl where the token_id is None.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    return RedirectResponse(
        f"{router.prefix}/{net}/token/{contract_index}/{contract_subindex}/_/refresh"
    )


async def send_metadata_to_redis(r: Redis, repl_dict: dict, net: str):
    if r is not None and len(repl_dict) > 0:
        token_address = f"{repl_dict['contract']}-{repl_dict['token_id']}"
        await r.xadd(
            f"blocks:metadata:{net}",
            {
                "data": json.dumps(
                    {
                        "token_address": token_address,
                    }
                ).encode("utf-8")
            },
        )


@router.post(
    "/{net}/token/{contract_index}/{contract_subindex}/{token_id}/refresh",
    response_class=JSONResponse,
)
async def add_token_address_to_metadata_refresh_queue(
    request: Request,
    net: str,
    contract_index: int,
    contract_subindex: int,
    token_id: str | None,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to queue a token for a refresh of the metadata from the token metadataUrl.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    token_id = "" if token_id == "_" else token_id
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    token_address = f"<{contract_index},{contract_subindex}>-{token_id}"
    token_from_collection = db_to_use[Collections.tokens_token_addresses_v2].find_one(
        {"_id": token_address}
    )
    if token_from_collection:
        ta: MongoTypeTokenAddress = MongoTypeTokenAddress(**token_from_collection)
        repl_dict = ta.model_dump(exclude_none=True)
        if "failed_attempt" in repl_dict:
            del repl_dict["failed_attempt"]
        await send_metadata_to_redis(request.app.r, repl_dict, net)
        return JSONResponse({"detail": "Ok"})
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested token_id {token_id} from contract <{contract_index},{contract_subindex}> is not found on {net}.",
        )


@router.get(
    "/{net}/token/tag/{tag}",
    response_class=JSONResponse,
)
async def get_instance_tag_information(
    request: Request,
    net: str,
    tag: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get the recognized tag information from a tag id.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    result = await db_to_use[Collections.tokens_tags].find_one({"_id": tag})
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested smart contract tag information for '{tag}' not found on {net}.",
        )


@router.get(
    "/{net}/token/tag/{tag}/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_nft_tag_tokens(
    request: Request,
    net: str,
    tag: str,
    skip: int,
    limit: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get individual tokens for recognized nft tag.
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

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    tag_result = await db_to_use[Collections.tokens_tags].find_one({"_id": tag})
    if not tag_result:
        raise HTTPException(
            status_code=404,
            detail=f"Requested tag '{tag}' not found on {net}.",
        )

    filter = {"contract": {"$in": tag_result["contracts"]}}
    sort = list({"last_height_processed": -1}.items())
    nft_tokens = (
        await db_to_use[Collections.tokens_token_addresses_v2]
        .find(filter=filter, sort=sort, skip=skip, limit=limit)
        .to_list(length=limit)
    )

    total_token_count = await db_to_use[Collections.tokens_token_addresses_v2].count_documents(
        filter
    )

    return {"nft_tokens": nft_tokens, "total_count": total_token_count}
