# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import math

from ccdexplorer.cis import CIS
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import MongoTypeInstance
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_ContractAddress,
)
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
)

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from pymongo import ASCENDING, DESCENDING
from pydantic import BaseModel, Field
from ccdexplorer.env import API_KEY_HEADER, API_URL
from fastapi.security.api_key import APIKeyHeader

from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_exchange_rates,
    get_grpcclient,
    get_mongo_db,
    get_httpx_client,
)
import httpx

# from ccdexplorer.ccdexplorer_api.app.utils import TokenHolding


class CIS5PublicKeysContracts(BaseModel):
    id: str = Field(..., alias="_id")
    wallet_contract_address: str
    cis2_token_contract_address: str
    token_id: Optional[str] = None
    token_address: Optional[str] = None
    address_or_public_key: str
    address_canonical_or_public_key: str
    token_amount: Optional[str] = None
    decimals: Optional[int] = None
    token_symbol: Optional[str] = None
    token_value: Optional[float] = None
    token_value_USD: Optional[float] = None
    verified_information: Optional[dict] = None
    address_information: Optional[dict] = None


router = APIRouter(tags=["Smart Wallet"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-keys",
    response_class=JSONResponse,
)
async def get_all_public_keys_for_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> list[str]:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    pipeline = [
        {"$match": {"wallet_contract_address": wallet_contract_address}},
        {"$group": {"_id": "$address_or_public_key"}},
    ]
    result = list(
        set(
            [
                x["_id"]
                for x in db_to_use[Collections.cis5_public_keys_contracts].aggregate(pipeline)
            ]
        )
    )
    return result


@router.get(
    "/{net}/smart-wallet/public-key/{public_key}",
    response_class=JSONResponse,
)
async def get_smart_wallet_details_from_public_key(
    request: Request,
    net: str,
    public_key: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    result = db_to_use[Collections.cis5_public_keys_info].find({"public_key": public_key})

    found = False
    for r in result:
        wallet_contract_address = CCD_ContractAddress.from_str(r["wallet_contract_address"])

        response = await httpx_client.get(
            f"{request.app.api_url}/v2/{net}/contract/{wallet_contract_address.index}/{wallet_contract_address.subindex}/supports-cis-standards"
        )
        result = response.json()
        if not result:
            continue
        if len(result) == 0:
            continue
        if "CIS-5" in result:
            return {
                "wallet_contract_address": r["wallet_contract_address"],
                "public_key": public_key,
            }

    if not found:
        raise HTTPException(
            status_code=404,
            detail=f"Requested public key {public_key} on {net} not found.",
        )


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/deployed",
    response_class=JSONResponse,
)
async def get_deployed_tx_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> CCD_BlockItemSummary:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    pipeline = [
        {
            "$match": {
                "$or": [
                    {"to_address_canonical": public_key},
                    {"from_address_canonical": public_key},
                ]
            },
        },
        {"$match": {"event_info.contract": wallet_contract_address}},
        {"$sort": {"tx_info.block_height": ASCENDING}},
        {"$limit": 1},
    ]
    result = list(db_to_use[Collections.tokens_logged_events_v2].aggregate(pipeline))
    if len(result) > 0:
        deployment_logged_event = result[0]
        deployment_tx_hash = deployment_logged_event["tx_info"]["tx_hash"]
        result = db_to_use[Collections.transactions].find_one(deployment_tx_hash)
        if result:
            result = CCD_BlockItemSummary(**result)
            return result
        else:
            raise HTTPException(
                status_code=404,
                detail=f"For requested public key {public_key} for smart wallet {wallet_contract_address_index} on {net}, can't find the deployment tx {deployment_tx_hash}.",
            )

    else:
        raise HTTPException(
            status_code=404,
            detail=f"Requested public key {public_key} for smart wallet {wallet_contract_address_index} on {net} not found.",
        )


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/transaction-count",
    response_class=JSONResponse,
)
async def get_tx_count_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """ """
    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    pipeline = [
        {
            "$match": {
                "$or": [
                    {"to_address_canonical": public_key},
                    {"from_address_canonical": public_key},
                ]
            },
        },
        {"$match": {"event_info.contract": wallet_contract_address}},
        {"$group": {"_id": "$tx_info.tx_hash"}},
        {"$count": "tx_count"},
    ]
    result = list(db_to_use[Collections.tokens_logged_events_v2].aggregate(pipeline))
    tx_count = result[0]["tx_count"] if len(result) > 0 else 0
    if tx_count > 0:
        return {
            "public_key": public_key,
            "contract": wallet_contract_address,
            "tx_count": tx_count,
        }
    else:
        raise HTTPException(
            status_code=404,
            detail=f"No transactions found for requested public key {public_key} for smart wallet {wallet_contract_address_index} on {net}.",
        )


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/logged-events/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_logged_events_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    skip: int,
    limit: int,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """ """
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

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    pipeline = [
        {
            "$match": {
                "$or": [
                    {"to_address_canonical": public_key},
                    {"from_address_canonical": public_key},
                ]
            },
        },
        {"$match": {"event_info.contract": wallet_contract_address}},
        {
            "$match": {
                "$or": [
                    {"recognized_event.token_amount": {"$ne": "0"}},
                    {"recognized_event.token_amount": {"$exists": False}},
                ]
            },
        },
        {"$sort": {"tx_info.block_height": DESCENDING}},
        {"$skip": skip},
        {"$limit": limit},
    ]
    result = list(db_to_use[Collections.tokens_logged_events_v2].aggregate(pipeline))
    logged_events_selected = result

    return {
        "logged_events_selected": logged_events_selected,
        "all_logged_events_count": 0,
    }


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/ccd-balance",
    response_class=JSONResponse,
)
async def get_ccd_balances_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    block_hash = "last_final"
    instance_index = wallet_contract_address_index
    instance_subindex = wallet_contract_address_subindex

    result = db_to_use[Collections.instances].find_one({"_id": wallet_contract_address})
    instance = MongoTypeInstance(**result)

    if instance and instance.v1:
        entrypoint_ccd = instance.v1.name[5:] + ".ccdBalanceOf"
    else:
        return []

    token_balance_ccd: dict[dict] = {}

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint_ccd, NET(net))
    rr, ii = ci.CCDbalanceOf(block_hash, [public_key])

    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        token_balance_ccd["ccd"] = {
            "contract": "ccd",
            "public_key": public_key,
            "balance": rr[0],
        }
        token_balance_ccd["ccd"].update(
            {"ccd_balance_in_USD": (rr[0] / 1_000_000) * exchange_rates["CCD"]["rate"]}
        )

    return {"ccd": token_balance_ccd}


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/tokens-available",
    response_class=JSONResponse,
)
async def get_tokens_available_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    pipeline = [
        {"$match": {"wallet_contract_address": wallet_contract_address}},
        {"$match": {"address_or_public_key": public_key}},
        {"$limit": 1},
    ]

    links_for_key = [
        x for x in db_to_use[Collections.cis5_public_keys_contracts].aggregate(pipeline)
    ]

    return len(links_for_key) > 0


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/tokens-list/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_cis2_tokens_list_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    skip: int,
    limit: int,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """ """
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

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    pipeline = [
        {"$match": {"wallet_contract_address": wallet_contract_address}},
        {"$match": {"address_or_public_key": public_key}},
        {"$sort": {"_id": ASCENDING}},
        {"$skip": skip},
        {"$limit": limit},
    ]

    links_for_key = [
        x for x in db_to_use[Collections.cis5_public_keys_contracts].aggregate(pipeline)
    ]
    cis2_contracts_dict = {
        f"{x['cis2_token_contract_address']}-{x['token_id_or_ccd']}": {
            "token_id": x["token_id_or_ccd"],
            "contract": x["cis2_token_contract_address"],
        }
        for x in links_for_key
        if x["token_id_or_ccd"] != "ccd"
    }

    token_balances_fungible: dict[dict] = {}
    token_balances_non_fungible: dict[dict] = {}
    token_balances_unverified: dict[dict] = {}
    for cis_2_contract_address_str, cis2_dict in cis2_contracts_dict.items():
        token_id = cis2_dict["token_id"]

        fungible = False
        unverified = True
        fungible_result = None
        non_fungible_result = None
        # now try to get additional information from tokens_tags
        token_address = cis_2_contract_address_str
        if token_id == "":
            # search for fungible token
            fungible_result = db_to_use[Collections.tokens_tags].find_one(
                {"related_token_address": token_address}
            )
        else:
            non_fungible_result = db_to_use[Collections.tokens_tags].find_one(
                {"contracts": {"$in": [cis2_dict["contract"]]}}
            )

        fungible = fungible_result is not None
        unverified = (fungible_result is None) and (non_fungible_result is None)

        this_token = {
            "token_address": cis_2_contract_address_str,
            "contract": cis2_dict["contract"],
            "public_key": public_key,
        }
        if fungible:
            this_token.update({"verified_information": fungible_result})
        else:
            this_token.update({"verified_information": non_fungible_result})

        result = db_to_use[Collections.tokens_token_addresses_v2].find_one({"_id": token_address})
        this_token.update({"address_information": result})

        token_vi = this_token["verified_information"]
        if not token_vi:
            continue

        if fungible:
            token_balances_fungible[cis_2_contract_address_str] = this_token
        else:
            token_balances_non_fungible[cis_2_contract_address_str] = this_token

        if unverified:
            token_balances_unverified[cis_2_contract_address_str] = this_token
    return {
        "fungible": token_balances_fungible,
        "non_fungible": token_balances_non_fungible,
        "unverified": token_balances_unverified,
    }


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/token-balances/{skip}/{limit}",
    response_class=JSONResponse,
)
async def get_token_balances_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    skip: int,
    limit: int,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet

    pipeline = [
        {"$match": {"wallet_contract_address": wallet_contract_address}},
        {"$match": {"address_or_public_key": public_key}},
        {"$sort": {"_id": ASCENDING}},
        {"$skip": skip},
        {"$limit": limit},
    ]

    links_for_key = [
        x for x in db_to_use[Collections.cis5_public_keys_contracts].aggregate(pipeline)
    ]
    cis2_contracts_dict = {
        f"{x['cis2_token_contract_address']}-{x['token_id_or_ccd']}": {
            "token_id": x["token_id_or_ccd"],
            "contract": x["cis2_token_contract_address"],
        }
        for x in links_for_key
        if x["token_id_or_ccd"] != "ccd"
    }

    block_hash = "last_final"
    instance_index = wallet_contract_address_index
    instance_subindex = wallet_contract_address_subindex

    result = db_to_use[Collections.instances].find_one({"_id": wallet_contract_address})
    instance = MongoTypeInstance(**result)

    if instance and instance.v1:
        entrypoint = instance.v1.name[5:] + ".cis2BalanceOf"
    else:
        return []
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET(net))

    token_balances_fungible: dict[dict] = {}
    token_balances_non_fungible: dict[dict] = {}
    token_balances_unverified: dict[dict] = {}

    public_keys = [public_key]
    for cis_2_contract_address_str, cis2_dict in cis2_contracts_dict.items():
        cis_2_contract_address = CCD_ContractAddress.from_str(cis2_dict["contract"])
        token_id = cis2_dict["token_id"]
        rr, ii = ci.CIS2balanceOf(block_hash, cis_2_contract_address, token_id, public_keys)

        if ii.failure.used_energy > 0:
            print(ii.failure)
        else:
            token_amount = rr[0]
            fungible = False
            unverified = True
            fungible_result = None
            non_fungible_result = None
            # now try to get additional information from tokens_tags
            token_address = cis_2_contract_address_str
            if token_id == "":
                # search for fungible token
                fungible_result = db_to_use[Collections.tokens_tags].find_one(
                    {"related_token_address": token_address}
                )
            else:
                non_fungible_result = db_to_use[Collections.tokens_tags].find_one(
                    {"contracts": {"$in": [cis2_dict["contract"]]}}
                )

            fungible = fungible_result is not None
            unverified = (fungible_result is None) and (non_fungible_result is None)

            this_token = {
                "token_address": cis_2_contract_address_str,
                "contract": cis2_dict["contract"],
                "public_key": public_key,
                "balance": rr[0],
            }
            if fungible:
                this_token.update({"verified_information": fungible_result})
            else:
                this_token.update({"verified_information": non_fungible_result})

            result = db_to_use[Collections.tokens_token_addresses_v2].find_one(
                {"_id": token_address}
            )
            this_token.update({"address_information": result})
            if unverified:
                token_balances_unverified[cis_2_contract_address_str] = this_token

            token_vi = this_token["verified_information"]
            if not token_vi:
                continue

            if fungible:
                if "get_price_from" not in token_vi:
                    continue
                this_token = update_fungible_token_with_price_info(
                    exchange_rates, this_token, token_amount, token_vi
                )
                token_balances_fungible[cis_2_contract_address_str] = this_token
            else:
                token_balances_non_fungible[cis_2_contract_address_str] = this_token

    return {
        "fungible": token_balances_fungible,
        "non_fungible": token_balances_non_fungible,
        "unverified": token_balances_unverified,
    }


def update_fungible_token_with_price_info(
    exchange_rates,
    this_token_: dict,
    token_amount,
    token_vi,
):
    this_token_.update({"token_symbol": token_vi["get_price_from"]})
    this_token_.update({"decimals": token_vi["decimals"]})
    this_token_.update({"token_value": int(token_amount) * (math.pow(10, -token_vi["decimals"]))})
    if this_token_["token_symbol"] in exchange_rates:
        this_token_.update(
            {
                "token_value_USD": (
                    this_token_["token_value"] * exchange_rates[this_token_["token_symbol"]]["rate"]
                )
            }
        )
    else:
        this_token_.update({"token_value_USD": 0})

    return this_token_


@router.get(
    "/{net}/smart-wallet/{wallet_contract_address_index}/{wallet_contract_address_subindex}/public-key/{public_key}/cis2-contracts",
    response_class=JSONResponse,
)
async def get_all_cis2_contracts_for_public_key_from_smart_wallet_contract(
    request: Request,
    net: str,
    wallet_contract_address_index: int,
    wallet_contract_address_subindex: int,
    public_key: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    api_key: str = Security(API_KEY_HEADER),
) -> list[str]:
    """ """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    wallet_contract_address = CCD_ContractAddress.from_index(
        wallet_contract_address_index, wallet_contract_address_subindex
    ).to_str()
    db_to_use = mongodb.testnet if net == "testnet" else mongodb.mainnet
    pipeline = [
        {"$match": {"wallet_contract_address": wallet_contract_address}},
        {"$match": {"address_or_public_key": public_key}},
        {"$group": {"_id": "$cis2_token_contract_address"}},
    ]
    result = list(
        set(
            [
                x["_id"]
                for x in db_to_use[Collections.cis5_public_keys_contracts].aggregate(pipeline)
            ]
        )
    )
    return result
