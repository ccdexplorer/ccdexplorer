# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import datetime as dt
import json
from collections import Counter
from datetime import timedelta
from enum import Enum
from typing import Any

from ccdexplorer.ccdexplorer_api.app.utils import await_await
import dateutil
import grpc
import pandas as pd
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ArInfo,
    CCD_BlockInfo,
    CCD_BlockItemSummary,
    CCD_IpInfo,
    CCD_WinningBaker,
)
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from grpc._channel import _MultiThreadedRendezvous

from ccdexplorer.env import API_KEY_HEADER
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_exchange_rates,
    get_grpcclient,
    get_mongo_db,
    get_mongo_motor,
)

router = APIRouter(tags=["Misc"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


class ExchangePeriod(Enum):
    h1 = 1
    d1 = 24
    d7 = 24 * 7
    d30 = 24 * 30
    d90 = 24 * 90
    all = 24 * 365 * 1000  # 1000 years


@router.get(
    "/{net}/misc/tx-data/{project_id}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_tx_data_for_project(
    request: Request,
    net: str,
    project_id: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get transactions counts for projects (and the chain).
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    dates_to_include = generate_dates_from_start_until_end(start_date, end_date)
    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": "statistics_transaction_types"}},
        {"$match": {"project": project_id}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    return JSONResponse([x for x in result])


@router.get(
    "/{net}/misc/today-in/{date}",
    response_class=JSONResponse,
)
async def get_today_in_data(
    request: Request,
    net: str,
    date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all interesting facts for this day.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet

    return_result = {"date": date}

    # day data
    result = await db_to_use[Collections.blocks_per_day].find_one({"date": date})
    if result:
        return_result["day_data"] = result

        pipeline = [
            {"$match": {"account_transaction": {"$exists": True}}},
            {
                "$match": {
                    "block_info.height": {
                        "$gte": result["height_for_first_block"],
                        "$lte": result["height_for_last_block"],
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "tx_count": {"$count": {}},
                    "fee_for_day": {"$sum": "$account_transaction.cost"},
                }
            },
        ]
        result = await await_await(db_to_use, Collections.transactions, pipeline)
        return_result["tx_count"] = result[0]["tx_count"]
        return_result["fee_for_day"] = result[0]["fee_for_day"]

    # logged events by contract
    pipeline = [
        {"$match": {"tx_info.date": date}},
        {"$group": {"_id": "$event_info.contract", "count": {"$count": {}}}},
        {"$sort": {"count": -1}},
    ]
    result = await await_await(db_to_use, Collections.tokens_logged_events_v2, pipeline)
    return_result["logged_events_by_contract"] = result

    # tx types
    pipeline = [
        {"$match": {"date": date}},
        {"$match": {"type": "statistics_transaction_types"}},
        {"$match": {"project": "all"}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    if len(result) > 0:
        if "tx_type_counts" in result[0]:
            result = result[0]["tx_type_counts"]
        else:
            result = {}
    else:
        result = {}
    return_result["tx_types"] = result

    # impacted addresses
    pipeline = [
        {"$match": {"date": date}},
        {  # this filters out account rewards, as they are special events
            "$match": {"tx_hash": {"$exists": True}},
        },
        {"$group": {"_id": "$impacted_address_canonical", "count": {"$count": {}}}},
        {"$sort": {"count": -1}},
    ]
    impacted_addresses_result = await await_await(
        db_to_use, Collections.impacted_addresses, pipeline
    )

    # filter out public keys that do not "exist"
    public_keys_in_results = [x["_id"] for x in impacted_addresses_result if len(x["_id"]) == 64]
    pipeline = [
        {"$match": {"public_key": {"$in": public_keys_in_results}}},
        {"$project": {"_id": 0, "public_key": 1}},
        {"$group": {"_id": "$public_key"}},  # Group by public_key to remove duplicates
    ]
    result = await await_await(db_to_use, Collections.cis5_public_keys_info, pipeline)
    recognized_public_keys = [x["_id"] for x in result]
    final_result = [
        x
        for x in impacted_addresses_result
        if ((len(x["_id"]) < 64) or (x["_id"] in recognized_public_keys))
    ]
    return_result["impacted_addresses"] = final_result

    # validator suspended and primed for...
    pipeline = [{"$match": {"date": date}}]
    primed_suspended = await await_await(db_to_use, Collections.validator_logs, pipeline)
    return_result["primed_suspended"] = primed_suspended

    return return_result


@router.get(
    "/{net}/misc/cns-domain/{tokenID}",
    response_class=JSONResponse,
)
async def get_bictory_cns_domain(
    request: Request,
    net: str,
    tokenID: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get possible Bictory CNS Domain name from tokenId.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    result = await db_to_use[Collections.cns_domains].find_one({"_id": tokenID})
    if result:
        return JSONResponse({"domain_name": result["domain_name"]})
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Domain name for tokenID {tokenID} is not found on {net}.",
        )


@router.get(
    "/{net}/misc/credential-issuers",
    response_class=JSONResponse,
)
async def get_credential_issuers(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get credential issuers for the requested net.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    result = await db_to_use[Collections.credentials_issuers].find({}).to_list(length=None)
    if result:
        credential_issuers = [x["_id"] for x in result]
        return JSONResponse(credential_issuers)
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error getting credential issuers on {net}.",
        )


@router.get(
    "/{net}/misc/exchange-rates",
    response_class=JSONResponse,
)
async def get_spot_exchange_rates(
    request: Request,
    net: str,
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get spot exchange rates.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    return exchange_rates


@router.get("/{net}/misc/protocol-updates")
async def get_protocol_updates(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_BlockItemSummary]:
    """
    Endpoint to get protocol update transactions for the requested net.
    """
    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    pipeline = [
        {"$match": {"update.payload.protocol_update": {"$exists": True}}},
        {"$sort": {"block_info.height": -1}},
    ]

    result = await await_await(db_to_use, Collections.transactions, pipeline)
    return result


@router.get("/{net}/misc/identity-providers")
async def get_identity_providers(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_IpInfo]:
    """
    Endpoint to get identity providers for the requested net.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    identity_providers = grpcclient.get_identity_providers("last_final", NET(net))
    return identity_providers


@router.get("/{net}/misc/anonymity-revokers")
async def get_anonymity_revokers(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_ArInfo]:
    """
    Endpoint to get anonymity revokers for the requested net.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    anonymity_revokers = grpcclient.get_anonymity_revokers("last_final", NET(net))
    return anonymity_revokers


@router.get(
    "/{net}/misc/labeled-accounts",
    response_class=JSONResponse,
)
async def get_labeled_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get community labeled accounts.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    # labeled accounts only exist for mainnet
    # db_to_use = mongomotor.mainnet
    db_utilities = mongomotor.utilities

    result = await db_utilities[CollectionsUtilities.labeled_accounts].find({}).to_list(length=None)
    labeled_accounts = {}
    for r in result:
        current_group = labeled_accounts.get(r["label_group"], {})
        current_group[r["_id"]] = r["label"]
        labeled_accounts[r["label_group"]] = current_group

    result = (
        await db_utilities[CollectionsUtilities.labeled_accounts_metadata]
        .find({})
        .to_list(length=None)
    )

    colors = {}
    descriptions = {}
    for r in result:
        colors[r["_id"]] = r.get("color")
        descriptions[r["_id"]] = r.get("description")

    tags = {
        "labels": labeled_accounts,
        "colors": colors,
        "descriptions": descriptions,
    }

    return JSONResponse(tags)


@router.get(
    "/{net}/misc/community-labeled-accounts",
    response_class=JSONResponse,
)
async def get_community_labeled_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get community labeled accounts (indexes).
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    # labeled accounts only exist for mainnet
    db_to_use = mongomotor.mainnet
    db_utilities = mongomotor.utilities

    result = await db_utilities[CollectionsUtilities.labeled_accounts].find({}).to_list(length=None)
    labeled_accounts = {}
    for r in result:
        current_group = labeled_accounts.get(r["label_group"], {})
        if "account_index" in r:
            current_group[r["account_index"]] = r["label"]
        else:
            current_group[r["_id"]] = r["label"]
        labeled_accounts[r["label_group"]] = current_group

    result = (
        await db_utilities[CollectionsUtilities.labeled_accounts_metadata]
        .find({})
        .to_list(length=None)
    )

    colors = {}
    descriptions = {}
    for r in result:
        colors[r["_id"]] = r.get("color")
        descriptions[r["_id"]] = r.get("description")

    ### insert projects into tags
    # display_names
    projects_display_names = {
        x["_id"]: x["display_name"]
        for x in await db_utilities[CollectionsUtilities.projects].find({}).to_list(length=None)
    }
    # account addresses
    project_account_addresses = (
        await db_to_use[Collections.projects].find({"type": "account_address"}).to_list(length=None)
    )

    dd = {}
    for paa in project_account_addresses:
        if "display_name" in paa:
            dd[paa["account_index"]] = paa["display_name"]
        else:
            dd[paa["account_index"]] = projects_display_names[paa["project_id"]]
    labeled_accounts["projects"] = dd

    # contract addresses
    project_contract_addresses = (
        await db_to_use[Collections.projects]
        .find({"type": "contract_address"})
        .to_list(length=None)
    )

    dd = {}
    for paa in project_contract_addresses:
        if "display_name" in paa:
            dd[paa["contract_address"]] = paa["display_name"]
        else:
            dd[paa["contract_address"]] = projects_display_names[paa["project_id"]]
    labeled_accounts["contracts"].update(dd)

    labels_melt = {}
    for label_group in labeled_accounts.keys():
        label_group_color = colors[label_group]
        for address, tag in labeled_accounts[label_group].items():
            labels_melt[address] = {
                "label": tag,
                "group": label_group,
                "color": label_group_color,
            }

    colors = {}
    descriptions = {}
    for r in result:
        colors[r["_id"]] = r.get("color")
        descriptions[r["_id"]] = r.get("description")

    del labeled_accounts["projects"]
    tags = {
        "labels_melt": labels_melt,
        "labeled_accounts": labeled_accounts,
        "colors": colors,
        "descriptions": descriptions,
    }

    return JSONResponse(tags)


def generate_dates_from_start_until_end(start: str, end: str):
    start_date = dateutil.parser.parse(start)
    end_date = dateutil.parser.parse(end)
    date_range = []

    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return date_range


@router.get(
    "/{net}/misc/statistics-chain/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_data_for_chain_analysis(
    request: Request,
    net: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get data for analysis.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    dates_to_include = generate_dates_from_start_until_end(start_date, end_date)
    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": "statistics_transaction_types"}},
        {"$match": {"project": "all"}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0, "project": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    return JSONResponse([x for x in result])


@router.get(
    "/{net}/misc/statistics/{analysis}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_data_for_analysis(
    request: Request,
    net: str,
    analysis: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get data for analysis.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        dates_to_include = generate_dates_from_start_until_end(start_date, end_date)
    except:  # noqa: E722
        raise HTTPException(
            status_code=404,
            detail="No valid date(s) given.",
        )

    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": analysis}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    return JSONResponse([x for x in result])


@router.get(
    "/{net}/misc/validator-nodes/count",
    response_class=JSONResponse,
)
async def get_nodes_count(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """
    Endpoint to get count of all validator nodes.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    result = await db_to_use[Collections.paydays_v2_current_payday].count_documents({})
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail="Error requesting nodes for {net}.",
        )


@router.get(
    "/{net}/misc/node/{node_id}",
    response_class=JSONResponse,
)
async def get_node_info(
    request: Request,
    net: str,
    node_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get node information for a given node id.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.mainnet
    result = await db_to_use[Collections.dashboard_nodes].find_one({"_id": node_id})
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail="Error requesting nodes for {net}.",
        )


@router.get(
    "/{net}/misc/projects/all-ids",
    response_class=JSONResponse,
)
async def get_all_project_ids(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    project_ids = {}
    result = await mongomotor.utilities[CollectionsUtilities.projects].find({}).to_list(length=None)
    for project in result:
        project_ids[project["_id"]] = project

    return project_ids


@router.get(
    "/{net}/misc/projects/{project_id}",
    response_class=JSONResponse,
)
async def get_project_id(
    request: Request,
    net: str,
    project_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> Any | None:
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    result = await mongomotor.utilities[CollectionsUtilities.projects].find_one({"_id": project_id})

    return result


@router.get(
    "/{net}/misc/projects/{project_id}/addresses",
    response_class=JSONResponse,
)
async def get_project_addresses(
    request: Request,
    net: str,
    project_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    project_addresses = (
        await db_to_use[Collections.projects].find({"project_id": project_id}).to_list(length=None)
    )

    return project_addresses


@router.get(
    "/misc/release-notes",
    response_class=JSONResponse,
)
async def get_release_notes(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    db_to_use = mongomotor.utilities
    release_notes = list(
        reversed(await db_to_use[CollectionsUtilities.release_notes].find({}).to_list(length=None))
    )

    return release_notes


@router.get(
    "/{net}/misc/consensus-detailed-status",
    response_class=JSONResponse,
)
async def get_consensus_detailed_status(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get consensus detailed status for the requested net.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        cds = grpcclient.get_consensus_detailed_status(net=NET(net))
    except Exception as e:  # noqa: E722
        raise HTTPException(
            status_code=404,
            detail=f"Error getting consensus detailed status on {net}. {e}",
        )

    return cds.model_dump(exclude_none=True)


async def get_exchange_txs_as_receiver(
    exchanges_canonical, start_block, end_block, mongomotor: MongoMotor
):
    pipeline = [
        {"$match": {"receiver_canonical": {"$in": exchanges_canonical}}},
        {"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}},
    ]
    txs_as_receiver = await await_await(
        mongomotor.mainnet, Collections.involved_accounts_transfer, pipeline
    )
    return txs_as_receiver


async def get_exchange_txs_as_sender(
    exchanges_canonical, start_block, end_block, mongomotor: MongoMotor
):
    pipeline = [
        {"$match": {"sender_canonical": {"$in": exchanges_canonical}}},
        {"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}},
    ]
    txs_as_sender = await await_await(
        mongomotor.mainnet, Collections.involved_accounts_transfer, pipeline
    )
    return txs_as_sender


@router.get("/{net}/sellers-and-buyers/{period}")
async def get_sellers_and_buyers_for_period(
    request: Request,
    net: str,
    period: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    tags: dict = Depends(get_labeled_accounts),
):
    """
    This endpoint retrieves sellers and buyers to known exchange accounts.
    Possible values for period (in hours):
    h1 = 1
    d1 = 24
    d7 = 24 * 7
    d30 = 24 * 30
    d90 = 24 * 90
    all = 24 * 365 * 1000

    """
    tags_json = json.loads(tags.body)
    exchanges_canonical = [x[:29] for x in tags_json["labels"]["exchanges"].keys()]

    now = dt.datetime.now().now().astimezone(dt.UTC)

    try:
        period = ExchangePeriod[period]
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail="Invalid period given.",
        )

    all_txs = []
    start_time = now - dt.timedelta(hours=period.value)
    start_time_block = (
        await mongomotor.mainnet[Collections.blocks]
        .find({"slot_time": {"$lt": start_time}})
        .sort("slot_time", -1)
        .to_list(length=1)
    )
    if len(start_time_block) > 0:
        start_block = start_time_block[0]["height"]
    else:
        start_block = 0

    end_block = 1_000_000_000

    txs_as_sender = await get_exchange_txs_as_sender(
        exchanges_canonical, start_block, end_block, mongomotor
    )
    txs_as_receiver = await get_exchange_txs_as_receiver(
        exchanges_canonical, start_block, end_block, mongomotor
    )

    all_txs.extend(txs_as_sender)
    all_txs.extend(txs_as_receiver)

    no_inter_exch_txs = [
        x
        for x in all_txs
        if not (
            (x["sender_canonical"] in exchanges_canonical)
            and (x["receiver_canonical"] in exchanges_canonical)
        )
    ]

    buyers = []
    sellers = []

    df = pd.DataFrame(no_inter_exch_txs)
    if len(df) > 0:
        f_sellers = df["receiver_canonical"].isin(exchanges_canonical)
        f_buyers = df["sender_canonical"].isin(exchanges_canonical)

        df_buyers_gb = df[f_buyers].groupby(["receiver_canonical"])
        df_sellers_gb = df[f_sellers].groupby(["sender_canonical"])

        buyers_address = set(df_buyers_gb.groups.keys())
        sellers_address = set(df_sellers_gb.groups.keys())

        all_addresses = list(buyers_address | sellers_address)

        for address in all_addresses:
            if address in buyers_address:
                group = df_buyers_gb.get_group((address,))
                buy_sum = group.amount.sum() / 1_000_000
                bought_txs_count = len(group)
            else:
                buy_sum = 0
                bought_txs_count = 0

            if address in sellers_address:
                group = df_sellers_gb.get_group((address,))
                sell_sum = group.amount.sum() / 1_000_000
                sold_txs_count = len(group)
            else:
                sell_sum = 0
                sold_txs_count = 0

            total = buy_sum - sell_sum
            if total < 0:
                seller = {
                    "address_canonical": address,
                    "total": total,
                    "bought": buy_sum,
                    "sold": sell_sum,
                    "sold_txs_count": sold_txs_count,
                    "bought_txs_count": bought_txs_count,
                }
                sellers.append(seller)
            else:
                buyer = {
                    "address_canonical": address,
                    "total": total,
                    "bought": buy_sum,
                    "sold": sell_sum,
                    "sold_txs_count": sold_txs_count,
                    "bought_txs_count": bought_txs_count,
                }
                buyers.append(buyer)

        sellers = sorted(sellers, key=lambda d: d["total"])
        buyers = sorted(buyers, key=lambda d: d["total"], reverse=True)
    return {
        "sellers": sellers,
        "buyers": buyers,
        "period_in_hours": period,
    }


@router.get("/{net}/misc/winning-bakers-epoch/genesis-index/{genesis_index}/epoch/{epoch}")
async def get_winning_bakers_epoch(
    request: Request,
    genesis_index: int,
    epoch: int,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get a summary of winning bakers for a given epoch and genesis index.
    """
    if net not in ["mainnet", "testnet"]:
        raise HTTPException(
            status_code=404,
            detail="Don't be silly. We only support mainnet and testnet.",
        )

    try:
        winning_bakers: list[CCD_WinningBaker] = grpcclient.get_winning_bakers_epoch(
            genesis_index, epoch, NET(net)
        )  # type: ignore
    except _MultiThreadedRendezvous as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE and "Future epoch" in e.details():  # type: ignore
            raise HTTPException(
                status_code=404,
                detail="Future epoch.",
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Error getting winning bakers for genesis index {genesis_index} and epoch {epoch} on {net}.",
            )
    filtered = [x for x in winning_bakers if not x.present]

    winner_counts = Counter(x.winner for x in filtered)

    sorted_counts = dict(sorted(winner_counts.items(), key=lambda item: item[1], reverse=True))
    return sorted_counts


@router.get(
    "/{net}/misc/validators-failed-rounds",
    response_class=JSONResponse,
)
async def get_validators_failed_rounds(
    request: Request,
    net: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    if net not in ["mainnet"]:
        raise HTTPException(
            status_code=404,
            detail="Mainnet only.",
        )

    doc = (mongodb.mainnet[Collections.helpers].find_one({"_id": "last_known_payday"})) or {}
    latest_payday_block: CCD_BlockInfo = grpcclient.get_block_info(block_input=doc["hash"])

    pipeline = [
        {"$match": {"genesis_index": latest_payday_block.genesis_index}},
        {"$match": {"epoch": {"$gte": latest_payday_block.epoch}}},
        {"$sort": {"epoch": 1}},  # ensure lowest epoch gets hour_index = 1
    ]

    result = mongodb.mainnet_db["paydays_v2_validators_missed"].aggregate(pipeline)

    cumulative = {}
    entries = []

    for i, entry in enumerate(result):
        entry["hour_index"] = i + 1

        missed = {}
        current_missed = entry.get("missed_rounds_count", {})

        # Add all known validators so far to `missed`, even if missed_epoch is 0
        validator_ids = set(cumulative.keys()) | set(current_missed.keys())

        for validator_id in validator_ids:
            missed_epoch = current_missed.get(validator_id, 0)
            cumulative[validator_id] = cumulative.get(validator_id, 0) + missed_epoch

            missed[validator_id] = {
                "missed_epoch": missed_epoch,
                "missed_payday": cumulative[validator_id],
            }

        entry["missed"] = missed
        entries.append(entry)

    return entries

    # @router.get("/{net}/misc/search/{search_term}", response_class=JSONResponse)
    # async def get_accounts_search(
    #     request: Request,
    #     net: str,
    #     search_term: str,
    #     mongomotor: MongoMotor = Depends(get_mongo_motor),
    #     api_key: str = Security(API_KEY_HEADER),
    # ) -> dict:
    #     """
    #     Endpoint to get to search for everything.

    #     """
    #     db_to_use = mongomotor.testnet if net == "testnet" else mongomotor.mainnet
    #     search_result = {}

    #     # # Accounts
    #     # search_on_address = (
    #     #     await db_to_use[Collections.all_account_addresses]
    #     #     .find({"_id": {"$regex": re.compile(r"{}".format(search_term))}})
    #     #     .to_list(length=5)
    #     # )
    #     # search_on_index = (
    #     #     await db_to_use[Collections.all_account_addresses]
    #     #     .find(
    #     #         {
    #     #             "$expr": {
    #     #                 "$regexMatch": {
    #     #                     "input": {"$toString": "$account_index"},
    #     #                     "regex": f"{search_term}",
    #     #                 }
    #     #             }
    #     #         }
    #     #     )
    #     #     .to_list(length=5)
    #     # )

    #     # search_accounts = search_on_address + search_on_index

    #     # search_result["accounts"] = search_accounts
    #     # search_term = str(search_term)[:3]
    #     # Blocks


#     caret = re.compile(r"{}$".format(search_term))
#     search_on_blocks = (
#         await db_to_use[Collections.blocks].find(
#             {"_id": {"$regex": f"/{caret.pattern}/"}}
#         )
#         # .find({"_id": {"$regex": re.compile(r"^{}".format(search_term))}})
#         .to_list(length=5)
#     )
#     search_result["blocks"] = search_on_blocks

#     # # Transactions
#     # search_on_transactions = (
#     #     await db_to_use[Collections.transactions]
#     #     .find({"_id": {"$regex": re.compile(r"{}".format(search_term))}})
#     #     .to_list(length=5)
#     # )
#     # search_result["transactions"] = search_on_transactions
#     return search_result
