import math

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_TokenEvent,
    CCD_TokenInfo,
)
from ccdexplorer.cis import agent_registry_contracts
from ccdexplorer.mongodb import Collections, CollectionsUtilities, MongoDB
from pymongo import ReplaceOne
from ccdexplorer.tooter import Tooter

try:
    from ..nightrunner.utils import (
        AnalysisType,
        get_start_end_block_from_date,
        write_queue_to_collection,
    )
except ImportError:
    from ccdexplorer.dagster_nightrunner.nightrunner.utils import (
        AnalysisType,
        get_start_end_block_from_date,
        write_queue_to_collection,
    )


def get_agent_registry_events_for_day(
    start_block: int,
    end_block: int,
    mongodb: MongoDB,
    registries: list[str],
):
    """Mint events on the agent registries during one day.

    `registries` is derived from which contracts report supporting CIS-8004,
    not from a list kept by hand -- a new registry is counted from the day it
    is deployed, without anyone remembering to add it.
    """
    if not registries:
        return []
    pipeline = [
        {"$match": {"tx_info.block_height": {"$gte": start_block, "$lte": end_block}}},
        {
            "$match": {
                "event_info.contract": {"$in": registries},
                "event_info.event_type": "CIS-2.mint_event",
            }
        },
    ]
    txs = [x for x in mongodb.mainnet[Collections.tokens_logged_events_v2].aggregate(pipeline)]
    return txs


def perform_agent_registry_statistics_update(context, d_date: str, mongodb: MongoDB) -> dict:
    analysis = AnalysisType.statistics_agent_registry

    _id = f"{d_date}-{analysis.value}"
    queue = []
    height_for_first_block, height_for_last_block = get_start_end_block_from_date(mongodb, d_date)
    registries = agent_registry_contracts(mongodb.mainnet[Collections.instances])
    contents = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "agents_registered": 0,
        # Recorded per day so a change in the registry set is visible in the
        # series rather than silently rewriting history.
        "registries": registries,
    }
    events = get_agent_registry_events_for_day(
        height_for_first_block, height_for_last_block, mongodb, registries
    )
    contents["agents_registered"] = len(events)
    if not registries:
        context.log.warning(
            "no CIS-8004 registries resolved; counting zero agents registered. "
            "Instances need cis_support populated (scripts/backfill_cis_support.py)."
        )

    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=contents,
            upsert=True,
        )
    )

    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(f"info: {contents}")
    return {"dct": contents}
