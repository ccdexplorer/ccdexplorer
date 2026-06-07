import math

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_TokenEvent,
    CCD_TokenInfo,
)
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
):
    pipeline = [
        {"$match": {"tx_info.block_height": {"$gte": start_block, "$lte": end_block}}},
        {
            "$match": {
                "event_info.contract": "<10082,0>",
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
    contents = {"_id": _id, "date": d_date, "type": analysis.value, "agents_registered": 0}
    events = get_agent_registry_events_for_day(
        height_for_first_block, height_for_last_block, mongodb
    )
    contents["agents_registered"] = len(events)

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
