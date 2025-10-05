from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    find_new_instances_from_project_modules,
    get_start_end_block_from_date,
    write_queue_to_collection,
)

# tooter: Tooter = Tooter()
# mongodb: MongoDB = MongoDB(tooter, nearest=True)


def perform_data_for_tx_types(context, d_date: str, mongodb: MongoDB) -> dict:
    """
    Calculate transaction type counts for the chain. This previously
    was also used for projects, hence the references to project_id.
    Now project_id is always "all".
    """

    queue = []
    analysis = AnalysisType.statistics_transaction_types
    find_new_instances_from_project_modules(mongodb)

    queue = []
    project_id = "all"

    _id = f"{d_date}-{analysis.value}-{project_id}"
    context.log.info(_id)
    height_for_first_block, height_for_last_block = get_start_end_block_from_date(mongodb, d_date)
    pipeline = [
        {
            "$match": {
                "block_info.height": {
                    "$gte": height_for_first_block,
                    "$lte": height_for_last_block,
                }
            }
        },
        {"$sortByCount": "$type.contents"},
    ]
    dd = list(mongodb.mainnet[Collections.transactions].aggregate(pipeline))
    tx_types = {}
    if len(dd) > 0:
        tx_types = {x["_id"]: x["count"] for x in dd}

    dct = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "project": project_id,
        "tx_type_counts": tx_types,
    }

    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )
    if len(queue) > 0:
        _ = mongodb.mainnet[Collections.statistics].bulk_write(queue)
        queue = []

    context.log.info(f"tx_type_counts: {tx_types}")
    write_queue_to_collection(mongodb, queue, analysis)
    return {"tx_type_counts": tx_types}
