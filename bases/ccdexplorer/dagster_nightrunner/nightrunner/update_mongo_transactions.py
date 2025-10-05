from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    write_queue_to_collection,
    get_start_end_block_from_date,
)


def perform_data_for_mongo_transactions(context, d_date: str, mongodb: MongoDB) -> dict:
    analysis = AnalysisType.statistics_mongo_transactions
    _id = f"{d_date}-{analysis.value}"
    queue = []
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
    dd = mongodb.mainnet[Collections.transactions].aggregate(pipeline)
    contents_day_list = list(dd)
    contents = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
    }
    contents_update = {x["_id"]: int(x["count"]) for x in contents_day_list}
    contents.update(contents_update)

    pipeline = [
        {
            "$match": {
                "block_info.height": {
                    "$gte": height_for_first_block,
                    "$lte": height_for_last_block,
                }
            }
        },
        {"$sortByCount": "$type.type"},
    ]
    dd = mongodb.mainnet[Collections.transactions].aggregate(pipeline)
    type_day_list = list(dd)
    contents_update = {x["_id"]: int(x["count"]) for x in type_day_list}
    contents.update(contents_update)

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
