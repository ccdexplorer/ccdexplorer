from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    get_start_end_block_from_date,
    write_queue_to_collection,
)

# tooter: Tooter = Tooter()
# mongodb: MongoDB = MongoDB(tooter, nearest=True)


def perform_data_for_tx_fees(context, d_date: str, mongodb: MongoDB) -> dict:
    """
    Calculate transaction fees per day.
    """

    queue = []
    analysis = AnalysisType.statistics_transaction_fees

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)
    height_for_first_block, height_for_last_block = get_start_end_block_from_date(mongodb, d_date)
    pipeline = [
        {"$match": {"account_transaction": {"$exists": True}}},
        {
            "$match": {
                "block_info.height": {
                    "$gte": height_for_first_block,
                    "$lte": height_for_last_block,
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "fee_for_day": {"$sum": "$account_transaction.cost"},
            }
        },
    ]
    result = mongodb.mainnet[Collections.transactions].aggregate(pipeline)
    ll = list(result)
    if len(ll) > 0:
        fee_for_day = ll[0]["fee_for_day"]
    else:
        fee_for_day = 0
    dct = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "fee_for_day": fee_for_day,
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

    context.log.info(f"tx_fees_for_day: {(fee_for_day / 1_000_000):,.0f} CCD")
    write_queue_to_collection(mongodb, queue, analysis)
    return {"message": f"tx_fees_for_day: {(fee_for_day / 1_000_000):,.0f} CCD"}
