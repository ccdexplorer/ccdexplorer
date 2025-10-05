from ccdexplorer.ccdscan import CCDScan
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    write_queue_to_collection,
)

# tooter: Tooter = Tooter()
# mongodb: MongoDB = MongoDB(tooter, nearest=True)


def get_hash_from_date(date: str, mongodb: MongoDB) -> str:
    result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": date})
    if result:
        return result["hash_for_last_block"]
    else:
        return ""


def perform_data_for_release_amounts(
    context, d_date: str, mongodb: MongoDB, ccdscan: CCDScan
) -> dict:
    """ """

    queue = []
    analysis = AnalysisType.statistics_release_amounts

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)
    block_hash = get_hash_from_date(d_date, mongodb)
    result = ccdscan.ql_request_block_for_release(block_hash)
    if not result:
        context.log.error(f"No data found for {d_date}.")
        return {}

    dct = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "block_hash": block_hash,
        "block_height": int(result["blockHeight"]),
        "total_amount": result["balanceStatistics"]["totalAmount"],
        "total_amount_released": result["balanceStatistics"]["totalAmountReleased"],
    }

    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )
    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(f"info: {dct}")
    return {"dct": dct}
