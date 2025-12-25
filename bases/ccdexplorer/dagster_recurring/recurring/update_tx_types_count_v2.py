import datetime as dt
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from pymongo.collection import Collection


def update_tx_types_count_hourly(
    context, mongodb: MongoDB, net: str, start: dt.datetime, end: dt.datetime
):
    dct = {}
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    context.log.info(
        f"{net} | Calculating tx count between {start.strftime('%H')} and {end.strftime('%H')}."
    )
    pipeline = [
        {
            "$match": {
                "block_info.slot_time": {"$gte": start, "$lt": end},
            }
        },
        # First: count per type
        {"$group": {"_id": "$type.contents", "count": {"$sum": 1}}},
        # Then: fold into a single document with an array of k/v pairs
        {
            "$group": {
                "_id": None,
                "counts_kv": {"$push": {"k": "$_id", "v": "$count"}},
                "total": {"$sum": "$count"},
            }
        },
        # Convert k/v array into an object: { "transfer": 123, "update": 456, ... }
        {
            "$project": {
                "_id": 0,
                "counts": {"$arrayToObject": "$counts_kv"},
                "total": 1,
            }
        },
    ]
    result = db[Collections.transactions].aggregate(pipeline)

    result = list(result)
    dd = result[0]
    dd.update(
        {
            "_id": f"{start.strftime('%Y-%m-%d-%H')}-{end.strftime('%H')}",
            "hour": int(start.strftime("%H")),
            "date": start.strftime("%Y-%m-%d"),
        }
    )

    _ = db[Collections.tx_types_count].bulk_write([ReplaceOne({"_id": dd["_id"]}, dd, upsert=True)])

    context.log.info(f"{net} | {dd}.")
    return dct
