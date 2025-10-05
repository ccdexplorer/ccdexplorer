import datetime as dt
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from pymongo.collection import Collection


def update_tx_types_count(context, mongodb: MongoDB, net: str):
    dct = {}
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    # this is the block we last processed making the list
    last_processed_block_for_list = db[Collections.helpers].find_one(
        {"_id": "heartbeat_last_block_processed_transactions_types_count"}
    )

    if not last_processed_block_for_list:
        context.log.info(f"{net} | No last processed block found for tx types count.")
        raise Exception(f"{net} | No last processed block found for tx types count.")

    last_processed_block_for_list_height = last_processed_block_for_list["height"]

    # this is the last finalized block in the collection of blocks
    heartbeat_last_processed_block = db[Collections.helpers].find_one(
        {"_id": "heartbeat_last_processed_block"}
    )

    if not heartbeat_last_processed_block:
        context.log.info(f"{net} | No last processed block found for tx types count.")
        raise Exception(f"{net} | No last processed block found for tx types count.")

    heartbeat_last_processed_block_height = heartbeat_last_processed_block["height"]

    context.log.info(
        f"{net} | Calculating tx count for {(heartbeat_last_processed_block_height - last_processed_block_for_list_height):,.0f} blocks."
    )
    pipeline = [
        {"$match": {"block_info.height": {"$gt": last_processed_block_for_list_height}}},
        {"$match": {"block_info.height": {"$lte": heartbeat_last_processed_block_height}}},
        # {"$match": {"account_transaction": {"$exists": True}}},
        {"$sort": {"type.contents": 1}},
        {"$group": {"_id": "$type.contents", "count": {"$sum": 1}}},
    ]
    result = db[Collections.transactions].aggregate(pipeline)

    local_queue = []

    # get previously stored results
    previous_result = db[Collections.tx_types_count].find({})
    previous_counts_dict = {x["_id"]: x["count"] for x in previous_result}

    if last_processed_block_for_list_height == -1:
        previous_counts_dict = {}

    new_results = {}
    updated_types = {}
    for r in result:
        if previous_counts_dict.get(r["_id"]):
            new_results[r["_id"]] = r["count"] + previous_counts_dict.get(r["_id"])
            updated_types[r["_id"]] = r["count"]
        else:
            new_results[r["_id"]] = r["count"]

    for k, v in new_results.items():
        local_queue.append(ReplaceOne({"_id": k}, {"count": v}, upsert=True))

    if len(local_queue) > 0:
        _ = db[Collections.tx_types_count].bulk_write(local_queue)

        query = {"_id": "heartbeat_last_block_processed_transactions_types_count"}
        db[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_last_block_processed_transactions_types_count",
                "height": heartbeat_last_processed_block_height,
            },
            upsert=True,
        )
        query = {"_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list"}
        db[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list",
                "timestamp": dt.datetime.now().astimezone(dt.timezone.utc),
            },
            upsert=True,
        )

    dct = {
        "updated_types": updated_types,
        "heartbeat_last_block_processed_transactions_types_count": heartbeat_last_processed_block_height,
    }
    context.log.info(f"{net} | {dct}.")
    return dct
