import datetime as dt
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from pymongo.collection import Collection


def update_top_impacted_addresses(context, mongodb: MongoDB, net: str):
    dct = {}
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    # this is the block we last processed making the top list
    last_processed_block_for_top_list = db[Collections.helpers].find_one(
        {"_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list"}
    )

    if not last_processed_block_for_top_list:
        context.log.info(f"{net} | No last processed block found for top impacted addresses.")
        raise Exception(f"{net} | No last processed block found for top impacted addresses.")

    last_processed_block_for_top_list_height = last_processed_block_for_top_list["height"]

    # this is the last finalized block in the collection of blocks
    heartbeat_last_processed_block = db[Collections.helpers].find_one(
        {"_id": "heartbeat_last_processed_block"}
    )

    if not heartbeat_last_processed_block:
        context.log.info(f"{net} | No last processed block found for top impacted addresses.")
        raise Exception(f"{net} | No last processed block found for top impacted addresses.")

    heartbeat_last_processed_block_height = heartbeat_last_processed_block["height"]

    context.log.info(
        f"{net} | Calculating tx count for {(heartbeat_last_processed_block_height - last_processed_block_for_top_list_height):,.0f} blocks."
    )
    pipeline = [
        {  # 1) filter your block window
            "$match": {
                "block_height": {
                    "$gt": last_processed_block_for_top_list_height,
                    "$lte": heartbeat_last_processed_block_height,
                },
                "tx_hash": {"$exists": True},
                "effect_type": {"$exists": True},
            }
        },
        {  # 2) count each (address, effect) pair
            "$group": {
                "_id": {
                    "address": "$impacted_address_canonical",
                    "effect": "$effect_type",
                },
                "n": {"$sum": 1},
            }
        },
        {  # 3) regroup per address, collecting effects as array
            "$group": {
                "_id": "$_id.address",
                "effects": {"$push": {"effect": "$_id.effect", "count": "$n"}},
                "count": {"$sum": "$n"},
            }
        },
        {  # 4) sort & limit
            "$sort": {"count": -1},
        },
        {"$limit": 100},
    ]
    result = list(db[Collections.impacted_addresses].aggregate(pipeline))
    # turn effects-list → effects-dict for each entry
    for r in result:
        r["effects"] = {e["effect"]: e["count"] for e in r["effects"]}

    local_queue = []

    # --- load previous state ---
    prev_cursor = db[Collections.impacted_addresses_all_top_list].find({})
    previous_accounts = {
        doc["_id"]: {
            "count": doc.get("count", 0),
            "effects": doc.get("effects", {}),
        }
        for doc in prev_cursor
    }

    # first‐run guard
    if last_processed_block_for_top_list_height == -1:
        previous_accounts.clear()

    # --- merge new window + previous cumulative ---
    merged_results: dict[str, dict] = {}

    for r in result:
        addr = r["_id"]
        curr_count = r["count"]
        curr_effects = r["effects"]

        if addr in previous_accounts:
            prev = previous_accounts[addr]
            # 1) sum overall counts
            total_count = prev["count"] + curr_count
            # 2) merge per‐effect counts
            effs = prev["effects"].copy()
            for eff_type, cnt in curr_effects.items():
                effs[eff_type] = effs.get(eff_type, 0) + cnt
        else:
            total_count = curr_count
            effs = curr_effects.copy()

        merged_results[addr] = {
            "count": total_count,
            "effects": effs,
        }

    # carry forward any addresses with no new events
    for addr, prev in previous_accounts.items():
        if addr not in merged_results:
            merged_results[addr] = prev

    # --- sort by cumulative count and take top 100 ---
    sorted_addrs = sorted(
        merged_results.items(),
        key=lambda item: item[1]["count"],
        reverse=True,
    )

    top_100 = sorted_addrs[:100]

    # --- prepare bulk upsert ops ---
    local_queue = [
        ReplaceOne(
            {"_id": addr},
            {"count": data["count"], "effects": data["effects"]},
            upsert=True,
        )
        for addr, data in top_100
    ]

    # --- write them out & bump heartbeats ---
    if local_queue:
        db[Collections.impacted_addresses_all_top_list].delete_many({})
        db[Collections.impacted_addresses_all_top_list].bulk_write(local_queue)

        db[Collections.helpers].replace_one(
            {"_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list"},
            {
                "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list",
                "height": heartbeat_last_processed_block_height,
            },
            upsert=True,
        )
        db[Collections.helpers].replace_one(
            {"_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list"},
            {
                "_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list",
                "timestamp": dt.datetime.now(dt.timezone.utc),
            },
            upsert=True,
        )

    # --- compute diffs (counts only) ---
    sorted_totals = [(addr, data["count"]) for addr, data in sorted_addrs]
    increases, new_entries = {}, {}

    for addr, new_total in sorted_totals[:100]:
        prev = previous_accounts.get(addr)
        if prev is None:
            new_entries[addr] = new_total
        else:
            delta = new_total - prev["count"]
            if delta > 0:
                increases[addr] = delta

    context.log.info("📈 Increases over previous run:")
    for addr, inc in increases.items():
        context.log.info(f"  • {addr}: +{inc}")

    context.log.info("🆕 New entries in top-100:")
    for addr, tot in new_entries.items():
        context.log.info(f"  • {addr}: {tot}")

    # --- return for caller ---
    dct = {
        "sorted_totals": sorted_totals,
        "heartbeat_last_block_processed_impacted_addresses_all_top_list": heartbeat_last_processed_block_height,
    }
    context.log.info(f"{net} | {dct}")
    return dct
