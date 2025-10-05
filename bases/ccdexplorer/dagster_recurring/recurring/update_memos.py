import json
import cbor2
from ccdexplorer.mongodb import Collections, MongoDB

from pymongo import ReplaceOne
from pymongo.collection import Collection


def decode_memo(hex: str):
    # try:
    #     decoded = cbor2.loads(bytes.fromhex(hex))
    #     if isinstance(decoded, str):
    #         decoded = decoded.lower()
    #     elif isinstance(decoded, int):
    #         decoded = str(decoded)
    #     else:
    #         try:
    #             decoded = str(decoded.value)
    #         except:  # noqa: E722
    #             pass

    #     return None, decoded
    # except:  # noqa: E722
    #     return "Decoding failure...", None

    raw = bytes.fromhex(hex)
    try:
        return None, str(cbor2.loads(raw))
    except Exception:
        try:
            return None, str(json.loads(raw.decode("utf-8")))
        except Exception:
            return None, str(raw)


def log_last_heartbeat_memo_to_hashes_in_mongo(db: dict[Collections, Collection], height: int):
    query = {"_id": "heartbeat_memos_last_processed_block"}
    db[Collections.helpers].replace_one(
        query,
        {
            "_id": "heartbeat_memos_last_processed_block",
            "height": height,
        },
        upsert=True,
    )


def update_memos_to_hashes(context, mongodb: MongoDB, net: str):
    dct = {}
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    # Read heartbeat_memos_last_processed_block
    result = db[Collections.helpers].find_one({"_id": "heartbeat_memos_last_processed_block"})
    # If it's not set, set to -1, which leads to resetting
    # memo search.
    if result:
        heartbeat_memos_last_processed_block = result["height"]
    else:
        heartbeat_memos_last_processed_block = -1

    pipeline = [
        {"$match": {"block_height": {"$gt": heartbeat_memos_last_processed_block}}},
        {"$match": {"memo": {"$exists": True}}},
        {"$project": {"memo": 1, "block_height": 1, "_id": 1}},
    ]
    result = db[Collections.involved_accounts_transfer].aggregate(pipeline)

    data = list(result)
    memos: list[dict] = []
    max_block_height = 0
    context.log.info(f"{net} | count of memo txs: {len(data)}")
    if len(data) > 0:
        for x in data:
            max_block_height = max(max_block_height, x["block_height"])
            hex_to_decode = x["memo"]

            decode_error, decoded_memo = decode_memo(hex_to_decode)
            if not decode_error:
                memos.append(
                    {
                        "memo": (
                            decoded_memo.lower() if isinstance(decoded_memo, str) else decoded_memo
                        ),
                        "hash": x["_id"],
                    }
                )

        if len(memos) > 0:
            # only if there are new memos to add to the list, should we read in
            # the collection again, otherwise it's a waste of resources.

            # this is the queue of collection documents to be added and/or replaced
            queue = []
            # if we find new memos, add them here, this leads to a new document in the queue
            new_memos = {}
            # if we find an existing memo, add them here, this leads to a replace document in the queue.
            updated_memos = {}

            set_memos = {x["_id"]: x["tx_hashes"] for x in db[Collections.memos_to_hashes].find({})}
            # old_len_set_memos = len(set_memos)
            for memo in memos:
                current_list_of_tx_hashes_for_memo = set_memos.get(memo["memo"], None)

                # a tx with a memo we have already seen
                # hence we need to replace the current document with the updated one
                # as we possibly can have multiple updates (txs) to the same memo
                # we need to store the updates in a separate variable.
                if current_list_of_tx_hashes_for_memo:
                    current_list_of_tx_hashes_for_memo.append(memo["hash"])

                    updated_memos[memo["memo"]] = list(set(current_list_of_tx_hashes_for_memo))

                # this is a new memo
                else:
                    new_memos[memo["memo"]] = [memo["hash"]]

            # make list for new and updated items
            for memo_key, tx_hashes in updated_memos.items():
                queue.append(
                    ReplaceOne(
                        {"_id": memo_key},
                        replacement={
                            "_id": memo_key,
                            "tx_hashes": tx_hashes,
                        },
                        upsert=True,
                    )
                )

            for memo_key, tx_hashes in new_memos.items():
                queue.append(
                    ReplaceOne(
                        {"_id": memo_key},
                        replacement={
                            "_id": memo_key,
                            "tx_hashes": tx_hashes,
                        },
                        upsert=True,
                    )
                )

            db[Collections.memos_to_hashes].bulk_write(queue)
            log_last_heartbeat_memo_to_hashes_in_mongo(db, max_block_height)

            context.log.info(
                f"{net} | Last block height processed: {max_block_height:,.0f}.\nAdded {len(new_memos):,.0f} key(s) in this run and updated {len(updated_memos):,.0f} key(s). New memo(s): {','.join(new_memos.keys())}."
            )
            dct = {
                "new_memos": new_memos,
                "updated_memos": updated_memos,
                "last_block_height_processed": max_block_height,
            }
    return dct
