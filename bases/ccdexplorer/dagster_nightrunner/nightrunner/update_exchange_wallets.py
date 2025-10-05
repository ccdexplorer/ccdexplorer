from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    write_queue_to_collection,
    get_exchanges,
)


def get_wallets_for_exchange(exchange_addresses: list[str], d_date: str, mongodb: MongoDB) -> int:
    pipeline = [
        {"$match": {"impacted_address_canonical": {"$in": exchange_addresses}}},
        {"$match": {"date": {"$lte": d_date}}},
        {"$project": {"impacted_address": 1, "_id": 0}},
    ]
    addresses = [
        x["impacted_address"]
        for x in mongodb.mainnet[Collections.impacted_addresses].aggregate(pipeline)
    ]
    return len(set(addresses))


def perform_data_for_exchange_wallets(context, d_date: str, mongodb: MongoDB) -> dict:
    """ """
    exchanges = get_exchanges(mongodb)

    analysis = AnalysisType.statistics_exchange_wallets
    queue = []

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)

    exchange_wallet_count = {}
    for key, address_list in exchanges.items():
        exchange_wallet_count[key] = get_wallets_for_exchange(address_list, d_date, mongodb)

    dct = {
        "_id": _id,
        "type": analysis.value,
        "date": d_date,
    }

    for key in exchanges:
        dct.update({key: exchange_wallet_count[key]})

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

    context.log.info(f"Exchange wallets: {dct}")
    write_queue_to_collection(mongodb, queue, analysis)
    return {"message": f"Exchange wallets: {dct}"}
