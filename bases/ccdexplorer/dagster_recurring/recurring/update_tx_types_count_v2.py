import datetime as dt

from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from pymongo.collection import Collection
from ccdexplorer.tooter import Tooter


def update_tx_types_count_hourly(
    context, mongodb: MongoDB, net: str, start: dt.datetime, end: dt.datetime
):
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    if context:
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
    if len(result) == 0:
        # No transactions in this period
        dd = {
            "counts": {},
            "total": 0,
        }
    else:
        dd = result[0]
    dd.update(
        {
            "_id": f"{start.strftime('%Y-%m-%d-%H')}-{end.strftime('%H')}",
            "hour": int(start.strftime("%H")),
            "date": start.strftime("%Y-%m-%d"),
        }
    )

    _ = db[Collections.tx_types_count].bulk_write([ReplaceOne({"_id": dd["_id"]}, dd, upsert=True)])

    if context:
        context.log.info(f"{net} | {dd}.")
    return dd


tooter: Tooter = Tooter()
mongodb: MongoDB = MongoDB(tooter, nearest=True)


if __name__ == "__main__":
    # d_date = "2025-12-25"
    # for start_hour in range(24):
    #     dd: dict = update_tx_types_count_hourly(
    #         None,
    #         mongodb,
    #         "mainnet",
    #         dt.datetime.fromisoformat(d_date) + dt.timedelta(hours=start_hour),
    #         dt.datetime.fromisoformat(d_date) + dt.timedelta(hours=start_hour + 1),
    #     )
    #     print(f"Processed mainnet {d_date} hour {start_hour}, with total {dd['total']}")

    start_date = dt.date(2022, 6, 9)

    # End at today 08:00
    now = dt.datetime.now().astimezone(dt.timezone.utc)
    end_dt = now.replace(hour=8, minute=0, second=0, microsecond=0)
    if now < end_dt:
        # If it's before 08:00 now, stop at yesterday 08:00
        end_dt -= dt.timedelta(days=1)

    current_date = start_date
    sum_total = 0
    net = "testnet"
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    coll = db[Collections.transactions]
    print("Mongo target:", coll.database.name, coll.name)
    print("tx docs:", coll.estimated_document_count())
    while True:
        day_start = dt.datetime.combine(current_date, dt.time.min).replace(tzinfo=dt.timezone.utc)
        day_end = day_start + dt.timedelta(days=1)

        # Stop once the day start exceeds our end window
        if day_start >= end_dt:
            break

        daily_total = 0

        for start_hour in range(24):
            hour_start = day_start + dt.timedelta(hours=start_hour)
            hour_end = hour_start + dt.timedelta(hours=1)

            # Do not go past the final cutoff (today 08:00)
            if hour_start >= end_dt:
                break

            dd: dict = update_tx_types_count_hourly(
                None,
                mongodb,
                net,
                hour_start,
                hour_end,
            )

            daily_total += dd.get("total", 0)
        sum_total += daily_total
        print(
            f"Processed {net} {current_date.isoformat()} → total txs: {daily_total:,.0f} - cumulative: {sum_total:,.0f}"
        )

        current_date += dt.timedelta(days=1)

if __name__ == "__main__":
    