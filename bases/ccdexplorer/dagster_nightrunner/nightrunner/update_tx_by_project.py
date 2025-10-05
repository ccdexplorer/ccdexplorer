from ccdexplorer.mongodb import Collections, MongoDB
from typing import Optional
from pydantic import BaseModel
from pymongo import ReplaceOne, UpdateOne
from pymongo.errors import OperationFailure

from ..nightrunner.utils import (
    AnalysisType,
    get_start_end_block_from_date,
    write_queue_to_collection,
    get_all_dates_with_info,
)


class AccountAddress(BaseModel):
    account_index: int
    account_address: str
    project_id: str


def get_project_account_addresses(mongodb: MongoDB):  # get account addresses
    pipeline = [
        {"$match": {"type": "account_address"}},
    ]
    project_addresses_in_collection = mongodb.mainnet[Collections.projects].aggregate(pipeline)
    project_addresses_in_collection = {
        AccountAddress(**address).account_address: AccountAddress(**address).project_id
        for address in list(project_addresses_in_collection)
    }
    return project_addresses_in_collection


def try_to_fill_unknown_txs(context, mongodb: MongoDB, optional_start_date: Optional[str] = None):
    """
    Try to fill txs that currently have no recognized_sender_id
    """
    project_addresses_in_collection = get_project_account_addresses(mongodb)
    project_addresses_set = set(project_addresses_in_collection.keys())
    dates: dict = get_all_dates_with_info(mongodb)
    if optional_start_date:
        # filter dates to only include those on or after optional_start_date
        dates = {day: info for day, info in dates.items() if day >= optional_start_date}
    for day, day_info in dates.items():
        try:
            pipeline = [
                {
                    "$match": {
                        "block_info.height": {
                            "$gte": day_info["height_for_first_block"],
                            "$lte": day_info["height_for_last_block"],
                        },
                        "recognized_sender_id": {"$exists": False},
                        "account_transaction": {"$exists": True},
                    }
                },
                {
                    "$group": {
                        "_id": "$account_transaction.sender",
                        "tx_ids": {"$push": "$_id"},
                    }
                },
            ]

            result = list(mongodb.mainnet[Collections.transactions].aggregate(pipeline))
            addresses_in_txs_with_ids = {doc["_id"]: doc["tx_ids"] for doc in result}
        except OperationFailure:
            # this exception is raised as a pymongo.errors.OperationFailure
            # for 2024-10-09 as there is an account with 99975 txs,
            # leading to a too large document size.
            pipeline = [
                {
                    "$match": {
                        "block_info.height": {
                            "$gte": day_info["height_for_first_block"],
                            "$lte": day_info["height_for_last_block"],
                        },
                        "recognized_sender_id": {"$exists": False},
                        "account_transaction": {"$exists": True},
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "account_transaction.sender": 1,
                    }
                },
            ]
            result = list(mongodb.mainnet[Collections.transactions].aggregate(pipeline))
            # a dict of ["account_transaction"]["sender"] keys with a list of _id values
            addresses_in_txs_with_ids = {}
            for tx in result:
                if tx["account_transaction"]["sender"] not in addresses_in_txs_with_ids:
                    addresses_in_txs_with_ids[tx["account_transaction"]["sender"]] = []
                addresses_in_txs_with_ids[tx["account_transaction"]["sender"]].append(tx["_id"])

        addresses_in_txs = set(addresses_in_txs_with_ids.keys())
        # Find the intersection of the addresses in the txs and the project addresses
        intersection = addresses_in_txs.intersection(project_addresses_set)
        context.log.info(day)
        if len(intersection) > 0:
            # print(day, len(intersection))
            bulk_updates = []
            for address in intersection:
                context.log.info(
                    f"Updating {len(addresses_in_txs_with_ids[address])} txs for {address} ({project_addresses_in_collection[address]})"
                )

                for tx_hash in addresses_in_txs_with_ids[address]:
                    bulk_updates.append(
                        UpdateOne(
                            {"_id": tx_hash},
                            {
                                "$set": {
                                    "recognized_sender_id": project_addresses_in_collection[address]
                                }
                            },
                        )
                    )

            # Perform bulk update
            if len(bulk_updates) > 0:
                mongodb.mainnet[Collections.transactions].bulk_write(bulk_updates)

                # Set complete to False for the dates where new data is available
                # These days will be reprocessed in the nightly statistics
                _ = mongodb.mainnet[Collections.statistics].bulk_write(
                    [
                        UpdateOne(
                            {"_id": f"statistics_transaction_types_by_project_dates-{day}"},
                            {"$set": {"complete": False}},
                        )
                    ]
                )
                context.log.info(
                    f"{day}: Day set to incomplete, as we have new recognized_sender_ids."
                )

    context.log.info("Done")
    _ = mongodb.mainnet[Collections.helpers].bulk_write(
        [
            UpdateOne(
                {"_id": "projects_try_to_find_recognized_sender_ids"},
                {"$set": {"value": False}},
            )
        ]
    )


def perform_data_for_tx_by_project(context, d_date: str, mongodb: MongoDB) -> dict:
    """
    Calculate statistics for tx counts by project per day.
    """

    queue = []
    analysis = AnalysisType.statistics_transaction_types_by_project

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)
    height_for_first_block, height_for_last_block = get_start_end_block_from_date(mongodb, d_date)

    pipeline = [
        {
            "$match": {
                "recognized_sender_id": {"$exists": True},
                "block_info.height": {
                    "$gte": height_for_first_block,
                    "$lte": height_for_last_block,
                },
            }
        },
        {
            "$group": {
                "_id": {
                    "project_id": "$recognized_sender_id",
                    "type_contents": "$type.contents",
                },
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id": "$_id.project_id",
                "type_counts": {"$push": {"type": "$_id.type_contents", "count": "$count"}},
            }
        },
        {"$project": {"_id": 0, "project_id": "$_id", "type_counts": 1}},
    ]

    dd = list(mongodb.mainnet[Collections.transactions].aggregate(pipeline))

    # Reformat the output
    tx_types = {}
    for entry in dd:
        project_id = entry["project_id"]
        type_counts = {item["type"]: item["count"] for item in entry["type_counts"]}
        tx_types[project_id] = type_counts

    for project_id, tx_types_for_project in tx_types.items():
        _id = f"{d_date}-{analysis.value}-{project_id}"
        dct = {
            "_id": _id,
            "date": d_date,
            "type": analysis.value,
            "project": project_id,
            "tx_type_counts": tx_types_for_project,
        }

        queue.append(
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        )
        context.log.info(f"{project_id}: {tx_types_for_project}")
    if len(queue) > 0:
        _ = mongodb.mainnet[Collections.statistics].bulk_write(queue)
        queue = []

    write_queue_to_collection(mongodb, queue, analysis)
    return {"message": "success."}
