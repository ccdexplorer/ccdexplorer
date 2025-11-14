from ccdexplorer.mongodb import MongoDB, Collections
from ccdexplorer.grpc_client.core import GRPCClient
from typing import Optional
from ccdexplorer.tooter.core import TooterChannel, TooterType
from pydantic import BaseModel
import datetime as dt
from pymongo import ReplaceOne
from rich import print
from ccdexplorer.env import RUN_ON_NET
from ccdexplorer.tooter import Tooter

tooter = Tooter()
mongodb = MongoDB(tooter)
grpc_client = GRPCClient()

net = RUN_ON_NET


class FailureRecord(BaseModel):
    block_height: int
    date_done: dt.datetime
    error: Optional[str] = None
    queue: str
    status: str
    traceback: Optional[str] = None


def get_failures_last_x_blocks(x: int, net: str):
    db_to_use = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    last_block_in_collection = db_to_use[Collections.blocks].find_one({}, sort={"height": -1})
    if last_block_in_collection:
        pipeline = [
            {"$match": {"status": "FAILURE"}},
            {"$match": {"block_height": {"$gte": last_block_in_collection["height"] - x}}},
            {"$sort": {"block_height": -1}},
        ]
        result = db_to_use[Collections.celery_taskmeta].aggregate(pipeline)
        return list(result)
    else:
        return []


if __name__ == "__main__":
    db_to_use = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    failures = get_failures_last_x_blocks(100000, net)
    blocks_to_retry = set()
    for failure in failures:
        failure = FailureRecord(**failure)
        # check if this failure was already retried and/of fixed in a previous run
        pipeline = [
            {"$match": {"block_height": failure.block_height}},
            {"$match": {"queue": failure.queue}},
            {"$match": {"status": "SUCCESS"}},
            {"$limit": 1},
        ]
        result = list(db_to_use[Collections.celery_taskmeta].aggregate(pipeline))
        if len(result) > 0:
            print(f"Block Height: {failure.block_height}, Queue: {failure.queue},SKIPPED")
            continue  # already fixed
        else:
            blocks_to_retry.add(failure.block_height)
            print(f"Block Height: {failure.block_height}, Queue: {failure.queue},ADDED")
            tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"{net}: Adding block {failure.block_height:,.0f} to SR. Failure occurred at {failure.date_done} in {failure.queue}, with error {failure.error}.",
                notifier_type=TooterType.INFO,
            )

    db_to_use[Collections.helpers].bulk_write(
        [
            ReplaceOne(
                {"_id": "special_purpose_block_request"},
                {"_id": "special_purpose_block_request", "heights": list(blocks_to_retry)},
                upsert=True,
            )
        ]
    )
