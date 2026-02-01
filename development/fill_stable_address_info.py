import queue

from ccdexplorer.domain.generic import NET, AccountInfoStable
from ccdexplorer.env import RUN_ON_NET
from ccdexplorer.grpc_client.core import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB
from ccdexplorer.tooter import Tooter
from pymongo import ReplaceOne
from rich import print

tooter = Tooter()
mongodb = MongoDB(tooter)
grpc_client = GRPCClient()

net = "mainnet"


def send_queue_to_mongodb_stable_address_info(db_to_use, queue: list[dict]):
    try:
        if len(queue) > 0:
            db_to_use[Collections.stable_address_info].bulk_write(
                [
                    ReplaceOne(
                        {"_id": rec["_id"]},
                        rec,
                        upsert=True,
                    )
                    for rec in queue
                ]
            )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    db_to_use = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    queue: list[dict] = []
    for account_index in range(103899, 104000):
        # print(f"Processing account index {account_index}")
        try:
            ai = grpc_client.get_account_info(
                "last_final", account_index=account_index, net=NET(net)
            )
        except Exception as e:
            print(f"Failed to get account info for index {account_index}: {e}")
            continue

        ai_stable: AccountInfoStable = AccountInfoStable.from_account_info(ai)
        record = ai_stable.to_collection()
        queue.append(record)
        if len(queue) >= 1000:
            send_queue_to_mongodb_stable_address_info(db_to_use, queue)
            queue = []
            print(f"Processed up to account index {account_index}")
    if len(queue) > 0:
        send_queue_to_mongodb_stable_address_info(db_to_use, queue)
