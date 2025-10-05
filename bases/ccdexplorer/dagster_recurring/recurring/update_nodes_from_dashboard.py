import datetime as dt

import httpx
from ccdexplorer.domain.node import ConcordiumNodeFromDashboard
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from pymongo.collection import Collection


def perform_update_nodes_from_dashboard(context, mongodb: MongoDB, net: str) -> int:
    len_nodes = 0
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    with httpx.Client() as client:
        if net == "testnet":
            url = "https://dashboard.testnet.concordium.com/nodesSummary"
        else:
            url = "https://dashboard.mainnet.concordium.software/nodesSummary"

        response = client.get(url)
        context.log.info(f"{net} | Request took {response.elapsed.total_seconds()}s")
        if response.status_code == 200:
            t = response.json()
            queue = []
            len_nodes = len(t)
            for raw_node in t:
                node = ConcordiumNodeFromDashboard(**raw_node)
                d = node.model_dump()
                d["_id"] = node.nodeId

                for k, v in d.items():
                    if isinstance(v, int):
                        d[k] = str(v)

                queue.append(ReplaceOne({"_id": node.nodeId}, d, upsert=True))

            _ = db[Collections.dashboard_nodes].delete_many({})
            _ = db[Collections.dashboard_nodes].bulk_write(queue)
            #
            #
            # update nodes status retrieval
            query = {"_id": "heartbeat_last_timestamp_dashboard_nodes"}
            db[Collections.helpers].replace_one(
                query,
                {
                    "_id": "heartbeat_last_timestamp_dashboard_nodes",
                    "timestamp": dt.datetime.now().astimezone(tz=dt.timezone.utc),
                },
                upsert=True,
            )
        else:
            context.log.error(
                f"{net} |Failed to fetch data from {url}. Status code: {response.status_code}"
            )
            raise Exception(
                f"{net} |Failed to fetch data from {url}. Status code: {response.status_code}"
            )
    return len_nodes
