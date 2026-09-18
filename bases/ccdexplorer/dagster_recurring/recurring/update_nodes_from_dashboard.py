import datetime as dt

import httpx2 as httpx
from ccdexplorer.domain.node import ConcordiumNodeFromDashboard
from ccdexplorer.mongodb import Collections, MongoDB, net_db
from pymongo import ReplaceOne
from pymongo.collection import Collection


def perform_update_nodes_from_dashboard(context, mongodb: MongoDB, net: str) -> int:
    len_nodes = 0
    db: dict[Collections, Collection] = net_db(mongodb, net)
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

            # Upsert first, then remove only what is no longer reporting.
            #
            # This used to delete_many({}) and then bulk_write. Those are two
            # separate operations, so between them the collection was empty --
            # every minute, for as long as the write took. Readers get no
            # error from that, just nothing: the nodes page renders an empty
            # table, the API returns [], and a refresh a second later looks
            # fine, which is exactly how it was reported.
            #
            # An empty response from the dashboard must not empty the
            # collection either: bulk_write([]) raises, and deleting on the
            # strength of a bad fetch would throw away every node until the
            # next successful run.
            if not queue:
                context.log.warning(
                    f"{net} | dashboard returned no nodes; keeping the existing "
                    f"{db[Collections.dashboard_nodes].count_documents({})} on record"
                )
                return len_nodes

            _ = db[Collections.dashboard_nodes].bulk_write(queue)
            reporting_ids = [op._filter["_id"] for op in queue]
            removed = db[Collections.dashboard_nodes].delete_many({"_id": {"$nin": reporting_ids}})
            if removed.deleted_count:
                context.log.info(f"{net} | {removed.deleted_count} node(s) stopped reporting")
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
