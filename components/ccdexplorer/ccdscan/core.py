import datetime as dt
from ccdexplorer.domain.generic import NET
import requests

NODES_REQUEST_LIMIT = 20


class CCDScan:
    # https://github.com/Concordium/concordium-scan/tree/e95e8b2b191fefcf381ef4b4a1c918dd1f11ae05/frontend/src/queries
    def __init__(self, tooter):
        self.tooter = tooter
        self.nodes_request_limit = NODES_REQUEST_LIMIT
        self.explorer_ccd_request_timestamp = dt.datetime.utcnow() - dt.timedelta(seconds=10)
        self.explorer_ccd_request_timestamp_delegators = dt.datetime.utcnow() - dt.timedelta(
            seconds=10
        )
        self.graphql_url = "https://api-ccdscan.mainnet.concordium.software/graphql/"
        self.graphql_url_testnet = "https://testnet.api.ccdscan.io/graphql"

    def ql_request_block_for_release(self, blockHash: str, net=NET.MAINNET):
        query = (
            "query {\n"
            f'blockByBlockHash(blockHash:"{blockHash}") \u007b \n'
            "blockHeight\n"
            "blockHash\n"
            "balanceStatistics {\n"
            "totalAmount\n"
            "totalAmountReleased\n"
            "}\n"
            "}\n"
            "}\n"
        )
        try:
            url_to_use = self.graphql_url if net == NET.MAINNET else self.graphql_url_testnet
            r = requests.post(url_to_use, json={"query": query})
            if r.status_code == 200:
                return r.json()["data"]["blockByBlockHash"]

        except Exception as e:
            print(query, e)
            return None
