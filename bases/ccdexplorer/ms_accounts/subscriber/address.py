from ccdexplorer.domain.generic import NET, AccountInfoStable
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    Collections,
)
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary, CCD_AccountInfo
from ccdexplorer.tooter import Tooter
from pymongo import ReplaceOne
from pymongo.collection import Collection
from rich.console import Console

console = Console()


class Address:
    async def process_new_address(self, net: NET, block_height: int):
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter

        db_to_use = self.testnet if net.value == "testnet" else self.mainnet

        try:
            pipeline = [
                {"$match": {"block_info.height": block_height}},
                {"$match": {"account_creation": {"$exists": True}}},
            ]
            txs = [
                CCD_BlockItemSummary(**x)
                for x in db_to_use[Collections.transactions].aggregate(pipeline)
            ]
            for tx in txs:
                if tx.account_creation is None:
                    continue
                new_address = tx.account_creation.address
                account_info: CCD_AccountInfo = self.grpc_client.get_account_info(
                    "last_final", hex_address=new_address, net=net
                )
                ai_stable = AccountInfoStable.from_account_info(account_info)
                canonical_address = ai_stable.account_address[:29]

                _ = db_to_use[Collections.all_account_addresses].bulk_write(
                    [ReplaceOne({"_id": canonical_address}, ai_stable.to_collection(), upsert=True)]
                )
                tooter_message = f"{net.value}: New address processed {new_address} at index {account_info.index}."
                console.log(tooter_message)
                self.tooter.send_to_tooter(tooter_message)
        except Exception as e:
            tooter_message = f"{net.value}: New address failed with error  {e}."
            console.log(tooter_message)
            self.tooter.send_to_tooter(tooter_message)
            return
