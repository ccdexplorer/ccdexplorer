from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ContractAddress,
    CCD_ContractTraceElement_Upgraded,
)
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections
from ccdexplorer.domain.mongo import MongoTypeInstance
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.tooter import Tooter
from pymongo import ReplaceOne
from pymongo.collection import Collection
from rich.console import Console
from ccdexplorer.env import RUN_ON_NET

console = Console()


class Instance:
    async def process_block_for_instances(self, payload, net: NET):
        db_to_use = self.testnet if net.value == "testnet" else self.mainnet
        block_height = payload.get("height")
        assert block_height is not None

        pipeline = [
            {"$match": {"block_info.height": block_height}},
        ]
        txs = [
            CCD_BlockItemSummary(**x)
            for x in db_to_use[Collections.transactions].aggregate(pipeline)
        ]
        for tx in txs:
            if tx.account_transaction:
                if tx.account_transaction.effects.contract_initialized:
                    await self.process_new_instance(
                        NET(RUN_ON_NET),
                        tx.account_transaction.effects.contract_initialized.address,
                    )
                if tx.account_transaction.effects.contract_update_issued:
                    for effect in tx.account_transaction.effects.contract_update_issued.effects:
                        if effect.upgraded:
                            await self.process_upgraded_instance(NET(RUN_ON_NET), effect.upgraded)

    async def try_adding_project(
        self, instance_info: dict, db_to_use: dict[Collections, Collection]
    ):
        # now find all contract from the module
        module_from_project = db_to_use[Collections.projects].find_one(
            {"module_ref": instance_info["source_module"]}
        )
        if module_from_project:
            project_id = module_from_project["project_id"]
            contract_address = instance_info["_id"]
            contract_as_class = CCD_ContractAddress.from_str(contract_address)
            _id = f"{project_id}-address-{contract_address}"
            d_address = {"project_id": project_id}
            d_address.update(
                {
                    "_id": _id,
                    "type": "contract_address",
                    "contract_index": contract_as_class.index,
                    "contract_address": contract_address,
                }
            )
            _ = db_to_use[Collections.projects].bulk_write(
                [
                    ReplaceOne(
                        {"_id": _id},
                        replacement=d_address,
                        upsert=True,
                    )
                ]
            )
            tooter_message = f"Contract {contract_address} classified as '{project_id}'."
            console.log(tooter_message)
            self.tooter.send_to_tooter(tooter_message)

    async def process_new_instance(self, net: NET, instance_as_class: CCD_ContractAddress):
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter

        db_to_use = self.testnet if net.value == "testnet" else self.mainnet

        instance_info_grpc = self.grpc_client.get_instance_info(
            instance_as_class.index,
            instance_as_class.subindex,
            "last_final",
            net,
        )
        instance_info: dict = instance_info_grpc.model_dump(exclude_none=True)
        instance_ref = instance_as_class.to_str()
        instance_info.update({"_id": instance_ref})

        if instance_info["v0"]["source_module"] == "":
            del instance_info["v0"]
            _source_module = instance_info["v1"]["source_module"]
        if instance_info["v1"]["source_module"] == "":
            del instance_info["v1"]
            _source_module = instance_info["v0"]["source_module"]

        instance_info.update({"source_module": _source_module})  # type: ignore
        _ = db_to_use[Collections.instances].bulk_write(
            [ReplaceOne({"_id": instance_ref}, instance_info, upsert=True)]
        )
        if net == "mainnet":
            await self.try_adding_project(instance_info, db_to_use)

        tooter_message = f"{net.value}: New instance processed {instance_ref}."
        print(tooter_message)
        self.tooter.send_to_tooter(tooter_message)

    async def process_upgraded_instance(
        self, net: NET, upgraded_effect: CCD_ContractTraceElement_Upgraded
    ):
        self.motor_mainnet: dict[Collections, Collection]
        self.motor_testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter

        db_to_use = self.motor_testnet if net == "testnet" else self.motor_mainnet

        instance_as_class = db_to_use[Collections.instances].find_one(
            {"_id": upgraded_effect.address.to_str()}
        )
        if instance_as_class:
            instance_as_class = MongoTypeInstance(**instance_as_class)
        else:
            tooter_message = f"{net}: Instance {upgraded_effect.address.to_str()} to be upgraded could not be found."
            self.tooter.send_to_tooter(tooter_message)
            return

        instance_as_class.source_module = upgraded_effect.to_module
        if instance_as_class.v0:
            instance_as_class.v0.source_module = upgraded_effect.to_module
        elif instance_as_class.v1:
            instance_as_class.v1.source_module = upgraded_effect.to_module

        _ = db_to_use[Collections.instances].bulk_write(
            [
                ReplaceOne(
                    {"_id": upgraded_effect.address.to_str()},
                    instance_as_class.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
        tooter_message = f"{net.value}: Instance processed {upgraded_effect.address.to_str()} upgraded from module {upgraded_effect.from_module} to module {upgraded_effect.to_module}."
        console.log(tooter_message)
        self.tooter.send_to_tooter(tooter_message)
