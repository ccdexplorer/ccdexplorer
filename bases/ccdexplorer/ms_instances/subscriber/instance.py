from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ContractAddress,
    CCD_ContractTraceElement_Upgraded,
)
from ccdexplorer.cis import build_cis_support
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, net_db
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
        db_to_use = net_db(self, net)
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

    def add_cis_support(
        self,
        net: NET,
        db_to_use: dict[Collections, Collection],
        instance_info: dict,
        instance_as_class: CCD_ContractAddress,
    ) -> None:
        """Resolve which CIS standards this instance supports and store it on it.

        Doing it here is what keeps it off the request path: the API would
        otherwise invoke `supports` once per standard on every contract page
        view, nine blocking gRPC calls for an answer that only changes when the
        instance is upgraded.

        Most instances cost nothing to resolve -- if the module exports no
        `supports` entrypoint, ms_modules already recorded that and no node
        call is made. A resolution that fails writes nothing, so the next
        reader retries rather than caching a failure as an answer.
        """
        module = db_to_use[Collections.modules].find_one(
            {"_id": instance_info.get("source_module")}
        )
        cis_support = build_cis_support(
            self.grpc_client,
            net,
            instance_info,
            module,
            instance_as_class.index,
            instance_as_class.subindex,
        )
        if cis_support is not None:
            instance_info["cis_support"] = cis_support

    async def process_new_instance(self, net: NET, instance_as_class: CCD_ContractAddress):
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter

        db_to_use = net_db(self, net)

        instance_info_grpc = self.grpc_client.get_instance_info(
            instance_as_class.index,
            instance_as_class.subindex,
            "last_final",
            net,
        )
        instance_info: dict = instance_info_grpc.model_dump(exclude_none=True)
        instance_ref = instance_as_class.to_str()
        instance_info.update({"_id": instance_ref})

        # Exactly one of v0/v1 is present.
        if "v0" in instance_info:
            _source_module = instance_info["v0"]["source_module"]
        else:
            _source_module = instance_info["v1"]["source_module"]

        instance_info.update({"source_module": _source_module})  # type: ignore
        self.add_cis_support(net, db_to_use, instance_info, instance_as_class)
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
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter

        db_to_use = net_db(self, net)

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

        # The entrypoint that answers `supports` is code, and the code just
        # changed -- so whatever was cached describes the old module. Drop it
        # first, so a failed re-resolve leaves no answer rather than a stale
        # one, then resolve against the module now in force.
        instance_as_class.cis_support = None
        instance_info = instance_as_class.model_dump(exclude_none=True)
        self.add_cis_support(net, db_to_use, instance_info, upgraded_effect.address)

        _ = db_to_use[Collections.instances].bulk_write(
            [
                ReplaceOne(
                    {"_id": upgraded_effect.address.to_str()},
                    instance_info,
                    upsert=True,
                )
            ]
        )
        tooter_message = f"{net.value}: Instance processed {upgraded_effect.address.to_str()} upgraded from module {upgraded_effect.from_module} to module {upgraded_effect.to_module}."
        console.log(tooter_message)
        self.tooter.send_to_tooter(tooter_message)
