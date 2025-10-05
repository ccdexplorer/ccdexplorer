# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false

import asyncio

from typing import Any


from ccdexplorer.domain.mongo import (
    MongoTypeLoggedEventV2,
    MongoTypeTokenAddress,
    MongoTypeTokenAddressV2,
    MongoTypeTokenForAddress,
    MongoTypeInstance,
    MongoTypeTokenLink,
)
from ccdexplorer.cis import CIS
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.domain.generic import NET
from ccdexplorer.mongodb import Collections
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
from pydantic import BaseModel
from pymongo import ASCENDING, ReplaceOne, DeleteOne
from pymongo.collection import Collection
from rich.console import Console

from enum import Enum

from ccdexplorer.env import RUN_ON_NET

console = Console()


async def publish_to_celery(processor: str, payload: dict[str, Any]) -> None:
    """
    Publish one task per processor to queue: {RUN_ON_NET}:queue:{processor}.
    Uses send_task so producer has zero dependency on worker code.
    """
    task_name = "process_block"

    # exact queue naming required:
    qname = f"{RUN_ON_NET}:queue:{processor}"

    # Send the task. Use .to_thread so we don't block the event loop on the small I/O.
    # Args are your payload; also include 'processor' explicitly so workers can route internally if desired.
    await asyncio.to_thread(
        celery_app.send_task,
        task_name,
        args=[],  # prefer kwargs for clarity
        kwargs={"processor": processor, "payload": payload},
        queue=qname,
    )


class LoggedEventsFromTX(BaseModel):
    for_ia: list[MongoTypeLoggedEventV2] = []
    for_collection: list[dict] = []


class AddressTypes(Enum):
    # Value is whether a canonical version should be stored
    account_or_contract = True
    public_key = False


########### Token Accounting V3
class TokenAccountingV2:
    async def events_process(self):
        (
            result,
            token_accounting_last_processed_block,
        ) = await self.find_events_to_process()
        await self.update_token_accounting_v2(
            result,
            token_accounting_last_processed_block,  # type: ignore
        )

    async def find_events_to_process(self):
        self.db: dict[Collections, Collection]
        # try:
        while self.sending:  # type: ignore
            await asyncio.sleep(0.3)
            print("waiting for sending to finish")
        # Read token_accounting_last_processed_block
        int_result = self.db[Collections.helpers].find_one(
            {"_id": "token_accounting_last_processed_block_v3"}
        )
        # If it's not set, set to -1, which leads to resetting
        # all token addresses and accounts, basically starting
        # over with token accounting.
        if int_result:
            token_accounting_last_processed_block = int_result["height"]
        else:
            token_accounting_last_processed_block = -1

        # Query the logged events collection for all logged events
        # after 'token_accounting_last_processed_block'.
        # Logged events are ordered by block_height, then by
        # transaction index (tx_index) and finally by event index
        # (ordering).
        pipeline = [
            {"$match": {"event_info.standard": "CIS-2"}},
            {"$match": {"tx_info.block_height": {"$gt": token_accounting_last_processed_block}}},
            {
                "$sort": {
                    "tx_info.block_height": ASCENDING,
                }
            },
            {"$limit": 1_000},
        ]
        result: list[MongoTypeLoggedEventV2] = [
            MongoTypeLoggedEventV2(**x)
            for x in self.db[Collections.tokens_logged_events_v2].aggregate(pipeline)
        ]
        return result, token_accounting_last_processed_block

    async def get_entrypoint(
        self, contract_address: CCD_ContractAddress, net: str, method_name: str
    ):
        self.entrypoint_cache: dict
        if not self.entrypoint_cache.get(contract_address.to_str()):
            result = self.db[Collections.instances].find_one({"_id": contract_address.to_str()})
            instance = MongoTypeInstance(**result)  # type: ignore

            entrypoint = instance.v1.name[5:] + "." + method_name  # type: ignore
            self.entrypoint_cache[contract_address.to_str()] = entrypoint
        else:
            entrypoint = self.entrypoint_cache[contract_address.to_str()]
        return entrypoint

    async def determine_token_amount(
        self,
        address_or_public_key: str,
        contract_address: CCD_ContractAddress,
        token_id: str,
        net: str,
    ):
        self.grpc_client: GRPCClient
        self.net: str

        entrypoint = await self.get_entrypoint(contract_address, net, "balanceOf")
        ci = CIS(
            self.grpc_client,
            contract_address.index,
            contract_address.subindex,
            entrypoint,
            NET(self.net),
        )
        rr, ii = ci.balanceOf(
            "last_final",
            token_id,
            [address_or_public_key],
        )

        if ii.failure.used_energy > 0:
            print(ii.failure.reason.model_dump(exclude_none=True))
            # this indicates that we had a lookup failure
            token_amount = -1
        else:
            token_amount = rr[0]
        return token_amount

    async def update_token_accounting_v2(
        self,
        net: str,
        block_height: int,
        save_process: bool = True,
    ):
        """
        This method takes logged events and processes them for
        token accounting. Note that token accounting only processes events with
        tag 255, 254, 253 and 251, which are transfer, mint, burn and metadata.
        The starting point is reading the helper document
        'token_accounting_last_processed_block', if that is either
        not there or set to -1, all token_addresses (and associated
        token_accounts) will be reset.
        """
        self.db: dict[Collections, Collection]

        pipeline = [
            {"$match": {"event_info.standard": "CIS-2"}},
            {"$match": {"tx_info.block_height": block_height}},
        ]
        result: list[MongoTypeLoggedEventV2] = [
            MongoTypeLoggedEventV2(**x)
            for x in self.db[Collections.tokens_logged_events_v2].aggregate(pipeline)
        ]

        if len(result) > 0:
            token_addresses_to_update = {}

            # Dict 'events_by_token_address' is keyed on token_address
            # and contains an ordered list of logged events related to
            # this token_address.
            events_by_token_address: dict[str, list] = {}
            for log in result:
                if log.recognized_event is None:
                    continue
                if log.recognized_event.tag in [247, 248, 249]:
                    continue

                # CIS-5 events do not have the token_address field set
                if "token_address" not in log.event_info.model_fields_set:
                    log.event_info.token_address = f"{log.recognized_event.cis2_token_contract_address}-{log.recognized_event.token_id}"  # type: ignore
                events_by_token_address[log.event_info.token_address] = (  # type: ignore
                    events_by_token_address.get(log.event_info.token_address, [])  # type: ignore
                )
                events_by_token_address[log.event_info.token_address].append(log)  # type: ignore

            console.log(
                f"Token accounting: {block_height:,.0f} - I found {len(result):,.0f} logged events on {self.net} to process from {len(list(events_by_token_address.keys())):,.0f} token addresses."
            )

            # Retrieve the token_addresses for all from the collection
            token_addresses_as_class_initial = {
                x["_id"]: MongoTypeTokenAddress(**x)
                for x in self.db[Collections.tokens_token_addresses_v2].find(
                    {"_id": {"$in": list(events_by_token_address.keys())}}
                )
            }

            links_to_save = []

            token_addresses_to_save = []
            for log in result:
                log: MongoTypeLoggedEventV2
                if log.recognized_event is None:
                    continue
                if log.event_info.token_address not in token_addresses_as_class_initial:
                    token_address_as_class = self.create_new_token_address_v2(
                        log.event_info.token_address,
                        log.tx_info.block_height,  # type: ignore
                    )
                    token_addresses_to_update[log.event_info.token_address] = token_address_as_class

                contract_ = (
                    log.event_info.contract
                    if log.recognized_event.tag > 250
                    else log.recognized_event.cis2_token_contract_address  # type: ignore
                )

                if log.recognized_event.tag == 252:
                    # this is an operatorUpdate event, doesn't have a token_id, nothing to do here.
                    continue

                token_id_ = log.recognized_event.token_id  # type: ignore
                addresses_to_save = []
                if log.recognized_event.tag == 255:
                    # transfer event
                    addresses_to_save.append(
                        {
                            "address": log.recognized_event.from_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )
                    addresses_to_save.append(
                        {
                            "address": log.recognized_event.to_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif log.recognized_event.tag == 254:
                    # mint event
                    addresses_to_save.append(
                        {
                            "address": log.recognized_event.to_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif log.recognized_event.tag == 253:
                    # burn event
                    addresses_to_save.append(
                        {
                            "address": log.recognized_event.from_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif log.recognized_event.tag == 248:
                    # deposit CIS2 tokens event (CIS-5)
                    addresses_to_save.append(
                        {
                            "address": log.recognized_event.from_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )
                    addresses_to_save.append(
                        {
                            "address": f"{log.event_info.contract}-{log.recognized_event.to_public_key_ed25519}",
                            "address_type": AddressTypes.public_key,
                        }
                    )

                elif log.recognized_event.tag == 246:
                    # withdraw CIS2 tokens event (CIS-5)
                    addresses_to_save.append(
                        {
                            "address": f"{log.event_info.contract}-{log.recognized_event.from_public_key_ed25519}",
                            "address_type": AddressTypes.public_key,
                        }
                    )
                    addresses_to_save.append(
                        {
                            "address": log.recognized_event.to_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif log.recognized_event.tag == 246:
                    # transfer CIS2 tokens event (CIS-5)
                    addresses_to_save.append(
                        {
                            "address": f"{log.event_info.contract}-{log.recognized_event.from_public_key_ed25519}",
                            "address_type": AddressTypes.public_key,
                        }
                    )
                    addresses_to_save.append(
                        {
                            "address": f"{log.event_info.contract}-{log.recognized_event.to_public_key_ed25519}",
                            "address_type": AddressTypes.public_key,
                        }
                    )

                elif log.recognized_event.tag == 251:
                    # metadata event (no addresses involved)
                    if log.event_info.token_address not in token_addresses_as_class_initial:
                        token_address_as_class = self.create_new_token_address_v2(
                            log.event_info.token_address, log.tx_info.block_height
                        )
                        token_addresses_to_update[log.event_info.token_address] = (
                            token_address_as_class
                        )

                    else:
                        token_address_as_class = token_addresses_as_class_initial[
                            log.event_info.token_address
                        ]

                    token_address_as_class.metadata_url = log.recognized_event.metadata.url
                    token_addresses_to_update[log.event_info.token_address] = token_address_as_class
                    # save_token_address = True

                unique_addresses = {
                    address["address"]: address for address in addresses_to_save
                }.values()
                for address in unique_addresses:
                    address_type: AddressTypes = address["address_type"]
                    canonical = (
                        address["address"][:29] if address_type.value else address["address"]
                    )
                    if address["address"] is None:
                        continue

                    _id = f"{contract_}-{token_id_}-{address['address']}"

                    # we need to call balanceOf for the address
                    # to determine what the actual balance is.
                    # If the token amount is zero, we will remove the link.
                    token_amount = await self.determine_token_amount(
                        address["address"],
                        CCD_ContractAddress.from_str(contract_),
                        token_id_,
                        self.net,
                    )

                    token_holding = MongoTypeTokenForAddress(
                        **{
                            "token_address": f"{contract_}-{token_id_}",
                            "contract": contract_,
                            "token_id": token_id_,
                            "token_amount": token_amount,
                        }
                    )

                    link_to_save = MongoTypeTokenLink(
                        **{
                            "_id": _id,
                            "account_address": address["address"],
                            "account_address_canonical": canonical,
                        }
                    )
                    link_to_save.token_holding = token_holding
                    repl_dict = link_to_save.model_dump(exclude_none=True)
                    if "id" in repl_dict:
                        del repl_dict["id"]
                    if token_holding.token_amount == "0":
                        # this means the address has no tokens anymore
                        # and we should remove the link.
                        links_to_save.append(DeleteOne({"_id": _id}))
                    else:
                        links_to_save.append(ReplaceOne({"_id": _id}, repl_dict, upsert=True))
                    # if address["address_type"] == AddressTypes.public_key:
                    #     links_to_save.append(
                    #         DeleteOne(
                    #             {
                    #                 "_id": f"{contract_}-{token_id_}-{address['address'].split('-')[1]}"
                    #             }
                    #         )
                    #     )

            for ta in token_addresses_to_update.values():
                ta: MongoTypeTokenAddress
                repl_dict = ta.model_dump(exclude_none=True)
                if (
                    "failed_attempt" in repl_dict
                    and "do_not_try_before" in repl_dict["failed_attempt"]
                ):
                    repl_dict["failed_attempt"]["do_not_try_before"] = (
                        f"{repl_dict['failed_attempt']['do_not_try_before']:%Y-%m-%dT%H:%M:%S.%fZ}"
                    )
                await self.send_metadata_to_redis(repl_dict)

                if "id" in repl_dict:
                    del repl_dict["id"]

                token_addresses_to_save.append(
                    ReplaceOne(
                        {"_id": ta.id},
                        replacement=repl_dict,
                        upsert=True,
                    )
                )
                if "failed_attempt" in repl_dict:
                    del repl_dict["failed_attempt"]

            if len(links_to_save) > 0:
                result2 = self.db[Collections.tokens_links_v3].bulk_write(links_to_save)
                console.log(
                    f"TL:  {len(links_to_save):5,.0f} | M {result2.matched_count:5,.0f} | Mod {result2.modified_count:5,.0f} | U {result2.upserted_count:5,.0f}"
                )
                # if result2.upserted_count > 0:
                #     console.log(result2.upserted_ids)
            if len(token_addresses_to_save) > 0:
                result3 = self.db[Collections.tokens_token_addresses_v2].bulk_write(
                    token_addresses_to_save
                )
                console.log(
                    f"TA:  {len(token_addresses_to_save):5,.0f} | M {result3.matched_count:5,.0f} | Mod {result3.modified_count:5,.0f} | U {result3.upserted_count:5,.0f}"
                )

            # if save_process:
            #     self.log_last_token_accounted_message_in_mongo(
            #         token_accounting_last_processed_block_when_done
            #     )

    def create_new_token_address_v2(
        self, token_address: str, height: int
    ) -> MongoTypeTokenAddressV2:
        instance_address = token_address.split("-")[0]
        token_id = token_address.split("-")[1]
        token_address = MongoTypeTokenAddressV2(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": token_id,
                "token_amount": str(int(0)),  # mongo limitation on int size
                "last_height_processed": height,
                "hidden": False,
            }
        )  # type: ignore
        return token_address

    ########## Token Accounting

    async def send_metadata_to_redis(self, repl_dict: dict):
        if len(repl_dict) > 0:
            token_address = f"{repl_dict['contract']}-{repl_dict['token_id']}"
            await publish_to_celery("metadata", {"token_address": token_address})
