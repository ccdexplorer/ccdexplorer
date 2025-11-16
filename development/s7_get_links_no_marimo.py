import io
from ccdexplorer.mongodb import MongoDB, Collections
from ccdexplorer.cis import CIS
from ccdexplorer.grpc_client.core import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import MongoTypeLoggedEventV2, MongoTypeInstance

from ccdexplorer.domain.s7 import (
    SpaceSevenEvents,
    s7_erc1155_v1_TransferEvent,
    s7_erc1155_v1_OnSaleEvent,
    s7_erc721_v2_TransferEvent,
    s7_erc721_v2_OnSaleEvent,
)
from ccdexplorer.domain.s7 import erc721_v1_contracts, erc721_v2_contracts, erc_1155_v1_contracts

s7 = SpaceSevenEvents()
s7_contract_to_erc_version = {}

for i in erc721_v1_contracts:
    s7_contract_to_erc_version[i] = "erc721_v1"

for i in erc721_v2_contracts:
    s7_contract_to_erc_version[i] = "erc721_v2"

for i in erc_1155_v1_contracts:
    s7_contract_to_erc_version[i] = "erc1155_v1"


mongodb = MongoDB(None)  # type: ignore
grpc_client = GRPCClient()
cis = CIS(grpc_client)


def remove_fake_token_link(contract_index: int, token_id: int, address_field: str):
    dd = {
        "_id": f"<{contract_index},0>-{token_id}-{address_field}",
    }
    _ = mongodb.mainnet[Collections.tokens_links_v3].delete_one(
        {"_id": dd["_id"]},
    )


def save_fake_token_link(contract_index: int, token_id: int, address_field: str):
    # print (f"_id = {f"<{contract_index},0>-{token_id}"}")
    result = mongodb.mainnet[Collections.tokens_token_addresses_v2].find_one(
        {"_id": f"<{contract_index},0>-{token_id}"}
    )
    if not result:
        return None
    if not address_field:
        return None
    dd = {
        "_id": f"<{contract_index},0>-{token_id}-{address_field}",
        "account_address": address_field,
        "account_address_canonical": address_field[:29],
        "token_holding": {
            "token_address": f"<{contract_index},0>-{token_id}",
            "contract": f"<{contract_index},0>",
            "token_id": str(token_id),
            "token_amount": "1",
        },
    }
    _ = mongodb.mainnet[Collections.tokens_links_v3].replace_one(
        {"_id": dd["_id"]}, dd, upsert=True
    )


def inventory_set_owner(contract_index: int):
    instance = MongoTypeInstance(
        **mongodb.mainnet[Collections.instances].find_one({"_id": f"<{contract_index},0>"})  # type: ignore
    )  # type: ignore
    if instance.v0:
        entrypoint = instance.v0.name[5:]
    if instance.v1:
        entrypoint = instance.v1.name[5:]

    logged_events = [
        MongoTypeLoggedEventV2(**x)
        for x in mongodb.mainnet[Collections.tokens_logged_events_v2]
        .find({"event_info.contract": f"<{contract_index},0>"})
        .sort(
            {
                "tx_info.block_height": 1,
                "event_info.effect_index": 1,
                "event_info.event_index": 1,
            }
        )
    ]

    for event in logged_events:
        s7_event = None
        contract_version = s7_contract_to_erc_version.get(contract_index)
        if contract_version is None:
            return None
        bs = io.BytesIO(bytes.fromhex(event.event_info.logged_event))
        tag_ = int.from_bytes(bs.read(1), byteorder="little")
        cis_instance = CIS(
            None,
            contract_index,
            0,
            entrypoint,  # type: ignore
            NET("mainnet"),
        )
        if contract_version == "erc721_v1":
            if tag_ == 0:
                s7_event = s7.s7_erc721_v1_inventory_add_trader_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 1:
                s7_event = s7.s7_erc721_v1_inventory_transfer_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 4:
                s7_event = s7.s7_erc721_v1_inventory_on_sale_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 5:
                s7_event = s7.s7_erc721_v1_inventory_buying_event(
                    cis_instance, event.event_info.logged_event
                )

        if contract_version == "erc721_v2":
            if tag_ == 0:
                s7_event = s7.s7_erc721_v2_inventory_add_trader_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 1:
                s7_event = s7.s7_erc721_v2_inventory_transfer_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 4:
                s7_event = s7.s7_erc721_v2_inventory_on_sale_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 5:
                s7_event = s7.s7_erc721_v2_inventory_buying_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 6:
                s7_event = s7.s7_erc721_v2_inventory_pause_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 7:
                s7_event = s7.s7_erc721_v2_inventory_closed_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 8:
                s7_event = s7.s7_erc721_v2_inventory_created_event(
                    cis_instance, event.event_info.logged_event
                )
        if contract_version == "erc1155_v1":
            if tag_ == 0:
                s7_event = s7.s7_erc1155_v1_inventory_add_trader_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 1:
                s7_event = s7.s7_erc1155_v1_inventory_transfer_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 4:
                s7_event = s7.s7_erc1155_v1_inventory_on_sale_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 5:
                s7_event = s7.s7_erc1155_v1_inventory_buying_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 6:
                s7_event = s7.s7_erc1155_v1_inventory_pause_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 7:
                s7_event = s7.s7_erc1155_v1_inventory_closed_event(
                    cis_instance, event.event_info.logged_event
                )
            if tag_ == 8:
                s7_event = s7.s7_erc1155_v1_inventory_create_event(
                    cis_instance, event.event_info.logged_event
                )
        if s7_event is None:
            continue
        if isinstance(s7_event, s7_erc1155_v1_TransferEvent) or isinstance(  # type: ignore
            s7_event,  # type: ignore
            s7_erc721_v2_TransferEvent,  # type: ignore
        ):
            # if the last even is transfer event, we can use it to set the owner
            if s7_event.from_:
                remove_fake_token_link(contract_index, s7_event.custom_token_id, s7_event.from_)  # type: ignore
                print(f"{contract_index}-{s7_event.custom_token_id}: rem link for {s7_event.from_}")
            if s7_event.to_:
                save_fake_token_link(contract_index, s7_event.custom_token_id, s7_event.to_)  # type: ignore
                print(f"{contract_index}-{s7_event.custom_token_id}: add link for {s7_event.to_}")

        if isinstance(s7_event, s7_erc1155_v1_OnSaleEvent) or isinstance(  # type: ignore
            s7_event,  # type: ignore
            s7_erc721_v2_OnSaleEvent,  # type: ignore
        ):  # type: ignore
            # if the last even is onsale event, we can use it to set the owner
            if s7_event.sender:
                save_fake_token_link(contract_index, s7_event.custom_token_id, s7_event.sender)  # type: ignore
                print(
                    f"{contract_index}-{s7_event.custom_token_id}: add link for {s7_event.sender}"
                )


def doit():
    all_contracts = erc721_v1_contracts + erc721_v2_contracts  # erc_1155_v1_contracts #
    all_contracts = erc_1155_v1_contracts
    # all_contracts = erc721_v2_contracts
    for contract in all_contracts:
        inventory_set_owner(contract)


if __name__ == "__main__":
    doit()
