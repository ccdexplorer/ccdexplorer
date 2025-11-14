from ccdexplorer.mongodb import MongoDB, Collections
from ccdexplorer.cis import CIS
from ccdexplorer.grpc_client.core import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.cis import s7_InventoryCreateParams_ERC721_V2
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
import marimo as mo
import pandas as pd
from pymongo import DeleteOne
from optparse import OptionParser
import requests
import inspect
from ccdexplorer.domain.s7 import erc721_v1_contracts, erc721_v2_contracts, erc_1155_v1_contracts

mongodb = MongoDB(None)
grpc_client = GRPCClient()
cis = CIS(grpc_client)
from rich import print


def print_receive_names_for(contracts: list[int]):
    for contract_index in contracts:
        actions = set()
        imp_txs = [
            x["tx_hash"]
            for x in mongodb.mainnet[Collections.impacted_addresses]
            .find({"impacted_address_canonical": f"<{contract_index},0>"})
            .sort({"block_height": -1})
        ]
        txs = [
            CCD_BlockItemSummary(**x)
            for x in mongodb.mainnet[Collections.transactions]
            .find({"_id": {"$in": imp_txs}})
            .sort({"block_info.height": -1})
        ]
        results = {}
        dd_per_token = {}
        for tx in txs:
            if not tx.account_transaction.effects.contract_update_issued:
                continue
            for effect in tx.account_transaction.effects.contract_update_issued.effects:
                if effect.updated:
                    actions.add(f"{tx.block_info.height}-{tx.hash}-{effect.updated.receive_name}")
        print(contract_index, actions)
    return (print_receive_names_for,)


def erc_721_v2(contract_index: int):
    erc_721_v2_txs = [
        x["tx_hash"]
        for x in mongodb.mainnet[Collections.impacted_addresses].find(
            {"impacted_address_canonical": f"<{contract_index},0>"}
        )
    ]
    txs = [
        CCD_BlockItemSummary(**x)
        for x in mongodb.mainnet[Collections.transactions].find({"_id": {"$in": erc_721_v2_txs}})
    ]
    results = {}
    for tx in txs:
        if not tx.account_transaction.effects.contract_update_issued:
            continue
        for effect in tx.account_transaction.effects.contract_update_issued.effects:
            if effect.updated:
                if effect.updated.receive_name == "trader.create_and_sell":
                    entrypoint = "trader.create_and_sell"

                    cis.instance_index = contract_index
                    cis.instance_subindex = 0
                    cis.entrypoint = entrypoint
                    cis.net = NET.MAINNET
                    hex = effect.updated.parameter
                    parsed_result = cis.s7_trader_create_and_sell_erc721_v2(hex)
                    results[tx.hash] = parsed_result
    return results


def inventory_set_owner(contract_index: int):
    # instance_info = mongodb.mainnet[Collections.instances].find_one(
    #     {"_id": f"<{contract_index},0>"}
    # )
    # if instance_info:
    #     if instance_info.get("v0"):
    #         if instance_info.get("v0").get("name") == "init_trader":
    #             print(f"Contract {contract_index} is init_trader, removing links")
    #             all_links = list(
    #                 mongodb.mainnet[Collections.tokens_links_v3].find(
    #                     {"token_holding.contract": f"<{contract_index},0>"}
    #                 )
    #             )
    #             q = []
    #             for link in all_links:
    #                 q.append(DeleteOne({"_id": link["_id"]}))
    #             if len(q) > 0:
    #                 rr = mongodb.mainnet[Collections.tokens_links_v3].bulk_write(q)
    #                 print(f"Removed {len(all_links)} links for contract {contract_index} ")
    #             return {}

    imp_txs = [
        x["tx_hash"]
        for x in mongodb.mainnet[Collections.impacted_addresses]
        .find({"impacted_address_canonical": f"<{contract_index},0>"})
        .sort({"block_height": -1})
    ]
    logged_events = [
        x
        for x in mongodb.mainnet[Collections.tokens_logged_events_v2]
        .find({"event_info.contract": f"<{contract_index},0>"})
        .sort(
            {
                "tx_info.block_height": -1,
                "event_info.effect_index": -1,
                "event_info.event_index": -1,
            }
        )
    ]
    results = {}
    dd_per_token = {}
    for tx in logged_events:
        if tx.account_transaction is None:
            continue
        if not tx.account_transaction.effects.contract_update_issued:
            continue
        for effect in tx.account_transaction.effects.contract_update_issued.effects:
            if effect.updated:
                if effect.updated.receive_name in [
                    "inventory.safe_transfer_from",
                    "inventory.transfer_from",
                    "inventory.transfer",
                    "inventory.create",
                    "inventory.close",
                ]:
                    # print (contract_index, tx.hash, tx.block_info.height, effect.updated.receive_name)
                    cis.instance_index = contract_index
                    cis.instance_subindex = 0
                    cis.entrypoint = effect.updated.receive_name
                    cis.net = NET.MAINNET
                    hex = effect.updated.parameter
                    if effect.updated.receive_name == "inventory.transfer":
                        parsed = cis.s7_inventory_transfer_erc721_v2_transfer_parameter(hex)
                        address_field = parsed.to_
                    elif effect.updated.receive_name in [
                        "inventory.safe_transfer_from",
                        "inventory.transfer_from",
                    ]:
                        parsed = cis.s7_inventory_safe_transfer_from_erc721_v2_parameter(hex)
                        address_field = parsed.to_
                    elif effect.updated.receive_name == "inventory.create":
                        parsed = cis.s7_inventory_create_erc721_v2_create_parameter(hex)
                        address_field = parsed.creator
                    else:
                        parsed = cis.s7_inventory_close_erc721_v2_close_parameter(hex)
                        address_field = parsed.sender
                    if parsed.custom_token_id not in dd_per_token:
                        dd_per_token[parsed.custom_token_id] = []
                    dd_per_token[parsed.custom_token_id].append(
                        {
                            "block_height": tx.block_info.height,
                            "action": effect.updated.receive_name,
                            "owner": address_field,
                        }
                    )

    print(dd_per_token)
    for token, dd in dd_per_token.items():
        action = dd[0]
        # print (action.keys())
        if action["action"] == "inventory.close":
            remove_fake_token_link(contract_index, token, action["owner"])
        elif (
            action["action"]
            in ["inventory.transfer", "inventory.safe_transfer_from", "inventory.transfer_from"]
        ) and (action["owner"] is None):
            remove_fake_token_link(contract_index, token, action["owner"])
        else:
            save_fake_token_link(contract_index, token, action["owner"])
    return results


def remove_fake_token_link(contract_index: int, token_id: int, address_field: str):
    dd = {
        "_id": f"<{contract_index},0>-{token_id}-{address_field}",
    }
    rr = mongodb.mainnet[Collections.tokens_links_v3].delete_one(
        {"_id": dd["_id"]},
    )
    return (remove_fake_token_link,)


def save_fake_token_link(contract_index: int, token_id: int, address_field: str):
    # print (f"_id = {f"<{contract_index},0>-{token_id}"}")
    result = mongodb.mainnet[Collections.tokens_token_addresses_v2].find_one(
        {"_id": f"<{contract_index},0>-{token_id}"}
    )
    if not result:
        # print ("ERROR", contract_index, token_id, address_field)
        return None
    if not address_field:
        # print ("ERROR", contract_index, token_id, address_field)
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
    rr = mongodb.mainnet[Collections.tokens_links_v3].replace_one(
        {"_id": dd["_id"]}, dd, upsert=True
    )
    # print
    return (save_fake_token_link,)


def inventory_create(contract_index: int):
    imp_txs = [
        x["tx_hash"]
        for x in mongodb.mainnet[Collections.impacted_addresses].find(
            {"impacted_address_canonical": f"<{contract_index},0>"}
        )
    ]
    txs = [
        CCD_BlockItemSummary(**x)
        for x in mongodb.mainnet[Collections.transactions].find({"_id": {"$in": imp_txs}})
    ]
    results = {}
    for tx in txs:
        if not tx.account_transaction.effects.contract_update_issued:
            continue
        for effect in tx.account_transaction.effects.contract_update_issued.effects:
            if effect.updated:
                if effect.updated.receive_name == "inventory.create":
                    cis.instance_index = contract_index
                    cis.instance_subindex = 0
                    cis.entrypoint = effect.updated.receive_name
                    cis.net = NET.MAINNET
                    hex = effect.updated.parameter
                    # print (tx.hash, hex)
                    parsed_result = None
                    if contract_index in erc721_v1_contracts:
                        version = "s7_erc721_v1"
                        parsed_result = cis.s7_inventory_create_erc721_v1(hex)
                        # save_fake_cis2_token(contract_index, version, parsed_result, tx)
                    elif contract_index in erc721_v2_contracts:
                        version = "s7_erc721_v2"
                        parsed_result = cis.s7_inventory_create_erc721_v2(hex)
                        save_fake_cis2_token(contract_index, version, parsed_result, tx)
                    elif contract_index in erc_1155_v1_contracts:
                        version = "s7_erc1155_v1"
                        parsed_result = cis.s7_inventory_create_erc1155_v1(hex)
                        save_fake_cis2_token(contract_index, version, parsed_result, tx)
                    else:
                        pass
                    if parsed_result:
                        results[f"{contract_index}-{tx.hash}"] = {
                            "contract_index": contract_index,
                            "version": version,
                            "tx_hash": tx.hash,
                            "parsed_result": parsed_result.model_dump(),
                        }
    return results


# @app.cell(hide_code=True)
# def _(
#     CCD_BlockItemSummary,
#     Collections,
#     NET,
#     cis,
#     erc721_v1_contracts,
#     erc721_v2_contracts,
#     erc_1155_v1_contracts,
#     mongodb,
#     print,
#     save_fake_cis2_token,
# ):
#     def trader_create_and_sell(contract_index: int):
#         imp_txs = [x["tx_hash"] for x in mongodb.mainnet[Collections.impacted_addresses].find({"impacted_address_canonical":f"<{contract_index},0>"})]
#         txs = [CCD_BlockItemSummary(**x) for x in mongodb.mainnet[Collections.transactions].find({"_id": {"$in": imp_txs}})]
#         results = {}
#         for tx in txs:
#             if not tx.account_transaction.effects.contract_update_issued:
#                 continue
#             for effect in tx.account_transaction.effects.contract_update_issued.effects:
#                 if effect.updated:
#                     if effect.updated.receive_name=="inventory.create":
#                         cis.instance_index = contract_index
#                         cis.instance_subindex = 0
#                         cis.entrypoint = effect.updated.receive_name
#                         cis.net = NET.MAINNET
#                         hex = effect.updated.parameter
#                         print (tx.hash, hex)
#                         parsed_result = None
#                         if contract_index in erc721_v1_contracts:
#                             version = "erc721_v1"
#                             parsed_result = cis.s7_inventory_create_erc721_v1(hex)
#                             # save_fake_cis2_token(version, parsed_result, tx)
#                         elif contract_index in erc721_v2_contracts:
#                             version = "erc721_v2"
#                             parsed_result = cis.s7_inventory_create_erc721_v2(hex)
#                             save_fake_cis2_token(version, parsed_result, tx)
#                         elif contract_index in erc_1155_v1_contracts:
#                             version = "erc1155_v1"
#                             parsed_result = cis.s7_inventory_create_erc1155_v1(hex)
#                             save_fake_cis2_token(version, parsed_result, tx)
#                         else:
#                             pass
#                         if parsed_result:
#                             results[f"{contract_index}-{tx.hash}"] = {"contract_index":contract_index, "version": version, "tx_hash": tx.hash, "parsed_result": parsed_result.model_dump()}
#         return results
#     return


def request_metadata(url: str):
    try:
        url = f"https://ipfs.io/ipfs/{url[7:]}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            t = resp.json()
            dd = {
                "name": t["name"],
                "unique": True,
                "description": t["description"],
                "thumbnail": {"url": f"https://ipfs.io/ipfs/{t['image'][7:]}"},
                "display": {"url": f"https://ipfs.io/ipfs/{t['image'][7:]}"},
            }
            # print (dd)
            return dd, url
        else:
            return None, url
    except Exception as e:
        print(e)
        return None, url


def save_fake_cis2_token(
    contract_index: int, version: str, result: s7_InventoryCreateParams_ERC721_V2, tx
):
    url = result.url
    if len(url) < 10:
        return None
    token_metadata, url = request_metadata(url)
    if not token_metadata:
        return None

    token_id = result.custom_token_id

    dd = {
        "_id": f"<{contract_index},0>-{token_id}",
        "contract": f"<{contract_index},0>",
        "token_id": token_id,
        "token_amount": 0,
        "metadata_url": url,
        "last_height_processed": tx.block_info.height,
        "special_type": version,
        "token_metadata": token_metadata,
        "mint_tx_hash": tx.hash,
    }
    rr = mongodb.mainnet[Collections.tokens_token_addresses_v2].replace_one(
        {"_id": dd["_id"]}, dd, upsert=True
    )
    # print (rr)


def add_contract_to_spaceseven_tag(contract_index: int):
    tag = "spaceseven"
    current_tag = mongodb.mainnet[Collections.tokens_tags].find_one({"_id": tag})
    if current_tag:
        current_contracts = set(current_tag["contracts"])
        current_contracts.add(f"<{contract_index},0>")
        current_tag["contracts"] = sorted(list(current_contracts))
        # print (current_tag["contracts"])
        rr = mongodb.mainnet[Collections.tokens_tags].replace_one(
            {"_id": current_tag["_id"]}, current_tag, upsert=True
        )
    # print (current_tag)


def doit():
    all_contracts = erc721_v1_contracts + erc721_v2_contracts  # erc_1155_v1_contracts #
    all_contracts = erc_1155_v1_contracts
    # all_contracts = erc721_v2_contracts
    results = {}
    for contract in mo.status.progress_bar(all_contracts):
        inventory_set_owner(contract)

    return (results,)


def save_to_file(pd, results):
    df = pd.json_normalize(results.values())
    df.to_csv("trader_create_and_sell_all_contracts.csv", index=False)
    return (df,)


if __name__ == "__main__":
    doit()
