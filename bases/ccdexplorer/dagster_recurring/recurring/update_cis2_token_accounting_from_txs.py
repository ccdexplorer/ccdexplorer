from enum import Enum

from ccdexplorer.cis import CIS
from ccdexplorer.domain.cis import (
    burnEvent,
    depositCIS2TokensEvent,
    mintEvent,
    tokenMetadataEvent,
    transferCIS2TokensEvent,
    transferEvent,
    withdrawCIS2TokensEvent,
)
from ccdexplorer.domain.generic import NET
from ccdexplorer.domain.mongo import (
    MongoTypeInstance,
    MongoTypeLoggedEventV2,
    MongoTypeTokenAddress,
    MongoTypeTokenAddressV2,
    MongoTypeTokenForAddress,
    MongoTypeTokenLink,
)
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_ContractAddress,
)
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ASCENDING, DeleteOne, ReplaceOne
from pymongo.collection import Collection

from .utils import get_helper, update_helper


class AddressTypes(Enum):
    # Value is whether a canonical version should be stored
    account_or_contract = True
    public_key = False


def create_new_token_address_v2(token_address: str, height: int) -> MongoTypeTokenAddressV2:
    instance_address = token_address.split("-")[0]
    token_id = token_address.split("-")[1]
    token_address_as_class = MongoTypeTokenAddressV2(
        **{
            "_id": token_address,
            "contract": instance_address,
            "token_id": token_id,
            "token_amount": str(int(0)),  # mongo limitation on int size
            "last_height_processed": height,
            "hidden": False,
        }
    )
    return token_address_as_class


def log_last_heartbeat_plts_in_mongo(db: dict[Collections, Collection], height: int):
    query = {"_id": "heartbeat_plts_last_processed_block"}
    db[Collections.helpers].replace_one(
        query,
        {
            "_id": "heartbeat_plts_last_processed_block",
            "height": height,
        },
        upsert=True,
    )


def get_entrypoint(
    contract_address: CCD_ContractAddress, net: str, mongodb: MongoDB, method_name: str
):
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    result = db[Collections.instances].find_one({"_id": contract_address.to_str()})
    instance = MongoTypeInstance(**result)  # type: ignore
    if instance.v1 is None:
        return None
    return instance.v1.name[5:] + "." + method_name


def determine_token_amount(
    address_or_public_key: str,
    contract_address: CCD_ContractAddress,
    token_id: str,
    net: str,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
):
    entrypoint = get_entrypoint(contract_address, net, mongodb, "balanceOf")
    ci = CIS(
        grpcclient,
        contract_address.index,
        contract_address.subindex,
        entrypoint,
        NET(net),
    )
    rr, ii = ci.balanceOf(
        "last_final",
        token_id,
        [address_or_public_key],
    )

    if ii.failure.used_energy > 0:
        print(ii.failure)
        # this indicates that we had a lookup failure
        token_amount = -1
    else:
        token_amount = rr[0]
    return token_amount


def update_cis2_token_accounting(
    context,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    net: str,
    save_process: bool = True,
) -> dict:
    dct = {}
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    dagster_cis2_last_processed_block = get_helper(
        "dagster_last_processed_token_accounting_height", net, mongodb
    )
    context.log.info(
        f"{net} | First block height processed: {(dagster_cis2_last_processed_block + 1):,.0f}."
    )
    max_block_height = CCD_BlockItemSummary(
        **db[Collections.transactions].find_one(sort=[("block_info.height", -1)])  # type: ignore
    ).block_info.height  # type: ignore

    pipeline = [
        {"$match": {"event_info.standard": "CIS-2"}},
        {"$match": {"tx_info.block_height": {"$gt": dagster_cis2_last_processed_block}}},
        {
            "$sort": {
                "tx_info.block_height": ASCENDING,
            }
        },
    ]
    result: list[MongoTypeLoggedEventV2] = [
        MongoTypeLoggedEventV2(**x)
        for x in db[Collections.tokens_logged_events_v2].aggregate(pipeline)
    ]
    token_accounting_last_processed_block_when_done = max([x.tx_info.block_height for x in result])

    links_to_save = []
    token_addresses_to_update = {}
    token_addresses_to_save = []
    context.log.info(f"{net} | count of logs : {len(result)}")
    for log in result:
        ################################
        log: MongoTypeLoggedEventV2
        if log.recognized_event is None:
            continue
        if not db[Collections.tokens_token_addresses_v2].find_one(
            {"_id": log.event_info.token_address}
        ):
            token_address_as_class = create_new_token_address_v2(
                log.event_info.token_address,  # type: ignore
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

        unique_addresses, token_addresses_to_update = find_unique_addresses_in_log(
            log, net, mongodb, token_addresses_to_update
        )

        for address in unique_addresses:
            address_type: AddressTypes = address["address_type"]
            canonical = address["address"][:29] if address_type.value else address["address"]
            if address["address"] is None:
                continue

            _id = f"{contract_}-{token_id_}-{address['address']}"

            # we need to call balanceOf for the address
            # to determine what the actual balance is.
            # If the token amount is zero, we will remove the link.
            token_amount = determine_token_amount(
                address["address"],
                CCD_ContractAddress.from_str(contract_),  # type: ignore
                token_id_,  # type: ignore
                net,
                mongodb,
                grpcclient,
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

    for ta in token_addresses_to_update.values():
        ta: MongoTypeTokenAddress
        repl_dict = ta.model_dump(exclude_none=True)
        if "failed_attempt" in repl_dict and "do_not_try_before" in repl_dict["failed_attempt"]:
            repl_dict["failed_attempt"]["do_not_try_before"] = (
                f"{repl_dict['failed_attempt']['do_not_try_before']:%Y-%m-%dT%H:%M:%S.%fZ}"
            )

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
        result2 = db[Collections.tokens_links_v3].bulk_write(links_to_save)
        context.log.info(
            f"TL:  {len(links_to_save):5,.0f} | M {result2.matched_count:5,.0f} | Mod {result2.modified_count:5,.0f} | U {result2.upserted_count:5,.0f}"
        )
        # if result2.upserted_count > 0:
        #     console.log(result2.upserted_ids)
    if len(token_addresses_to_save) > 0:
        result3 = db[Collections.tokens_token_addresses_v2].bulk_write(token_addresses_to_save)
        context.log.info(
            f"TA:  {len(token_addresses_to_save):5,.0f} | M {result3.matched_count:5,.0f} | Mod {result3.modified_count:5,.0f} | U {result3.upserted_count:5,.0f}"
        )

    if save_process:
        update_helper(
            "dagster_last_processed_token_accounting_height",
            token_accounting_last_processed_block_when_done,
            net,
            mongodb,
        )

    #################################

    context.log.info(f"{net} | Last block height processed: {max_block_height:,.0f}.")
    dct = {
        "token_addresses_to_save": token_addresses_to_save,
        "last_block_height_processed": token_accounting_last_processed_block_when_done,
    }
    return dct


def find_unique_addresses_in_log(
    log: MongoTypeLoggedEventV2,
    net: str,
    mongodb: MongoDB,
    token_addresses_to_update: dict,
):
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    addresses_to_save = []
    if log.recognized_event is None:
        return {}
    if isinstance(log.recognized_event, transferEvent):
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

    elif isinstance(log.recognized_event, mintEvent):
        addresses_to_save.append(
            {
                "address": log.recognized_event.to_address,
                "address_type": AddressTypes.account_or_contract,
            }
        )

    elif isinstance(log.recognized_event, burnEvent):
        addresses_to_save.append(
            {
                "address": log.recognized_event.from_address,
                "address_type": AddressTypes.account_or_contract,
            }
        )

    elif isinstance(log.recognized_event, depositCIS2TokensEvent):
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

    elif isinstance(log.recognized_event, withdrawCIS2TokensEvent):
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

    elif isinstance(log.recognized_event, transferCIS2TokensEvent):
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

    elif isinstance(log.recognized_event, tokenMetadataEvent):
        # metadata event (no addresses involved)
        token_address_as_class = db[Collections.tokens_token_addresses_v2].find_one(
            {"_id": log.event_info.token_address}
        )
        if not token_address_as_class:
            token_address_as_class = create_new_token_address_v2(
                log.event_info.token_address,  # type: ignore
                log.tx_info.block_height,  # type: ignore
            )
            token_addresses_to_update[log.event_info.token_address] = token_address_as_class
            token_addresses_to_update[log.event_info.token_address] = token_address_as_class

        if not isinstance(token_address_as_class, MongoTypeTokenAddressV2):
            token_address_as_class = MongoTypeTokenAddressV2(**token_address_as_class)

        token_address_as_class.metadata_url = log.recognized_event.metadata.url
        token_addresses_to_update[log.event_info.token_address] = token_address_as_class
        # save_token_address = True

    unique_addresses = {address["address"]: address for address in addresses_to_save}.values()

    return unique_addresses, token_addresses_to_update
