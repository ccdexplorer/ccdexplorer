from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_TokenEvent,
    CCD_CreatePLT,
    CCD_InitializationParameters,
)
from ccdexplorer.mongodb import Collections, MongoDB, CollectionsUtilities
from pymongo import DeleteOne, ReplaceOne
from pymongo.collection import Collection
import httpx


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


def update_plts(mongodb: MongoDB, grpc_client: GRPCClient, net: str, block_height: int) -> None:
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    address_plt_pairs_impacted: list[dict] = []

    pipeline = [
        {"$match": {"block_info.height": block_height}},
        {
            "$match": {
                "$or": [
                    {"account_transaction.effects.token_update_effect": {"$exists": True}},
                    {"token_creation": {"$exists": True}},
                ]
            }
        },
        {"$sort": {"block_info.height": 1, "index": 1}},
    ]
    txs = [CCD_BlockItemSummary(**x) for x in db[Collections.transactions].aggregate(pipeline)]

    queue = []
    print(f"{net} | {block_height:,.0f} | count of txs with plt: {len(txs)}")
    for tx in txs:
        events: list[CCD_TokenEvent] = []
        if tx.token_creation:
            events = tx.token_creation.events

            # save new PLT to _tags
            save_new_plt(mongodb, net, tx)

        elif tx.account_transaction:
            if tx.account_transaction.effects.token_update_effect:
                events = tx.account_transaction.effects.token_update_effect.events

        for event in events:
            token_id = event.token_id

            if event.module_event:
                pass
            elif event.transfer_event:
                address_plt_pairs_impacted.append(
                    {
                        "token_holder": event.transfer_event.from_.account,
                        "token_id": token_id,
                    }
                )
                address_plt_pairs_impacted.append(
                    {
                        "token_holder": event.transfer_event.to.account,
                        "token_id": token_id,
                    }
                )
            elif event.mint_event:
                address_plt_pairs_impacted.append(
                    {
                        "token_holder": event.mint_event.target.account,
                        "token_id": token_id,
                    }
                )

            elif event.burn_event:
                address_plt_pairs_impacted.append(
                    {
                        "token_holder": event.burn_event.target.account,
                        "token_id": token_id,
                    }
                )

    holder_to_ids: dict[str, list[str]] = {}

    for account in address_plt_pairs_impacted:
        holder_to_ids.setdefault(account["token_holder"], []).append(account["token_id"])

    for account in holder_to_ids.keys():
        canonical_account_address_entry = db[Collections.all_account_addresses].find_one(
            {"_id": account[:29]}
        )
        if not canonical_account_address_entry:
            continue
        canonical_account = canonical_account_address_entry["account_address"]
        account_info = grpc_client.get_account_info(
            block_hash="last_final", hex_address=account, net=NET(net)
        )
        if account_info:
            if account_info.tokens:
                for token_info in account_info.tokens:
                    token_id = token_info.token_id
                    token_account_state = token_info.token_account_state
                    balance = token_account_state.balance
                    _id = f"{token_id}-{canonical_account}"

                    # If a account_plt pair is present in the collection
                    # it means that the account has a PLT balance > 0
                    # This balance is always the the most recent one,
                    # as the link is updated on every transaction.
                    if balance.value == "0":
                        queue.append(DeleteOne({"_id": _id}))
                    else:
                        d = {
                            "_id": _id,
                            "account_address": canonical_account,
                            "account_address_canonical": canonical_account[:29],
                            "token_id": token_id,
                            "balance": str(balance.value),
                        }
                        queue.append(ReplaceOne({"_id": _id}, replacement=d, upsert=True))

    if len(queue) > 0:
        db[Collections.plts_links].bulk_write(queue)
    log_last_heartbeat_plts_in_mongo(db, block_height)

    print(
        f"{net} | {block_height:,.0f} | Modified {len(address_plt_pairs_impacted):,.0f} account_id pairs"
    )


def save_new_plt(mongodb: MongoDB, net: str, tx: CCD_BlockItemSummary) -> None:
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    db_utilities: dict[CollectionsUtilities, Collection] = mongodb.utilities
    create_plt = tx.token_creation.create_plt  # type: ignore

    # as this dictionary will also contain other parameters, such as display name, logo url,
    # etc, we need to first retrieve. If present, then overwrite only the below (useful for
    # re-doing transactions)
    plt_info = db[Collections.plts_tags].find_one({"_id": create_plt.token_id})

    if not plt_info:
        plt_info = {}

    plt_info.update(
        {
            "_id": create_plt.token_id,
            "token_id": create_plt.token_id,
            "token_module": create_plt.token_module,
            "decimals": create_plt.decimals,
            "initialization_parameters": create_plt.initialization_parameters.model_dump(
                exclude_none=True
            ),
        }
    )
    token_metadata = fetch_and_process_metadata(create_plt.initialization_parameters)
    if token_metadata:
        plt_info["token_metadata"] = token_metadata

    db[Collections.plts_tags].bulk_write(
        [ReplaceOne({"_id": create_plt.token_id}, replacement=plt_info, upsert=True)]
    )

    if net == "mainnet":
        label_governance_account(db, db_utilities, create_plt)


def fetch_and_process_metadata(
    create_plt_initialization_parameters: CCD_InitializationParameters,
):
    token_metadata = None
    try:
        with httpx.Client() as client:
            resp = client.get(create_plt_initialization_parameters.metadata["url"])
            resp.raise_for_status()

            if resp.status_code == 200:
                try:
                    t: dict = resp.json()
                    if "display" in t:
                        if isinstance(t["display"], dict):
                            t["display"] = t["display"].get("url", "")
                    if "thumbnail" in t:
                        if isinstance(t["thumbnail"], dict):
                            t["thumbnail"] = t["thumbnail"].get("url", "")

                    token_metadata = t
                    print(f"URL parsed for PLT {create_plt_initialization_parameters.name}.")
                except ValueError:
                    print(
                        f"Could not parse response as JSON. "
                        f"Content-Type={resp.headers.get('content-type')}, "
                        f"Length={len(resp.content)} bytes"
                        f" for token {create_plt_initialization_parameters.name}."
                    )
            else:
                error = (
                    f"Metadata error for  PLT {create_plt_initialization_parameters.name}."
                    f"resulted in status {resp.status_code}."
                )
                print(error)

    except httpx.RequestError as e:
        print(e)

    return token_metadata


def label_governance_account(
    db: dict[Collections, Collection],
    db_utilities: dict[CollectionsUtilities, Collection],
    create_plt: CCD_CreatePLT,
) -> None:
    ga_account = db[Collections.all_account_addresses].find_one(
        {"account_address": create_plt.initialization_parameters.governance_account.account}
    )

    # maybe it's labeled before for other PLTs?
    labeled_account = db_utilities[CollectionsUtilities.labeled_accounts].find_one(
        {"_id": create_plt.initialization_parameters.governance_account.account}
    )

    if labeled_account:
        label = f"{labeled_account['label']}/{create_plt.token_id}"
    else:
        label = f"GA-{create_plt.token_id}"

    ga_label = {}
    ga_label["_id"] = create_plt.initialization_parameters.governance_account.account
    ga_label["account_index"] = ga_account["account_index"] if ga_account else None
    ga_label["label_group"] = "PLT-Governance"
    ga_label["label"] = label

    # also add the Governance account as labeled account
    db_utilities[CollectionsUtilities.labeled_accounts].bulk_write(
        [ReplaceOne({"_id": ga_label["_id"]}, replacement=ga_label, upsert=True)]
    )
