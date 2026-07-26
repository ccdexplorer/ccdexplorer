from __future__ import annotations

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_LockId,
    CCD_LockInfoDecoded,
)
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import DeleteMany, UpdateOne
from pymongo.collection import Collection


def resolve_account_address_from_index(db: dict[Collections, Collection], account_index: int) -> str | None:
    entry = db[Collections.stable_address_info].find_one({"account_index": account_index})
    return entry.get("account_address") if entry else None


def compute_account_roles(
    db: dict[Collections, Collection], lock_id: CCD_LockId, decoded: CCD_LockInfoDecoded
) -> dict[str, set[str]]:
    """Map each involved account (canonical address) to the set of roles it plays in this lock."""
    roles: dict[str, set[str]] = {}

    creator_address = resolve_account_address_from_index(db, lock_id.account_index)
    if creator_address:
        roles.setdefault(creator_address, set()).add("creator")

    for grant in decoded.controller.grants:
        roles.setdefault(grant.account, set()).add("controller")

    for fund in decoded.funds:
        roles.setdefault(fund.account, set()).add("funder")

    if isinstance(decoded.recipients, list):
        for account in decoded.recipients:
            roles.setdefault(account, set()).add("recipient")

    return roles


def update_locks(mongodb: MongoDB, grpc_client: GRPCClient, net: str, block_height: int) -> None:
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet

    pipeline = [
        {"$match": {"block_info.height": block_height}},
        {"$match": {"account_transaction.effects.meta_update_effect": {"$exists": True}}},
        {"$sort": {"block_info.height": 1, "index": 1}},
    ]
    txs = [CCD_BlockItemSummary(**x) for x in db[Collections.transactions].aggregate(pipeline)]

    # lock_id_str -> {"lock_id": CCD_LockId, "destroyed": bool, "touching_txs": [...]}
    touched: dict[str, dict] = {}

    for tx in txs:
        if not (tx.account_transaction and tx.account_transaction.effects.meta_update_effect):
            continue

        for event in tx.account_transaction.effects.meta_update_effect.events:
            lock_id: CCD_LockId | None = None
            destroyed = False
            if event.lock_create_event:
                lock_id = event.lock_create_event.lock_id
            elif event.lock_destroy_event:
                lock_id = event.lock_destroy_event.lock_id
                destroyed = True
            elif event.transfer_event:
                lock_id = event.transfer_event.from_lock or event.transfer_event.to_lock

            if lock_id is None:
                continue

            entry = touched.setdefault(
                lock_id.to_str(),
                {"lock_id": lock_id, "destroyed": False, "touching_txs": []},
            )
            entry["touching_txs"].append({"tx_hash": tx.hash, "block_height": tx.block_info.height})
            if destroyed:
                entry["destroyed"] = True

    if not touched:
        return

    queue = []
    links_queue = []
    for lock_id_str, entry in touched.items():
        lock_id: CCD_LockId = entry["lock_id"]
        set_fields: dict = {"last_updated_block_height": block_height}

        if entry["destroyed"]:
            # A lock can only be destroyed once its balance is zero, so there is no
            # current state left to refresh - just flip the status. Link rows are kept
            # as-is (not deleted): `plts_locks_links` only records the relationship, so
            # status/expiry/etc. are always read live from `plts_locks` via a join, not
            # duplicated here - nothing on the link rows needs updating on destroy.
            set_fields["status"] = "cancelled"
        else:
            lock_info = grpc_client.get_lock_info("last_final", lock_id, net=NET(net))
            decoded = grpc_client.decode_lock_info(lock_info.lock_info)
            token_ids = sorted(set(decoded.controller.tokens))
            set_fields.update(
                {
                    "status": "open",
                    "recipients": decoded.recipients,
                    "expiry": decoded.expiry,
                    "controller": decoded.controller.model_dump(),
                    "funds": [fund.model_dump() for fund in decoded.funds],
                    "token_ids": token_ids,
                }
            )

            # Roles change over time (e.g. LockReturn removes a funder entirely), and the
            # lock's full state is already re-fetched live above, so fully resync this
            # lock's link rows too rather than accumulating stale ones. Link rows only
            # record the relationship (which accounts, in which roles) - lock attributes
            # like status/expiry/token_ids live solely on `plts_locks` and are joined in
            # at query time, not duplicated here.
            account_roles = compute_account_roles(db, lock_id, decoded)
            links_queue.append(DeleteMany({"lock_id_str": lock_id_str}))
            for account, roles in account_roles.items():
                links_queue.append(
                    UpdateOne(
                        {"_id": f"{lock_id_str}::{account[:29]}"},
                        {
                            "$set": {
                                "lock_id_str": lock_id_str,
                                "account_address": account,
                                "account_address_canonical": account[:29],
                                "account_roles": sorted(roles),
                                "last_updated_block_height": block_height,
                            }
                        },
                        upsert=True,
                    )
                )

        queue.append(
            UpdateOne(
                {"_id": lock_id_str},
                {
                    "$set": set_fields,
                    "$setOnInsert": {
                        "lock_id": lock_id.model_dump(),
                        "created_block_height": block_height,
                    },
                    "$push": {"touching_txs": {"$each": entry["touching_txs"]}},
                },
                upsert=True,
            )
        )

    db[Collections.plts_locks].bulk_write(queue)
    if links_queue:
        db[Collections.plts_locks_links].bulk_write(links_queue)
