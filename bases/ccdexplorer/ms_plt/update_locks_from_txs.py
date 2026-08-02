from __future__ import annotations

import grpc

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_LockId,
    CCD_LockInfoDecoded,
)
from ccdexplorer.mongodb import Collections, MongoDB, net_db
from pymongo import DeleteMany, UpdateOne
from pymongo.collection import Collection


def resolve_account_address_from_index(
    db: dict[Collections, Collection], account_index: int
) -> str | None:
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
    db: dict[Collections, Collection] = net_db(mongodb, net)

    pipeline = [
        {"$match": {"block_info.height": block_height}},
        {"$match": {"account_transaction.effects.meta_update_effect": {"$exists": True}}},
        {"$sort": {"block_info.height": 1, "index": 1}},
    ]
    txs = [CCD_BlockItemSummary(**x) for x in db[Collections.transactions].aggregate(pipeline)]

    # lock_id_str -> {"lock_id": CCD_LockId, "destroyed": bool}
    # Purely an in-memory per-block working set of which locks need their live state
    # refreshed below - transaction history for a lock is not tracked here (it's derived
    # from `impacted_addresses` via its `lock_id` tag, see ms_events_and_impacts).
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
                {"lock_id": lock_id, "destroyed": False},
            )
            if destroyed:
                entry["destroyed"] = True

    if not touched:
        return

    queue = []
    links_queue = []
    for lock_id_str, entry in touched.items():
        lock_id: CCD_LockId = entry["lock_id"]
        print(f"Working on lock_id {lock_id}")
        set_fields: dict = {"last_updated_block_height": block_height}

        if entry["destroyed"]:
            # Cancel does NOT require the lock's balance to already be zero: per
            # concordium-node's execute_lock_cancel (plt-scheduler/src/protocol_level_locks/p11.rs),
            # the scheduler unlocks every account's remaining locked balance as part of
            # executing the cancel itself, then deletes the lock. So there's no live state
            # left to refresh (the lock is gone), but the `funds`/`recipients`/`controller`/
            # `expiry` we have on file here are only as of the last non-destroy update, and
            # may be stale - e.g. still show non-zero funds that the cancel just swept back
            # to their owners' normal balances. Just flip the status. Link rows are kept
            # as-is (not deleted): `plts_locks_links` only records the relationship, so
            # status/expiry/etc. are always read live from `plts_locks` via a join, not
            # duplicated here - nothing on the link rows needs updating on destroy.
            set_fields["status"] = "cancelled"
        else:
            try:
                # Query state as of the block actually being processed, not "last_final"
                # (the current chain tip). During backfill those can be far apart, and
                # querying the tip would incorrectly look up a lock that's since been
                # destroyed by a later block we haven't replayed yet.
                lock_info = grpc_client.get_lock_info(block_height, lock_id, net=NET(net))
            except grpc.RpcError as e:
                if isinstance(e, grpc.Call) and e.code() == grpc.StatusCode.NOT_FOUND:
                    # Shouldn't normally happen now that we query the exact block, but
                    # keep this as a safety net (e.g. node pruning) rather than crashing
                    # the whole backfill run - treat it the same as an explicit destroy.
                    print(f"lock_id {lock_id} not found at block {block_height}; marking cancelled")
                    set_fields["status"] = "cancelled"
                else:
                    raise
            else:
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

                # Roles change over time (e.g. LockReturn removes a funder entirely), and
                # the lock's full state is already re-fetched live above, so fully resync
                # this lock's link rows too rather than accumulating stale ones. Link rows
                # only record the relationship (which accounts, in which roles) - lock
                # attributes like status/expiry/token_ids live solely on `plts_locks` and
                # are joined in at query time, not duplicated here.
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
                    # strip any leftover touching_txs from before this field was removed -
                    # $set/$setOnInsert alone would leave a stale array on existing docs
                    "$unset": {"touching_txs": ""},
                },
                upsert=True,
            )
        )

    db[Collections.plts_locks].bulk_write(queue)
    if links_queue:
        db[Collections.plts_locks_links].bulk_write(links_queue)
