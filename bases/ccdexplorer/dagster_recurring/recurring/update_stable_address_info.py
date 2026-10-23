"""Bring stable_address_info up to the chain's own list of accounts.

That collection is the only thing in the system that knows an account's
index: the site builds its boot-time address map from it, /accounts/get-indexes
answers from it, and a link to an account the collection has never heard of
renders as an address instead of an index.

It was only ever written one account at a time, by ms_accounts reacting to an
account_creation transaction. An account that never produced one on the chain
as indexed -- a genesis account, or anything predating the indexer's current
window -- therefore could not appear at all. Measured on 2026-10-23: devnet
had 85 accounts on chain and 29 in the collection, and those 29 were exactly
its 29 account_creation transactions, at indices 56-84. Indices 0-55 were its
genesis set, invisible, and the transaction that surfaced the problem was sent
by index 4.

mainnet has no such hole because a separate mainnet-only job walks the whole
account list and carries a hardcoded list of its 14 genesis addresses. That
job is deliberately untouched; this is the same walk, for every net, which a
devnet needs because it is replaced periodically and each replacement brings a
fresh genesis set no hardcoded list could anticipate.

Only the accounts the collection is missing are fetched, so the steady state
is one get_account_list per net and nothing else -- cheap enough to schedule
against mainnet's 104,000 as well as devnet's 85.
"""

from ccdexplorer.domain.generic import NET, AccountInfoStable
from ccdexplorer.mongodb import Collections, MongoDB, net_db
from pymongo import ReplaceOne

#: Accounts per bulk_write. mainnet's first run has ~104,000 to add, and one
#: operation list that long is a single request Mongo has to hold whole.
BATCH_SIZE = 500


def reconcile_stable_address_info(context, mongodb: MongoDB, grpcclient, net: str) -> dict:
    """Add every account the chain has that the collection does not.

    Additive only. An account cannot leave the chain, so a row with no match
    on chain is a row this walk failed to see rather than one to drop --
    deleting on that basis would turn a flaky node into data loss.

    A single account the node refuses is counted and stepped over. A devnet
    can be replaced underneath a walk, and losing the other 84 accounts to
    the one that went missing mid-run is a worse trade than reporting it.
    """
    db = net_db(mongodb, net)
    collection = db[Collections.stable_address_info]

    # "last_final" rather than a stored block hash: a replaced devnet has no
    # memory of the hash the previous one ended on.
    on_chain = grpcclient.get_account_list("last_final", net=NET(net))
    known = {doc["_id"] for doc in collection.find({}, {"_id": 1})}
    missing = [address for address in on_chain if str(address)[:29] not in known]

    context.log.info(
        f"{net} | {len(on_chain):,} accounts on chain, {len(known):,} known, "
        f"{len(missing):,} to add."
    )

    added = 0
    failed = 0
    queue: list[ReplaceOne] = []

    def flush():
        nonlocal added, queue
        if queue:
            collection.bulk_write(queue)
            added += len(queue)
            queue = []

    for address in missing:
        try:
            account_info = grpcclient.get_account_info(
                "last_final", hex_address=str(address), net=NET(net)
            )
            document = AccountInfoStable.from_account_info(account_info).to_collection()
        except Exception as error:  # noqa: BLE001
            # Broad on purpose: one account, not the run.
            failed += 1
            context.log.warning(f"{net} | could not read {address}: {error}")
            continue

        # to_collection() already sets _id to the canonical 29-character
        # prefix, which is the key the site's map, get-indexes and
        # ms_accounts all look up by.
        queue.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
        if len(queue) >= BATCH_SIZE:
            flush()
    flush()

    result = {
        "net": net,
        "on_chain": len(on_chain),
        "already_known": len(known),
        "missing": len(missing),
        "added": added,
        "failed": failed,
    }
    context.log.info(f"{net} | {result}")
    return result
