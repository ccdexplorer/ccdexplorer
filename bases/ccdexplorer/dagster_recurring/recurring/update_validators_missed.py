from collections import Counter

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    CCD_WinningBaker,
)
from ccdexplorer.mongodb import MongoDB, Collections
from pymongo import ReplaceOne
import time


def _counts_for_epoch(winning_bakers: list[CCD_WinningBaker]) -> dict:
    """Turn one epoch's winning-baker rounds into the counts we store.

    `WinningBaker.present` is True when the winner of that round produced a
    block that made it onto the finalized chain, so a round with present=False
    is a block that should have existed and does not.

    We keep the missed counts (what this collection has always held) and add
    the denominator next to them: without knowing how many rounds a validator
    won, a miss count of 5 says nothing about whether that validator is
    healthy. Blocks actually baked is `rounds_won_count - missed_rounds_count`,
    so it is not stored separately.
    """
    missed = Counter(str(x.winner) for x in winning_bakers if not x.present)
    won = Counter(str(x.winner) for x in winning_bakers)

    def by_count_desc(counter: Counter) -> dict:
        return dict(sorted(counter.items(), key=lambda item: item[1], reverse=True))

    return {
        "missed_rounds_count": by_count_desc(missed),
        "rounds_won_count": by_count_desc(won),
        "rounds_total": len(winning_bakers),
        "rounds_missed_total": sum(missed.values()),
    }


# How far back a run looks for epochs it never recorded. Two paydays: long
# enough to catch a seam left by an outage or a concurrent rebuild, short
# enough that the check stays a single indexed query.
GAP_LOOKBACK_EPOCHS = 48


def epochs_to_record(
    highest_stored: int | None,
    recent_stored: set[int],
    payday_epoch: int,
    chain_epoch: int,
    lookback: int = GAP_LOOKBACK_EPOCHS,
) -> list[int]:
    """The epochs whose rounds this run should record, oldest first.

    The upper bound is `chain_epoch - 1`. The chain's current epoch is still in
    progress and GetWinningBakersEpoch rejects it as a "Future epoch", so that
    is the newest epoch there is complete data for.

    New epochs are taken from above the highest stored one rather than from the
    current payday. Anchoring to the payday orphaned exactly one epoch per
    payday: the upper bound never reached the payday's final epoch before the
    lower bound jumped past it at rollover, and nothing ever went back for it.

    Appending alone cannot repair a hole below the highest stored epoch, though,
    and a hole is exactly what a concurrent writer leaves behind -- rebuilding
    the collection while this job was still running put one at the seam between
    the two. So each run also sweeps the last `lookback` epochs and picks up
    anything missing. The sweep is bounded so the work per run stays constant;
    anything older than the window is a job for the rebuild script.

    `recent_stored` is the set of epochs already stored within that window.

    Falls back to the payday epoch when nothing is stored for this genesis, and
    to 1 when even that is out of range -- which happens right after a protocol
    update, when the last known payday block still belongs to the old genesis
    and its epoch number does not apply to the new one.
    """
    frontier = chain_epoch - 1

    if highest_stored is None:
        start = 1 if payday_epoch > frontier else payday_epoch
        return list(range(start, frontier + 1))

    fresh = range(highest_stored + 1, frontier + 1)
    window = range(max(1, highest_stored - lookback + 1), highest_stored + 1)
    holes = [epoch for epoch in window if epoch not in recent_stored]

    return sorted(holes + list(fresh))


def perform_validators_missed_update(
    context,
    grpcclient: GRPCClient,
    mongodb: MongoDB,
) -> dict:
    latest_block = grpcclient.get_block_info("last_final")
    context.log.info(
        f"height: {latest_block.height}, genesis: {latest_block.genesis_index}, epoch: {latest_block.epoch}"
    )
    doc = (mongodb.mainnet[Collections.helpers].find_one({"_id": "last_known_payday"})) or {}
    latest_payday_block: CCD_BlockInfo = grpcclient.get_block_info(block_input=doc["hash"])

    db = mongodb.mainnet_db
    collection = db[Collections.paydays_v2_validators_missed.value]
    context.log.info(
        f"latest payday - height: {latest_payday_block.height}, genesis: {latest_payday_block.genesis_index}, epoch: {latest_payday_block.epoch}"
    )

    highest_document = collection.find_one(
        {"genesis_index": latest_block.genesis_index}, sort=[("epoch", -1)]
    )
    highest = highest_document["epoch"] if highest_document else None

    recent_stored: set[int] = set()
    if highest is not None:
        recent_stored = {
            d["epoch"]
            for d in collection.find(
                {
                    "genesis_index": latest_block.genesis_index,
                    "epoch": {"$gte": max(1, highest - GAP_LOOKBACK_EPOCHS + 1)},
                },
                {"epoch": 1},
            )
        }

    epochs = epochs_to_record(
        highest,
        recent_stored,
        latest_payday_block.epoch,  # type: ignore
        latest_block.epoch,  # type: ignore
    )
    backfilled = [e for e in epochs if highest is not None and e <= highest]
    context.log.info(
        f"recording {len(epochs)} epoch(s), highest stored {highest if highest is not None else 'none'}"
        + (f", filling gaps {backfilled}" if backfilled else "")
    )

    for epoch in epochs:
        # A document is labelled with the epoch it holds the rounds of. Each
        # epoch is written on its own, so a failure part way through leaves the
        # collection contiguous and the next run resumes from the same place.
        winning_bakers: list[CCD_WinningBaker] = grpcclient.get_winning_bakers_epoch(
            latest_block.genesis_index,
            epoch,  # type: ignore
        )

        counts = _counts_for_epoch(winning_bakers)

        _id = f"genesis-{latest_block.genesis_index}-epoch-{epoch}"  # type: ignore
        dct = {
            "_id": _id,
            "genesis_index": latest_block.genesis_index,
            "epoch": epoch,  # type: ignore
        }
        context.log.info(
            f"genesis: {latest_block.genesis_index}, epoch: {epoch}, "
            f"rounds: {counts['rounds_total']}, missed: {counts['rounds_missed_total']}, "
            f"missed_rounds_count: {counts['missed_rounds_count']}"
        )
        dct.update(counts)
        time.sleep(0.1)
        collection.bulk_write(
            [
                ReplaceOne(
                    {"_id": _id},
                    replacement=dct,
                    upsert=True,
                )
            ]
        )
    return {}
