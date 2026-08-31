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
    context.log.info(
        f"latest payday - height: {latest_payday_block.height}, genesis: {latest_payday_block.genesis_index}, epoch: {latest_payday_block.epoch}"
    )
    if latest_payday_block.epoch > latest_block.epoch:  # type: ignore
        start = 1
        end = latest_block.epoch
    else:
        start = latest_payday_block.epoch
        end = latest_block.epoch
    for epoch in range(start, end):  # type: ignore
        local_queue = []
        # NOTE: we query epoch - 1 but label the document `epoch`, so a doc
        # holds the previous epoch's rounds. That predates this change and the
        # failed-rounds API reads it as-is, so it is left alone. Every count
        # below comes from this one call, so the fields within a document
        # always agree with each other — they just do not agree with the label.
        winning_bakers: list[CCD_WinningBaker] = grpcclient.get_winning_bakers_epoch(
            latest_block.genesis_index,
            epoch - 1,  # type: ignore
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
        local_queue.append(
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        )

        if len(local_queue) > 0:
            _ = db["paydays_v2_validators_missed"].bulk_write(local_queue)
    return {}
