"""Rebuild paydays_v2_validators_missed with correct epoch labels.

Dry run by default -- pass --write to actually touch Mongo.

Documents used to be labelled `epoch` while holding the rounds of `epoch - 1`,
and one epoch per payday was never written at all (the job's window jumped past
it at every payday rollover). This re-derives every epoch from the node and
writes it under the epoch it actually describes, which fixes the labels, fills
the gaps, and populates rounds_won_count in a single pass.

Written as upserts over the full range, never delete-then-insert: the
collection is never empty and no document is orphaned, because the new label
set covers the old one. While a run is in progress the collection is mixed --
rebuilt documents mean epoch N, untouched ones still mean N - 1 -- so the tally
column and the failed-rounds page read oddly until it finishes.

    python scripts/rebuild_validators_missed.py                 # dry run
    python scripts/rebuild_validators_missed.py --genesis 9     # one genesis
    python scripts/rebuild_validators_missed.py --write         # for real
"""

import argparse
import sys
import time

from dotenv import load_dotenv

load_dotenv()

from ccdexplorer.dagster_recurring.recurring.update_validators_missed import (  # noqa: E402
    _counts_for_epoch,
)
from ccdexplorer.grpc_client import GRPCClient  # noqa: E402
from ccdexplorer.mongodb import Collections, MongoDB  # noqa: E402
from ccdexplorer.tooter.core import Tooter  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--write", action="store_true", help="actually write (default: dry run)")
parser.add_argument("--genesis", type=int, default=None, help="only this genesis index")
parser.add_argument("--limit", type=int, default=0, help="stop after N epochs")
parser.add_argument("--sleep", type=float, default=0.1, help="seconds between gRPC calls")
args = parser.parse_args()

mongodb = MongoDB(Tooter(), caller_name="rebuild_validators_missed")
grpc = GRPCClient()
coll = mongodb.mainnet_db[Collections.paydays_v2_validators_missed.value]

chain = grpc.get_block_info("last_final")
frontier = chain.epoch - 1  # the current epoch is still in progress

# Rebuild each genesis over the span it already covers. The lower bound drops by
# one because the oldest document's label was one ahead of the rounds it held.
spans = []
for row in coll.aggregate(
    [{"$group": {"_id": "$genesis_index", "low": {"$min": "$epoch"}, "high": {"$max": "$epoch"}}}]
):
    genesis = row["_id"]
    if args.genesis is not None and genesis != args.genesis:
        continue
    high = frontier if genesis == chain.genesis_index else row["high"]
    spans.append((genesis, max(1, row["low"] - 1), high))
spans.sort()

planned = sum(high - low + 1 for _, low, high in spans)
if args.limit:
    planned = min(planned, args.limit)

print(f"{'WRITING' if args.write else 'DRY RUN (no writes)'}: chain at epoch {chain.epoch} "
      f"(genesis {chain.genesis_index}), frontier {frontier}")
for genesis, low, high in spans:
    print(f"  genesis {genesis}: epochs {low}..{high} ({high - low + 1:,})")
print(f"  {planned:,} epochs to rebuild\n")

done = failed = 0
errors: dict[str, int] = {}
started = time.monotonic()


def progress() -> str:
    seen = done + failed
    pct = f"{100 * seen / planned:5.1f}%" if planned else "     "
    if seen < 2:
        return f"{seen:>6,}/{planned:,} {pct}"
    eta = int((time.monotonic() - started) / seen * (planned - seen))
    return f"{seen:>6,}/{planned:,} {pct}  eta {eta // 3600}:{eta // 60 % 60:02d}:{eta % 60:02d}"


for genesis, low, high in spans:
    for epoch in range(low, high + 1):
        if args.limit and done + failed >= args.limit:
            break
        _id = f"genesis-{genesis}-epoch-{epoch}"
        try:
            winning_bakers = grpc.get_winning_bakers_epoch(genesis, epoch)
        except Exception as exc:
            detail = str(exc)
            kind = "future epoch" if "Future epoch" in detail else "pruned/unavailable"
            errors[kind] = errors.get(kind, 0) + 1
            failed += 1
            print(f"[{progress()}] {_id}  SKIP ({kind})", flush=True)
            continue

        if not winning_bakers:
            errors["empty response"] = errors.get("empty response", 0) + 1
            failed += 1
            print(f"[{progress()}] {_id}  SKIP (empty response)", flush=True)
            continue

        counts = _counts_for_epoch(winning_bakers)
        document = {"_id": _id, "genesis_index": genesis, "epoch": epoch, **counts}

        if args.write:
            coll.replace_one({"_id": _id}, document, upsert=True)
        done += 1

        print(
            f"[{progress()}] {_id}  "
            f"rounds {counts['rounds_total']:>5,}  "
            f"validators {len(counts['rounds_won_count']):>3}  "
            f"missed {counts['rounds_missed_total']:>3}"
            f"{'' if args.write else '   (dry run)'}",
            flush=True,
        )
        time.sleep(args.sleep)

elapsed = int(time.monotonic() - started)
print(f"\n{'rebuilt' if args.write else 'would rebuild'}: {done:,}"
      f"  in {elapsed // 3600}:{elapsed // 60 % 60:02d}:{elapsed % 60:02d}")
print(f"unreadable from node: {failed:,}")
for kind, n in sorted(errors.items()):
    print(f"    {kind}: {n:,}")
if not args.write:
    print("\nDry run. Re-run with --write to apply.")
sys.exit(0)
