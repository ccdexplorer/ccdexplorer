"""Backfill rounds_won_count / rounds_total / rounds_missed_total.

Dry run by default -- pass --write to actually touch Mongo.

Only $sets the three new fields. missed_rounds_count is never written: it is
recomputed and compared against what is stored, and a document whose misses do
not reproduce is reported and skipped rather than silently corrected. Uses the
same _counts_for_epoch as the live job, and the same epoch-1 convention, so a
backfilled document is indistinguishable from one the job wrote.

    python backfill_validators_missed.py                 # dry run, everything
    python backfill_validators_missed.py --limit 50      # dry run, 50 docs
    python backfill_validators_missed.py --write         # for real
    python backfill_validators_missed.py --write --genesis 9
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
from ccdexplorer.mongodb import MongoDB  # noqa: E402
from ccdexplorer.tooter.core import Tooter  # noqa: E402
from pymongo import UpdateOne  # noqa: E402

NEW_FIELDS = ("rounds_won_count", "rounds_total", "rounds_missed_total")
BATCH = 200

parser = argparse.ArgumentParser()
parser.add_argument("--write", action="store_true", help="actually write (default: dry run)")
parser.add_argument("--limit", type=int, default=0, help="stop after N documents")
parser.add_argument("--genesis", type=int, default=None, help="only this genesis index")
parser.add_argument("--oldest-first", action="store_true", help="default is newest first")
parser.add_argument("--sleep", type=float, default=0.1, help="seconds between gRPC calls")
args = parser.parse_args()

mongodb = MongoDB(Tooter(), caller_name="backfill_validators_missed")
grpc = GRPCClient()
coll = mongodb.mainnet_db["paydays_v2_validators_missed"]

query = {"rounds_won_count": {"$exists": False}}
if args.genesis is not None:
    query["genesis_index"] = args.genesis

total = coll.count_documents(query)
order = 1 if args.oldest_first else -1
cursor = coll.find(query).sort([("genesis_index", order), ("epoch", order)])
if args.limit:
    cursor = cursor.limit(args.limit)

mode = "WRITING" if args.write else "DRY RUN (no writes)"
print(f"{mode}: {total:,} documents lack the new fields"
      f"{f' (limited to {args.limit})' if args.limit else ''}\n")

pending: list[UpdateOne] = []
done = failed = mismatched = 0
errors: dict[str, int] = {}
mismatches: list[str] = []


def flush() -> None:
    global pending
    if pending and args.write:
        coll.bulk_write(pending, ordered=False)
    pending = []


for doc in cursor:
    _id, genesis, epoch = doc["_id"], doc["genesis_index"], doc["epoch"]
    try:
        # Same convention as the job: document labelled `epoch` holds epoch - 1.
        winning_bakers = grpc.get_winning_bakers_epoch(genesis, epoch - 1)
    except Exception as exc:  # pruned, future epoch, node unavailable
        detail = str(exc)
        kind = "pruned/unavailable"
        if "Future epoch" in detail:
            kind = "future epoch"
        elif "NOT_FOUND" in detail:
            kind = "not found"
        errors[kind] = errors.get(kind, 0) + 1
        failed += 1
        continue

    if not winning_bakers:
        errors["empty response"] = errors.get("empty response", 0) + 1
        failed += 1
        continue

    counts = _counts_for_epoch(winning_bakers)

    if counts["missed_rounds_count"] != doc.get("missed_rounds_count", {}):
        mismatched += 1
        if len(mismatches) < 20:
            mismatches.append(
                f"  {_id}: stored={doc.get('missed_rounds_count')} "
                f"recomputed={counts['missed_rounds_count']}"
            )
        continue  # never overwrite a document we cannot reproduce

    pending.append(UpdateOne({"_id": _id}, {"$set": {f: counts[f] for f in NEW_FIELDS}}))
    done += 1

    if len(pending) >= BATCH:
        flush()
        print(f"  {done:,} updated, {failed:,} unreadable, {mismatched:,} mismatched", flush=True)

    time.sleep(args.sleep)

flush()

print(f"\n{'written' if args.write else 'would write'}: {done:,}")
print(f"unreadable from node:  {failed:,}")
for kind, n in sorted(errors.items()):
    print(f"    {kind}: {n:,}")
print(f"missed_rounds_count mismatched (skipped): {mismatched:,}")
for line in mismatches:
    print(line)
if mismatched > len(mismatches):
    print(f"  ... and {mismatched - len(mismatches):,} more")

if not args.write:
    print("\nDry run. Re-run with --write to apply.")
sys.exit(0)
