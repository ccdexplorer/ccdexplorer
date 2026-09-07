#!/usr/bin/env python3
"""Re-run metadata fetching locally for a set of tokens.

Run from the repo root so the workspace packages resolve:

    .venv/bin/python projects/ms_metadata/main_local.py

Prefix with DRY_RUN=1 to list what would be fetched without writing to MongoDB.
"""

from __future__ import annotations

import os
import sys
import time

import httpx2 as httpx
from ccdexplorer.domain.generic import NET
from ccdexplorer.env import RUN_ON_NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.ms_metadata.subscriber import Subscriber
from ccdexplorer.tooter import Tooter

# --- what to re-run -------------------------------------------------------
CONTRACT = "<10082,0>"
# Tokens that still need work: no metadata at all, or metadata whose source URL
# was never recorded (see the metadata_url note in read_and_store_metadata).
# Narrow to one clause, or drop both, to re-fetch the whole contract.
SELECT = {
    "contract": CONTRACT,
    "$or": [
        {"token_metadata": {"$exists": False}},
        {"metadata_url": {"$exists": False}},
    ],
}
# Or set this to bypass the query entirely, e.g. ["<10082,0>-c507000000000000"]
TOKEN_ADDRESSES: list[str] = []
DRY_RUN = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")
# Pause between tokens. These fetches hit third-party servers -- a whole
# contract is a couple of thousand requests at one host -- so pace them.
DELAY_SECONDS = float(os.getenv("DELAY_SECONDS", "1.0"))
# --------------------------------------------------------------------------


def main() -> None:
    tooter = Tooter()
    mongodb = MongoDB(tooter, caller_name="ms_metadata")
    motormongo = MongoMotor(tooter, nearest=True, caller_name="ms_metadata")
    subscriber = Subscriber(GRPCClient(), tooter, motormongo, mongodb)
    net = NET(RUN_ON_NET)

    token_addresses = TOKEN_ADDRESSES or [
        x["_id"]
        for x in mongodb.mainnet[Collections.tokens_token_addresses_v2].aggregate(
            [{"$match": SELECT}, {"$project": {"_id": 1}}]
        )
    ]

    print(
        f"[local] {RUN_ON_NET}: {len(token_addresses)} token(s) to process"
        f"{' (DRY RUN)' if DRY_RUN else ''}, {DELAY_SECONDS}s between requests",
        flush=True,
    )
    if DRY_RUN:
        for addr in token_addresses:
            print(f"   would fetch {addr}")
        return

    # Same client settings as the worker: ipfs.io redirects to a trailing slash,
    # which without follow_redirects reaches raise_for_status() as a failure.
    httpx_client = httpx.Client(follow_redirects=True, timeout=10.0)

    ok = failed = 0
    started = time.monotonic()
    for n, addr in enumerate(token_addresses, start=1):
        if n > 1 and DELAY_SECONDS:
            time.sleep(DELAY_SECONDS)
        error = subscriber.fetch_token_metadata(net, addr, httpx_client)
        if error:
            failed += 1
        else:
            ok += 1
        if n % 50 == 0 or n == len(token_addresses):
            elapsed = time.monotonic() - started
            eta = (elapsed / n) * (len(token_addresses) - n)
            print(
                f"[local] {n}/{len(token_addresses)} — {ok} resolved, {failed} still failing"
                f" — {elapsed / 60:.1f}m elapsed, ~{eta / 60:.0f}m left",
                flush=True,
            )

    print(f"[local] done: {ok} resolved, {failed} still failing")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
