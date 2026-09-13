#!/usr/bin/env python3
"""Re-run metadata fetching locally for a set of tokens.

Run from the repo root so the workspace packages resolve:

    .venv/bin/python projects/ms_metadata/main_local.py

Prefix with DRY_RUN=1 to list what would be fetched without writing to MongoDB.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx2 as httpx
from ccdexplorer.domain.generic import NET
from ccdexplorer.env import RUN_ON_NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor, net_db
from ccdexplorer.ms_metadata.subscriber import Subscriber
from ccdexplorer.tooter import Tooter

# --- what to re-run -------------------------------------------------------
CONTRACT = os.getenv("CONTRACT", "<10082,0>")
# Tokens that still need work. Three cases, and the third is the reason for
# this run: metadata that fetched fine but parsed to nothing, because the
# document is a CIS-8004 agent card and shares no field with TokenMetaData.
# Those stored `{}` and discarded the only description the token has; the
# subscriber now keeps the published document in `raw_metadata`, so they need
# re-fetching to pick it up.
# Narrow to one clause, or drop them all, to re-fetch the whole contract.
SELECT = {
    "contract": CONTRACT,
    "$or": [
        {"token_metadata": {"$exists": False}},
        {"metadata_url": {"$exists": False}},
        {"token_metadata": {}, "raw_metadata": {"$exists": False}},
        # On an agent registry the card is kept even when CIS-2 parsing found
        # a name, because the page leads with the card there. Tokens fetched
        # before that rule existed have the name but not the card.
        {"raw_metadata": {"$exists": False}, "token_metadata.name": {"$exists": True}},
    ],
}
# Or set this to bypass the query entirely, e.g. ["<10082,0>-c507000000000000"]
TOKEN_ADDRESSES: list[str] = []
DRY_RUN = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")
# Pause between tokens. These fetches hit third-party servers -- a whole
# contract is a couple of thousand requests at one host -- so pace them.
DELAY_SECONDS = float(os.getenv("DELAY_SECONDS", "1.0"))
# How many fetches are in flight at once. Sequential runs are bound by round
# trip time, not by the pause between them -- at 200ms a piece, a
# hundred-thousand-token contract takes most of a day however small the delay
# is. Raising this is what makes such a run finish; the delay is what keeps a
# small host from being hammered. Use one or the other, not both.
#
# Safe to raise only because every worker touches its own request and its own
# document: pymongo and httpx are both thread-safe, and a token whose
# metadata_url is already known needs no gRPC call at all. A contract whose
# tokens still need the URL resolving from chain goes through the gRPC client
# instead, so keep CONCURRENCY low for those.
CONCURRENCY = int(os.getenv("CONCURRENCY", "1"))
# --------------------------------------------------------------------------


def main() -> None:
    tooter = Tooter()
    mongodb = MongoDB(tooter, caller_name="ms_metadata")
    motormongo = MongoMotor(tooter, nearest=True, caller_name="ms_metadata")
    subscriber = Subscriber(GRPCClient(), tooter, motormongo, mongodb)
    # NET picks which database is read as well as which chain is queried. The
    # selection used to read mainnet regardless, so a testnet run would fetch
    # for tokens that do not exist on the net it was writing to.
    net = NET(os.getenv("NET", RUN_ON_NET or "mainnet"))
    db = net_db(mongodb, net)

    token_addresses = TOKEN_ADDRESSES or [
        x["_id"]
        for x in db[Collections.tokens_token_addresses_v2].aggregate(
            [{"$match": SELECT}, {"$project": {"_id": 1}}]
        )
    ]

    print(
        f"[local] {net.value}: {len(token_addresses)} token(s) to process"
        f"{' (DRY RUN)' if DRY_RUN else ''}, {DELAY_SECONDS}s delay, "
        f"{CONCURRENCY} at a time",
        flush=True,
    )
    if DRY_RUN:
        for addr in token_addresses:
            print(f"   would fetch {addr}")
        return

    # Same client settings as the worker: ipfs.io redirects to a trailing slash,
    # which without follow_redirects reaches raise_for_status() as a failure.
    httpx_client = httpx.Client(follow_redirects=True, timeout=10.0)

    total = len(token_addresses)
    counts = {"ok": 0, "failed": 0, "done": 0}
    lock = threading.Lock()
    started = time.monotonic()
    every = 50 if total < 5000 else 1000

    def process(addr: str) -> None:
        if DELAY_SECONDS:
            time.sleep(DELAY_SECONDS)
        try:
            error = subscriber.fetch_token_metadata(net, addr, httpx_client)
        except Exception as exc:  # noqa: BLE001 - one bad token must not stop the run
            error = f"{type(exc).__name__}: {exc}"
        with lock:
            counts["failed" if error else "ok"] += 1
            counts["done"] += 1
            n = counts["done"]
            if n % every == 0 or n == total:
                elapsed = time.monotonic() - started
                eta = (elapsed / n) * (total - n)
                print(
                    f"[local] {n}/{total} — {counts['ok']} resolved, "
                    f"{counts['failed']} still failing — {elapsed / 60:.1f}m elapsed, "
                    f"~{eta / 60:.0f}m left",
                    flush=True,
                )

    if CONCURRENCY > 1:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            list(pool.map(process, token_addresses))
    else:
        for addr in token_addresses:
            process(addr)

    print(f"[local] done: {counts['ok']} resolved, {counts['failed']} still failing")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
