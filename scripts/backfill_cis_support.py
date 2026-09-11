#!/usr/bin/env python3
"""Fill in `cis_support` on instances that predate the cache.

ms_instances resolves CIS support when an instance is created or upgraded, so
everything from here on is covered. Instances that already existed are not,
and the API cannot fill them in itself -- it reads from the nearest replica,
not the primary. This is the one-off pass for them.

Run from the repo root:

    .venv/bin/python scripts/backfill_cis_support.py

Most instances cost nothing: if the module exports no `supports` entrypoint,
the answer is settled from what ms_modules parsed out of the wasm and no node
call is made. Only the remainder is paced.

    DRY_RUN=1     list what would be resolved, write nothing
    NET=testnet   default mainnet
    DELAY=0.2     seconds between instances that need a node call
    LIMIT=100     stop after this many instances
"""

from __future__ import annotations

import os
import sys
import time

from ccdexplorer.cis import build_cis_support, cached_standards, module_can_support
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB
from ccdexplorer.tooter import Tooter

DRY_RUN = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")
NET_NAME = os.getenv("NET", "mainnet")
# Only paced when a node call is actually made -- the free ones need no pacing.
DELAY = float(os.getenv("DELAY", "0.2"))
LIMIT = int(os.getenv("LIMIT", "0"))


def main() -> None:
    tooter = Tooter()
    mongodb = MongoDB(tooter, caller_name="backfill_cis_support")
    net = NET(NET_NAME)
    db = mongodb.mainnet if net == NET.MAINNET else mongodb.testnet
    grpc_client = GRPCClient()

    instances = list(db[Collections.instances].find({}))
    modules = {m["_id"]: m for m in db[Collections.modules].find({})}
    if LIMIT:
        instances = instances[:LIMIT]

    print(
        f"[backfill] {NET_NAME}: {len(instances)} instance(s), "
        f"{len(modules)} module(s){' (DRY RUN)' if DRY_RUN else ''}",
        flush=True,
    )

    free = queried = skipped = failed = 0
    started = time.monotonic()

    for n, instance in enumerate(instances, start=1):
        if cached_standards(instance) is not None:
            skipped += 1
            continue

        address = instance["_id"]
        try:
            index, subindex = (int(x) for x in address.strip("<>").split(","))
        except ValueError:
            failed += 1
            continue

        module = modules.get(instance.get("source_module"))
        needs_node = module_can_support(module)

        if DRY_RUN:
            print(f"   {address}: {'query node' if needs_node else 'free (no supports export)'}")
            if needs_node:
                queried += 1
            else:
                free += 1
            continue

        if needs_node and DELAY:
            time.sleep(DELAY)

        cis_support = build_cis_support(grpc_client, net, instance, module, index, subindex)
        if cis_support is None:
            # Left absent on purpose, so it is retried rather than settled.
            failed += 1
            continue

        db[Collections.instances].update_one(
            {"_id": address}, {"$set": {"cis_support": cis_support}}
        )
        if needs_node:
            queried += 1
        else:
            free += 1

        if n % 250 == 0 or n == len(instances):
            elapsed = time.monotonic() - started
            print(
                f"[backfill] {n}/{len(instances)} — {free} free, {queried} queried, "
                f"{skipped} already cached, {failed} undetermined — {elapsed / 60:.1f}m",
                flush=True,
            )

    print(
        f"[backfill] done: {free} free, {queried} queried, "
        f"{skipped} already cached, {failed} undetermined"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[backfill] interrupted", file=sys.stderr)
