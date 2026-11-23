#!/usr/bin/env python3
"""Verify modules again, from your own machine, for the net in RUN_ON_NET.

Run it from the repo root (the module files are saved under ./tmp):

    uv run python projects/ms_modules/main_local.py
    uv run python projects/ms_modules/main_local.py --apply
    uv run python projects/ms_modules/main_local.py --apply <module_ref> ...

Without --apply it only lists the modules it would verify. With module refs it
takes those, whatever their state; without, every failure that had a source
link to check. Results are written to the database your .env points at and
sent to the tooter, like a live verification.

It needs on this machine: concordium-client, a running docker (OrbStack is
fine; the build images are amd64 and run emulated, so allow a few minutes
each), `cargo-concordium` (4.x) for modules built before wasm32v1, and
`cargo-concordium-5` on the PATH for the ones built with the wasm32v1 image:

    cargo +1.97.0 install cargo-concordium@5.0.0 --locked --root ~/.cargo-concordium-5
    ln -s ~/.cargo-concordium-5/bin/cargo-concordium ~/.cargo/bin/cargo-concordium-5

It does not reprocess the module: that would replace its stored document.
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import sys

from ccdexplorer.concordium_client import ConcordiumClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.env import RUN_ON_NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.ms_modules.subscriber import Subscriber
from ccdexplorer.tooter import Tooter

#: Failures worth another go: there was a source to check, so the verdict can
#: change. "No embedded build information" and "No source code found" cannot --
#: nothing was linked, and nothing about the verifier changes that.
RETRYABLE = {
    "verification.verification_status": "verified_failed",
    "verification.link_to_source_code": {"$exists": True, "$nin": [None, ""]},
}


async def main(apply: bool, refs: list[str]) -> None:
    net = NET(RUN_ON_NET)
    tooter = Tooter()
    mongodb = MongoDB(tooter, caller_name="ms_modules")
    db = mongodb.mainnet if net == NET.MAINNET else mongodb.testnet

    query = {"_id": {"$in": refs}} if refs else RETRYABLE
    modules = list(db[Collections.modules].find(query, {"module_name": 1, "verification": 1}))
    print(f"[local] {net.value}: {len(modules)} module(s)")
    for m in modules:
        explanation = (m.get("verification") or {}).get("explanation") or ""
        print(f"  {m['_id']}  {m.get('module_name')}  {explanation[:70]}")
    if not apply:
        print("[local] dry run; pass --apply to verify them")
        return

    for tool in ("concordium-client", "docker", "cargo-concordium", "cargo-concordium-5"):
        if shutil.which(tool) is None:
            print(f"[local] warning: {tool} is not on the PATH")

    subscriber = Subscriber(
        GRPCClient(),
        tooter,
        MongoMotor(tooter, nearest=True, caller_name="ms_modules"),
        mongodb,
        ConcordiumClient(tooter=tooter),
    )
    for m in modules:
        print(f"[local] verifying {m['_id']}")
        await subscriber.verify_module(net, subscriber.concordium_client, m["_id"])
        after = db[Collections.modules].find_one({"_id": m["_id"]}, {"verification": 1}) or {}
        verification = after.get("verification") or {}
        print(f"        -> {verification.get('verified')} {verification.get('explanation')}")
    print("[local] done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("refs", nargs="*", help="module refs; default: every retryable failure")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.apply, args.refs))
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
