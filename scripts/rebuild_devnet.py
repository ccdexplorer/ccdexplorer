#!/usr/bin/env python3
"""
Tear down and rebuild the concordium_devnet MongoDB database.

This script only ever touches devnet - the database name and Redis key
prefix are hardcoded, not parameterized, since devnet is reset far more
often than mainnet/testnet and a stray flag should never be able to point
this at either of those.

Steps:
  1. Drop the concordium_devnet database entirely.
  2. Recreate every collection in the `Collections` enum, so the schema
     exists even for collections that have no secondary indices (and thus
     wouldn't otherwise get created until something first writes to them).
  3. Recreate indices from a previously exported index file
     (see `ccdexplorer.mongodb.index_migration` / `python -m
     ccdexplorer.mongodb.index_migration export ...`).
  4. Reset the heartbeat indexer's progress marker (`Collections.helpers`)
     so it walks the chain from `--start-height` on next run.
  5. Clear the devnet-scoped Redis keys (change-stream resume token +
     any leftover Celery queue backlog) so ms_block_analyser and the
     consumers don't try to resume from state that no longer exists.

After running this, start heartbeat / ms_block_analyser / ms_indexers /
ms_plt / etc. with RUN_ON_NET=devnet and they will walk the (short) chain
from scratch and repopulate everything.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import redis as redis_lib
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

from ccdexplorer.env import DEVNET_MONGO_URI
from ccdexplorer.mongodb import Collections
from ccdexplorer.mongodb.index_migration import apply_indices

DB_NAME = "concordium_devnet"
REDIS_KEY_PATTERN = "devnet:*"

FORBIDDEN_DB_NAMES = ("concordium_mainnet", "concordium_testnet")
FORBIDDEN_REDIS_PREFIXES = ("mainnet", "testnet")


def _guard_devnet_db(db_name: str) -> None:
    """Independent, redundant checks: abort unless db_name is unambiguously devnet.

    Each check is deliberately overlapping so a future edit to any one of them
    (or to DB_NAME itself) can't silently open the door to mainnet/testnet.
    """
    if db_name != DB_NAME:
        raise RuntimeError(
            f"Refusing to operate on {db_name!r}: does not match the hardcoded {DB_NAME!r}."
        )
    if db_name in FORBIDDEN_DB_NAMES:
        raise RuntimeError(f"Refusing to operate on {db_name!r}: mainnet/testnet are forbidden.")
    if "mainnet" in db_name or "testnet" in db_name:
        raise RuntimeError(f"Refusing to operate on {db_name!r}: name contains mainnet/testnet.")
    if "devnet" not in db_name:
        raise RuntimeError(f"Refusing to operate on {db_name!r}: does not contain 'devnet'.")


def _guard_devnet_redis_pattern(pattern: str) -> None:
    """Same redundant-checks approach as `_guard_devnet_db`, for the Redis key pattern."""
    if pattern != REDIS_KEY_PATTERN:
        raise RuntimeError(
            f"Refusing to use Redis pattern {pattern!r}: "
            f"does not match the hardcoded {REDIS_KEY_PATTERN!r}."
        )
    if pattern.startswith(FORBIDDEN_REDIS_PREFIXES):
        raise RuntimeError(f"Refusing to use Redis pattern {pattern!r}: mainnet/testnet forbidden.")
    if not pattern.startswith("devnet"):
        raise RuntimeError(f"Refusing to use Redis pattern {pattern!r}: must start with 'devnet'.")


def drop_database() -> None:
    _guard_devnet_db(DB_NAME)
    client = MongoClient(DEVNET_MONGO_URI)
    client.drop_database(DB_NAME)
    print(f"Dropped database {DB_NAME}")


def create_collections() -> None:
    _guard_devnet_db(DB_NAME)
    client = MongoClient(DEVNET_MONGO_URI)
    db = client[DB_NAME]
    created = 0
    for collection in Collections:
        try:
            db.create_collection(collection.value)
            created += 1
        except CollectionInvalid:
            pass  # already exists
    print(f"Created {created} collections in {DB_NAME} ({len(list(Collections))} total)")


def rebuild_indices(indices_file: str) -> None:
    _guard_devnet_db(DB_NAME)
    client = MongoClient(DEVNET_MONGO_URI)
    indices = json.loads(Path(indices_file).read_text())
    apply_indices(client[DB_NAME], indices)
    print(f"Applied indices for {len(indices)} collections to {DB_NAME}")


def seed_heartbeat_progress(start_height: int) -> None:
    _guard_devnet_db(DB_NAME)
    client = MongoClient(DEVNET_MONGO_URI)
    client[DB_NAME]["helpers"].update_one(
        {"_id": "heartbeat_last_processed_block"},
        {"$set": {"height": start_height}},
        upsert=True,
    )
    print(f"Seeded heartbeat progress doc at height {start_height} in {DB_NAME}")


def clear_redis_state(redis_url: str) -> None:
    _guard_devnet_redis_pattern(REDIS_KEY_PATTERN)
    r = redis_lib.Redis.from_url(redis_url)
    keys = list(r.scan_iter(match=REDIS_KEY_PATTERN))
    if keys:
        r.delete(*keys)
    print(f"Cleared {len(keys)} Redis keys matching {REDIS_KEY_PATTERN!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url", default=None, help="Skip Redis cleanup if omitted")
    parser.add_argument(
        "--indices-file",
        default="mainnet_indices.json",
        help="Index definitions exported via `python -m ccdexplorer.mongodb.index_migration export`",
    )
    parser.add_argument(
        "--start-height",
        type=int,
        default=-1,
        help="Height to seed the heartbeat progress doc with (-1 = start from genesis)",
    )
    args = parser.parse_args()

    drop_database()
    create_collections()
    rebuild_indices(args.indices_file)
    seed_heartbeat_progress(args.start_height)
    if args.redis_url:
        clear_redis_state(args.redis_url)


if __name__ == "__main__":
    main()
