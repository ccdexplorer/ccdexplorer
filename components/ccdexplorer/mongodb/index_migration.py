"""Export/replay real index definitions between network databases.

Devnets get reset frequently, and the historical index-creation logic in
`heartbeat_sub/start_over.py` has drifted from what mainnet actually runs.
This exports the real index definitions straight from a source database
(e.g. mainnet) and replays them onto a target database (e.g. a freshly
dropped devnet), so the source of truth is the live database, not
hand-maintained code.
"""

from __future__ import annotations

import json
from pathlib import Path

from pymongo.database import Database


def export_indices(db: Database) -> dict[str, list[dict]]:
    """Real index definitions per collection, excluding views and the default _id_ index."""
    indices: dict[str, list[dict]] = {}
    for name in db.list_collection_names(filter={"type": "collection"}):
        if name.startswith("system."):
            continue
        specs = []
        for idx in db[name].list_indexes():
            idx = dict(idx)
            if idx["name"] == "_id_":
                continue
            key = dict(idx.pop("key"))
            idx.pop("v", None)
            idx.pop("ns", None)
            specs.append({"key": key, "options": idx})
        if specs:
            indices[name] = specs
    return indices


def apply_indices(db: Database, indices: dict[str, list[dict]]) -> None:
    """Recreate indices; create_index is a no-op if an equivalent index already exists."""
    for name, specs in indices.items():
        for spec in specs:
            db[name].create_index(list(spec["key"].items()), **spec["options"])


def _main() -> None:
    import argparse

    from pymongo import MongoClient

    from ccdexplorer.env import MONGO_URI, DEVNET_MONGO_URI

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)

    p_export = sub.add_parser("export", help="Export real index definitions from a database")
    p_export.add_argument("--db", default="concordium_mainnet")
    p_export.add_argument("--out", default="mainnet_indices.json")

    p_apply = sub.add_parser("apply", help="Apply exported index definitions to a database")
    p_apply.add_argument("--db", default="concordium_devnet")
    p_apply.add_argument("--file", default="mainnet_indices.json")

    args = parser.parse_args()

    if args.action == "export":
        # Reading from mainnet (or whatever --db is given) needs the regular,
        # broader-access connection string.
        client = MongoClient(MONGO_URI)
        indices = export_indices(client[args.db])
        Path(args.out).write_text(json.dumps(indices, indent=2, default=str))
        print(f"Wrote {sum(len(v) for v in indices.values())} index specs to {args.out}")
    else:
        # Applying targets devnet by default, so prefer the narrower,
        # devnet-only credentials if they've been configured.
        client = MongoClient(DEVNET_MONGO_URI)
        indices = json.loads(Path(args.file).read_text())
        apply_indices(client[args.db], indices)
        print(f"Applied indices for {len(indices)} collections to {args.db}")


if __name__ == "__main__":
    _main()
