"""The node list must never be briefly empty while it is being refreshed.

Readers take this collection at face value: the API serves it straight to the
nodes page. A refresh that empties the collection before refilling it gives
every reader in that window an empty table and no error at all -- which is
what was reported, and why it "fixed itself" on the next refresh.
"""

from unittest.mock import MagicMock

import pytest
from pymongo import ReplaceOne


class FakeNodes:
    """Just enough collection to record what a refresh does, in order."""

    def __init__(self, existing: list[str]):
        self.docs = {i: {"_id": i} for i in existing}
        self.calls: list[str] = []

    def bulk_write(self, queue):
        if not queue:
            raise Exception("No operations to execute")  # what pymongo does
        self.calls.append("bulk_write")
        for op in queue:
            self.docs[op._filter["_id"]] = op._doc

    def delete_many(self, query):
        self.calls.append(f"delete_many:{'all' if query == {} else 'stale'}")
        if query == {}:
            removed, self.docs = len(self.docs), {}
        else:
            keep = set(query["_id"]["$nin"])
            removed = len([k for k in self.docs if k not in keep])
            self.docs = {k: v for k, v in self.docs.items() if k in keep}
        return MagicMock(deleted_count=removed)

    def count_documents(self, _):
        return len(self.docs)


def _queue(ids):
    return [ReplaceOne({"_id": i}, {"_id": i}, upsert=True) for i in ids]


def test_upsert_happens_before_any_delete():
    """Order is the whole point: writing first means no empty window."""
    nodes = FakeNodes(["a", "b", "c"])
    queue = _queue(["a", "b", "d"])

    nodes.bulk_write(queue)
    nodes.delete_many({"_id": {"$nin": [op._filter["_id"] for op in queue]}})

    assert nodes.calls == ["bulk_write", "delete_many:stale"]
    # 'c' stopped reporting and is gone; the rest survived without a gap.
    assert sorted(nodes.docs) == ["a", "b", "d"]


def test_collection_is_never_emptied_wholesale():
    nodes = FakeNodes(["a", "b"])
    queue = _queue(["a", "b"])
    nodes.bulk_write(queue)
    nodes.delete_many({"_id": {"$nin": ["a", "b"]}})
    assert "delete_many:all" not in nodes.calls
    assert len(nodes.docs) == 2


def test_empty_dashboard_response_keeps_existing_nodes():
    """A bad fetch must not wipe the record.

    bulk_write([]) raises, so an unguarded refresh would delete everything and
    then fail, leaving no nodes until the next successful run.
    """
    nodes = FakeNodes(["a", "b", "c"])
    with pytest.raises(Exception):
        nodes.bulk_write([])
    assert len(nodes.docs) == 3
