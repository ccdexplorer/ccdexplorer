"""Indexing one block waits on the node once, not four times in a row.

Catching the devnet up after three days, the indexers ran at ~5.6 blocks a
second. A profile inside the container put ~156 of the ~180 ms per block in
four gRPC calls made one after another -- get_block_transaction_events,
get_block_info, then (in do_specials) get_block_info again and
get_block_special_events -- each ~35-45 ms to the node. The services must
stay one block at a time, so the fix is within the block: ask for what the
block needs together, and ask for block_info once.
"""

import time
from types import SimpleNamespace

import grpc
import pytest

from ccdexplorer.ms_indexers.subscriber import Subscriber

HOLD = 0.03  # seconds each fake node call takes


class _Node:
    """Stands in for GRPCClient; records overlap and how often each call ran."""

    def __init__(self, fail_on: str | None = None):
        self.fail_on = fail_on
        self.in_flight = 0
        self.max_in_flight = 0
        self.calls: list[str] = []

    def _call(self, name, result):
        self.calls.append(name)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            time.sleep(HOLD)
            if name == self.fail_on:
                raise grpc.RpcError()
            return result
        finally:
            self.in_flight -= 1

    def get_block_transaction_events(self, height, net):
        return self._call("tx_events", SimpleNamespace(transaction_summaries=[]))

    def get_block_info(self, height, net):
        return self._call(
            "block_info",
            SimpleNamespace(height=height, hash="h", slot_time="2026-11-25T00:00:00Z"),
        )

    def get_block_special_events(self, height, net):
        event = SimpleNamespace(model_dump=lambda exclude_none=True: {"kind": "reward"})
        return self._call("special_events", [event])


class _Mongo:
    def __init__(self):
        self.written: list[list] = []
        self.connection = self

    def bulk_write(self, ops):
        self.written.append(list(ops))
        return SimpleNamespace(upserted_count=1, matched_count=0, modified_count=0)


def _subscriber(node, mongo):
    # Subscriber.__init__ wires up real mongo/grpc connections; none of that
    # is under test, so skip it and set only what indexing a block reads.
    sub = Subscriber.__new__(Subscriber)
    sub.grpc_client = node
    sub.mongodb = mongo
    sub.net = "devnet"
    sub.namespace = "concordium_devnet"
    sub.queues = {}
    return sub


def test_block_info_is_fetched_once_per_block():
    node = _Node()
    _subscriber(node, _Mongo()).index_block(7_000_000)

    assert node.calls.count("block_info") == 1


def test_the_nodes_answers_are_asked_for_together():
    node = _Node()
    started = time.perf_counter()
    _subscriber(node, _Mongo()).index_block(7_000_000)
    elapsed = time.perf_counter() - started

    assert node.max_in_flight == 3
    # three calls in series would cost 3 * HOLD
    assert elapsed < 2 * HOLD


def test_the_special_events_still_reach_mongo():
    mongo = _Mongo()
    _subscriber(_Node(), mongo).index_block(7_000_000)

    (ops,) = mongo.written
    assert ops[0]._doc == {"_id": 7_000_000, "special_events": [{"kind": "reward"}]}


@pytest.mark.parametrize("failing", ["tx_events", "block_info", "special_events"])
def test_a_failed_call_is_raised_so_celery_can_retry_the_block(failing):
    mongo = _Mongo()
    with pytest.raises(grpc.RpcError):
        _subscriber(_Node(fail_on=failing), mongo).index_block(7_000_000)

    assert mongo.written == []  # nothing half-written for a block that will be retried
