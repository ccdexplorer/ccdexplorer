"""An events-and-impacts block asks the node for its two answers together.

Same catch-up as the indexers: the service managed ~9.5 blocks a second, and
an empty devnet block (nothing to extract) still cost ~105 ms, ~78 of it two
node calls -- get_block_info, then get_block_transaction_events -- made one
after the other. Blocks stay one at a time; only the two calls overlap.
"""

import datetime as dt
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.ms_events_and_impacts.subscriber import Subscriber

HOLD = 0.03


class _Node:
    def __init__(self, fail_on=None):
        self.fail_on = fail_on
        self.in_flight = 0
        self.max_in_flight = 0

    def _call(self, name, result):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            time.sleep(HOLD)
            if name == self.fail_on:
                raise grpc.RpcError()
            return result
        finally:
            self.in_flight -= 1

    def get_block_info(self, height, net):
        fields = {"height": height, "hash": "h", "slot_time": dt.datetime.now(dt.UTC)}
        return self._call("block_info", SimpleNamespace(model_dump=lambda: fields))

    def get_block_transaction_events(self, height, net):
        return self._call("tx_events", SimpleNamespace(transaction_summaries=[]))


def _subscriber(node):
    sub = Subscriber.__new__(Subscriber)
    sub.grpc_client = node
    sub.devnet = MagicMock()  # every collection is a MagicMock
    return sub


async def test_the_two_node_calls_overlap():
    node = _Node()
    started = time.perf_counter()
    await _subscriber(node).process_new_logged_events_from_block(NET.DEVNET, 7_000_000, "h")
    elapsed = time.perf_counter() - started

    assert node.max_in_flight == 2
    assert elapsed < 1.6 * HOLD  # in series it would be 2 * HOLD plus the writes


@pytest.mark.parametrize("failing", ["block_info", "tx_events"])
async def test_a_failed_call_is_raised_so_celery_can_retry_the_block(failing):
    with pytest.raises(grpc.RpcError):
        await _subscriber(_Node(fail_on=failing)).process_new_logged_events_from_block(
            NET.DEVNET, 7_000_000, "h"
        )
