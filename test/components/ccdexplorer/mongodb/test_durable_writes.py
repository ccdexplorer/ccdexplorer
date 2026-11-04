"""Majority write concern, for the clients that ask for it.

A `w=1` write is acknowledged by the primary before any secondary has it, so
an election throws it away. On 2026-10-30 at 00:14 UTC the monthly
unique-addresses run examined 1,417,361 documents in 10.5 seconds, mongod
stopped answering replication heartbeats, ccd-100 lost the primary, and eleven
seconds of writes that had all reported success were rolled back.

These tests are about the option, not the driver: they assert the kwargs handed
to MongoClient, because that is the whole of the behaviour and the alternative
is a replica set in CI.
"""

from ccdexplorer.mongodb.core import DURABLE_WRITE_TIMEOUT_MS, _client_kwargs


def test_the_default_is_unchanged():
    """Every other service keeps w=1. Majority costs a round trip to a
    secondary per write, which the per-block ingest path does thousands of
    times and does not need -- it can recompute from the chain."""
    kwargs = _client_kwargs(nearest=False, caller_name="heartbeat")

    assert "w" not in kwargs
    assert "wtimeoutms" not in kwargs


def test_durable_writes_asks_for_a_majority():
    kwargs = _client_kwargs(nearest=False, caller_name="x", durable_writes=True)

    assert kwargs["w"] == "majority"


def test_a_majority_write_cannot_wait_for_ever():
    """Without a timeout a write blocks as long as the set cannot reach
    majority, which would wedge a nightly job behind a dead secondary."""
    kwargs = _client_kwargs(nearest=False, caller_name="x", durable_writes=True)

    assert kwargs["wtimeoutms"] == DURABLE_WRITE_TIMEOUT_MS
    assert DURABLE_WRITE_TIMEOUT_MS >= 10_000, "too tight for a set across the tunnel"


def test_it_does_not_disturb_the_other_options():
    """appname and maxPoolSize are what make a connection attributable and
    bounded; a new option must not displace them."""
    kwargs = _client_kwargs(nearest=True, caller_name="dagster_nightrunner", durable_writes=True)

    assert kwargs["appname"] == "ccdexplorer:dagster_nightrunner"
    assert "maxPoolSize" in kwargs
    assert "read_preference" in kwargs


def test_the_nightrunner_asks_for_durable_writes():
    """The reason the option exists. Read off the source rather than by
    building a client, which would need a replica set."""
    import inspect

    from ccdexplorer.dagster_nightrunner.src import _resources

    source = inspect.getsource(_resources.shared_mongodb)

    assert "durable_writes=True" in source
