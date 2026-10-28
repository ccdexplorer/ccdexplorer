"""The dashboard_nodes cache refresher, now a task inside the api.

The staleness stamp does the work a lock would: both api instances run this
loop, and each skips a net the other has just refreshed. These pin that, and
that one bad fetch neither ends the loop nor stops the other net.
"""

import asyncio
import datetime as dt

import pytest

NOW = dt.datetime(2026, 10, 28, 12, 0, tzinfo=dt.timezone.utc)


def _mod():
    from ccdexplorer.ccdexplorer_api.app import node_cache

    return node_cache


class FakeHelpers:
    def __init__(self, stamp):
        self._stamp = stamp

    def find_one(self, _q):
        return None if self._stamp is None else {"timestamp": self._stamp}


class FakeMongo:
    def __init__(self, stamp=None):
        from ccdexplorer.mongodb import Collections

        self.mainnet = {Collections.helpers: FakeHelpers(stamp)}
        self.testnet = {Collections.helpers: FakeHelpers(stamp)}


# --- the staleness guard --------------------------------------------------


def test_a_fresh_stamp_means_another_instance_just_did_it(monkeypatch):
    m = _mod()
    called = []
    monkeypatch.setattr(m, "refresh_dashboard_nodes", lambda *a: called.append(a))

    did = m.refresh_if_stale(FakeMongo(stamp=NOW - dt.timedelta(seconds=10)), "mainnet", now=NOW)

    assert did is False
    assert called == []


def test_a_stale_stamp_refreshes(monkeypatch):
    m = _mod()
    called = []
    monkeypatch.setattr(m, "refresh_dashboard_nodes", lambda *a: called.append(a))

    did = m.refresh_if_stale(FakeMongo(stamp=NOW - dt.timedelta(seconds=120)), "mainnet", now=NOW)

    assert did is True
    assert len(called) == 1


def test_no_stamp_at_all_refreshes(monkeypatch):
    """First run on a fresh database, or the stamp was never written."""
    m = _mod()
    called = []
    monkeypatch.setattr(m, "refresh_dashboard_nodes", lambda *a: called.append(a))

    assert m.refresh_if_stale(FakeMongo(stamp=None), "mainnet", now=NOW) is True
    assert len(called) == 1


def test_a_naive_stamp_is_read_as_utc(monkeypatch):
    m = _mod()
    monkeypatch.setattr(m, "refresh_dashboard_nodes", lambda *a: None)

    naive = (NOW - dt.timedelta(seconds=5)).replace(tzinfo=None)

    assert m.refresh_if_stale(FakeMongo(stamp=naive), "mainnet", now=NOW) is False


# --- the loop -------------------------------------------------------------


async def test_the_loop_covers_every_dashboard_net(monkeypatch):
    m = _mod()
    seen = []
    monkeypatch.setattr(m, "refresh_if_stale", lambda mongo, net, *a: seen.append(net) or True)

    await m.run_refresher(FakeMongo(), iterations=1, interval=0)

    assert seen == ["mainnet", "testnet"]


async def test_one_net_failing_does_not_stop_the_other(monkeypatch):
    m = _mod()
    seen = []

    def flaky(mongo, net, *a):
        seen.append(net)
        if net == "mainnet":
            raise RuntimeError("dashboard 503")
        return True

    monkeypatch.setattr(m, "refresh_if_stale", flaky)

    await m.run_refresher(FakeMongo(), iterations=1, interval=0)

    assert seen == ["mainnet", "testnet"]


async def test_it_is_cancellable(monkeypatch):
    """The lifespan cancels it on shutdown; it must not swallow that."""
    m = _mod()
    monkeypatch.setattr(m, "refresh_if_stale", lambda *a: True)

    task = asyncio.create_task(m.run_refresher(FakeMongo(), interval=10))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_devnet_is_never_refreshed(monkeypatch):
    """It publishes no dashboard; it used to be fed mainnet's nodes."""
    m = _mod()

    assert "devnet" not in tuple(m.DASHBOARDS)
