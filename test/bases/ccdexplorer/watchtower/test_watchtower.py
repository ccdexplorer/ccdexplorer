"""The watchtower's checks, after the move off ccdexplorer_fundamentals.

Four of these pin bugs the old version had, which is most of why the port was
worth doing rather than just repointing imports:

  * `check_mongo` found the primary with `next(...)` outside its try, so an
    election (no PRIMARY) crashed the check that exists to report it.
  * `for net in NET` started including DEVNET, while the database was picked
    with `mainnet if net == NET.MAINNET else testnet` -- reading devnet
    heights out of the testnet database.
  * a missing helper marker raised KeyError.
"""

import datetime as dt
from types import SimpleNamespace

import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.mongodb import Collections

NOW = dt.datetime(2026, 10, 27, 20, 0, tzinfo=dt.timezone.utc)


def _core():
    """Resolved per call, never bound at collection time."""
    from ccdexplorer.watchtower import core

    return core


class FakeTooter:
    def __init__(self):
        self.messages = []

    def relay(self, *, channel, title, chat_id, body, notifier_type, **kw):
        self.messages.append(body)


class FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    def find(self, _query):
        return list(self._docs)


class FakeMongo:
    """Shaped for net_db, which does getattr(container, "mainnet"|"testnet")."""

    def __init__(self, mainnet=(), testnet=(), devnet=None, status=None, hello=None):
        self.mainnet = {Collections.helpers: FakeCollection(mainnet)}
        self.testnet = {Collections.helpers: FakeCollection(testnet)}
        if devnet is not None:
            self.devnet = {Collections.helpers: FakeCollection(devnet)}
        self.connection = SimpleNamespace(
            admin=SimpleNamespace(
                command=lambda name: status if name == "replSetGetStatus" else hello
            )
        )


class FakeGRPC:
    def __init__(self, heights):
        self.heights = heights
        self.asked = []

    def get_block_info(self, block_input, net):
        self.asked.append((block_input, net))
        return SimpleNamespace(height=self.heights[net])


def _wt(core, **kw):
    kw.setdefault("grpcclient", FakeGRPC({NET.MAINNET: 100, NET.TESTNET: 100}))
    kw.setdefault("tooter", FakeTooter())
    kw.setdefault("mongodb", FakeMongo())
    kw.setdefault("motormongo", FakeMongo())
    return core.WatchTower(**kw)


def _marker(_id, height):
    return {"_id": _id, "height": height}


# --- heartbeat lag ---------------------------------------------------------


async def test_a_lagging_heartbeat_is_reported():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            mainnet=[_marker("heartbeat_last_processed_block", 50)],
            testnet=[_marker("heartbeat_last_processed_block", 100)],
        ),
        grpcclient=FakeGRPC({NET.MAINNET: 100, NET.TESTNET: 100}),
    )

    await wt.check_heartbeat_running()

    assert any("mainnet: Heartbeat lagging 50 blocks" in m for m in tooter.messages)
    assert not any("testnet" in m for m in tooter.messages)


async def test_a_heartbeat_within_tolerance_is_silent():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            mainnet=[_marker("heartbeat_last_processed_block", 95)],
            testnet=[_marker("heartbeat_last_processed_block", 95)],
        ),
    )

    await wt.check_heartbeat_running()

    assert tooter.messages == []


async def test_devnet_is_not_checked_against_the_testnet_database():
    """The old net/database pairing would have read devnet out of testnet."""
    core = _core()
    grpc = FakeGRPC({NET.MAINNET: 100, NET.TESTNET: 100})
    wt = _wt(
        core,
        mongodb=FakeMongo(
            mainnet=[_marker("heartbeat_last_processed_block", 100)],
            testnet=[_marker("heartbeat_last_processed_block", 100)],
            devnet=[_marker("heartbeat_last_processed_block", 1)],
        ),
        grpcclient=grpc,
    )

    await wt.check_heartbeat_running()

    assert [n for _, n in grpc.asked] == [NET.MAINNET, NET.TESTNET]
    assert NET.DEVNET not in core.WATCHED_NETS


async def test_a_missing_marker_informs_instead_of_raising():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(core, tooter=tooter, mongodb=FakeMongo(mainnet=[], testnet=[]))

    await wt.check_heartbeat_running()

    assert len(tooter.messages) == 2
    assert all("no heartbeat_last_processed_block" in m for m in tooter.messages)


# --- bot lag --------------------------------------------------------------


async def test_a_lagging_bot_is_reported():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            mainnet=[
                _marker("heartbeat_last_processed_block", 200),
                _marker("bot_last_processed_block", 150),
            ]
        ),
    )

    await wt.check_bot()

    assert tooter.messages == ["Watchtower: Bot lagging 50 blocks."]


async def test_a_current_bot_is_silent():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            mainnet=[
                _marker("heartbeat_last_processed_block", 200),
                _marker("bot_last_processed_block", 199),
            ]
        ),
    )

    await wt.check_bot()

    assert tooter.messages == []


# --- replica set ----------------------------------------------------------


def _member(name, state=1, stateStr="PRIMARY", ping=None, optime=NOW):
    return {
        "name": name,
        "state": state,
        "stateStr": stateStr,
        "pingMs": ping,
        "optimeDate": optime,
    }


async def test_a_healthy_three_member_set_is_silent():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={
                "members": [
                    _member("a"),
                    _member("b", 2, "SECONDARY", ping=10),
                    _member("c", 2, "SECONDARY", ping=20),
                ]
            },
            hello={"primary": "a"},
        ),
    )

    await wt.check_mongo()

    assert tooter.messages == []


async def test_no_primary_is_reported_rather_than_crashing():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={
                "members": [
                    _member("a", 2, "SECONDARY"),
                    _member("b", 2, "SECONDARY"),
                    _member("c", 2, "SECONDARY"),
                ]
            },
            hello={"primary": None},
        ),
    )

    await wt.check_mongo()

    assert any("no PRIMARY" in m for m in tooter.messages)
    assert not any("check_mongo:" in m for m in tooter.messages)


async def test_a_missing_member_is_reported():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={"members": [_member("a"), _member("b", 2, "SECONDARY")]},
            hello={"primary": "a"},
        ),
    )

    await wt.check_mongo()

    assert any("only 2 replicaset members online" in m for m in tooter.messages)


async def test_a_lagging_secondary_is_reported():
    core = _core()
    tooter = FakeTooter()
    behind = NOW - dt.timedelta(minutes=30)
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={
                "members": [
                    _member("a", optime=NOW),
                    _member("b", 2, "SECONDARY", optime=behind),
                    _member("c", 2, "SECONDARY", optime=NOW),
                ]
            },
            hello={"primary": "a"},
        ),
    )

    await wt.check_mongo()

    assert any("is 30 mins behind primary" in m for m in tooter.messages)
