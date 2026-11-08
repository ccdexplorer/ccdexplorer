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
    """Records both severities separately.

    `messages` stays every message, whichever way it went, so the assertions
    written before alert() existed still mean what they meant. `paged` is only
    what went to Bark as well -- the split is the point of the change, and a
    test that cannot see it cannot pin it.
    """

    def __init__(self):
        self.messages = []
        self.paged = []
        self.keys = []

    def relay(self, *, channel, title, chat_id, body, notifier_type, **kw):
        self.messages.append(body)

    def critical(self, message, key=None, chat_id=None):
        self.messages.append(message)
        self.paged.append(message)
        self.keys.append(key)


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


# --- which conditions page, and which only inform -------------------------
#
# Every alert used to leave through one inform() as REQUESTS_ERROR, so
# "ReplicaSet has no PRIMARY" and "Primary: ccd-100" were indistinguishable.
# These pin the split: `paged` is what also rings the phone via Bark.
# Getting it wrong in either direction is bad -- a missed outage, or a pager
# that cries wolf until it is muted.


async def test_no_primary_pages():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={"members": [_member("a", 2, "SECONDARY"), _member("b", 2, "SECONDARY")]},
            hello={"primary": None},
        ),
    )

    await wt.check_mongo()

    assert any("no PRIMARY" in m for m in tooter.paged)
    assert "watchtower:no-primary" in tooter.keys


async def test_a_member_in_a_bad_state_pages():
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={
                "members": [
                    _member("a"),
                    _member("b", 2, "SECONDARY"),
                    _member("c", 8, "DOWN"),
                ]
            },
            hello={"primary": "a"},
        ),
    )

    await wt.check_mongo()

    assert any("has state DOWN" in m for m in tooter.paged)


async def test_a_slow_ping_does_not_page():
    """The one I would most regret paging on: PING_MS is a single fixed
    threshold for a 1.4ms datacenter member and a 20ms member behind a
    consumer line, and pingMs is an EWMA that has decayed by the time anyone
    looks. It stays an inform.
    """
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={
                "members": [
                    _member("a"),
                    _member("b", 2, "SECONDARY", ping=431),
                    _member("c", 2, "SECONDARY"),
                ]
            },
            hello={"primary": "a"},
        ),
    )

    await wt.check_mongo()

    assert any("431 ms" in m for m in tooter.messages)
    assert tooter.paged == []


async def test_a_failed_check_pages_rather_than_passing_silently():
    """If the check itself cannot run we are blind, which is worse than any
    single finding it could have reported."""
    core = _core()
    tooter = FakeTooter()
    wt = _wt(core, tooter=tooter, mongodb=FakeMongo(status={"bad": "shape"}))

    await wt.check_mongo()

    assert any("check_mongo:" in m for m in tooter.paged)


async def test_a_stalled_heartbeat_pages_per_net():
    """Nothing else in the estate watches indexer liveness -- live-everything
    has no heartbeat or last_processed_block check at all."""
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            mainnet=[_marker("heartbeat_last_processed_block", 50)],
            testnet=[_marker("heartbeat_last_processed_block", 100)],
        ),
    )

    await wt.check_heartbeat_running()

    assert any("mainnet: Heartbeat lagging 50" in m for m in tooter.paged)
    assert "watchtower:heartbeat-lagging:mainnet" in tooter.keys


async def test_a_lagging_bot_does_not_page():
    """Notifications arrive late; nothing is down. It stays an inform."""
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
    assert tooter.paged == []


async def test_every_paged_alert_carries_a_stable_key():
    """Replication lag and member state both name the member, because the
    message text carries a number or state that changes between checks."""
    core = _core()
    tooter = FakeTooter()
    wt = _wt(
        core,
        tooter=tooter,
        mongodb=FakeMongo(
            status={
                "members": [
                    _member("a"),
                    _member("b", 2, "SECONDARY", optime=NOW - dt.timedelta(minutes=30)),
                    _member("c", 2, "SECONDARY"),
                ]
            },
            hello={"primary": "a"},
        ),
    )

    await wt.check_mongo()

    assert "watchtower:member-lag:b" in tooter.keys
    assert all(k and k.startswith("watchtower:") for k in tooter.keys)
