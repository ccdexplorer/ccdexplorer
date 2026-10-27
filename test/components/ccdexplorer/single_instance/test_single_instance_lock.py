"""One instance of a service at a time.

`heartbeat` and `ms_block_analyser` are the only two services in the estate
that cannot safely run twice. Every other `ms_*` service is a Celery worker
consuming a named queue, where a second copy is how you scale rather than a
bug. These two are different:

- heartbeat reads its position from a single Mongo document, requests the next
  run of heights, processes them and writes the new height back. Two copies
  read the same height, fetch the same blocks and race on the write.
- ms_block_analyser tails a Mongo change stream and publishes one Celery task
  per processor per block. Two copies publish every block twice, so every
  worker downstream does its work twice, and they also share one resume-token
  key in Redis.

Nothing prevented that before this lock: the guarantee was "only one copy is
deployed", which holds right up to the moment you fail a host over and the
original comes back with `restart: always`.

The lease is deliberately time-bound rather than a flag someone has to clear.
A container that is SIGKILLed never runs its release path, and a lock that
needs manual clearing after a crash is worse than no lock at all -- it turns
an outage into an outage plus a puzzle.
"""

import threading
import time

import mongomock
import pytest

from ccdexplorer.single_instance import SingleInstanceLock


@pytest.fixture
def collection():
    return mongomock.MongoClient().concordium_mainnet.helpers


class _Clock:
    """A hand-wound clock, so expiry tests don't pay for real seconds."""

    def __init__(self, now: float = 1_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _lock(collection, service="heartbeat", holder="A", lease=60.0, clock=None):
    return SingleInstanceLock(
        collection=collection,
        service=service,
        holder=holder,
        lease_seconds=lease,
        now_fn=clock or time.time,
    )


# --- claiming -------------------------------------------------------------


def test_a_free_lock_is_acquired(collection):
    assert _lock(collection).acquire() is True


def test_a_second_instance_cannot_acquire_while_the_lease_is_live(collection):
    """The whole reason this component exists."""
    clock = _Clock()
    first = _lock(collection, holder="ccd-1", clock=clock)
    second = _lock(collection, holder="ccd-100", clock=clock)

    assert first.acquire() is True
    assert second.acquire() is False


def test_the_same_holder_can_reacquire_its_own_lease(collection):
    """A process that calls acquire twice has not lost anything."""
    clock = _Clock()
    lock = _lock(collection, clock=clock)

    assert lock.acquire() is True
    assert lock.acquire() is True


def test_an_expired_lease_is_taken_over(collection):
    """A holder that was SIGKILLed must not hold the lock for ever."""
    clock = _Clock()
    dead = _lock(collection, holder="ccd-1", lease=60.0, clock=clock)
    live = _lock(collection, holder="ccd-100", lease=60.0, clock=clock)

    assert dead.acquire() is True
    assert live.acquire() is False

    clock.advance(61)

    assert live.acquire() is True


def test_two_services_do_not_share_a_lock(collection):
    """heartbeat and ms_block_analyser both run; they just each run once."""
    clock = _Clock()
    heartbeat = _lock(collection, service="heartbeat", clock=clock)
    analyser = _lock(collection, service="ms_block_analyser", clock=clock)

    assert heartbeat.acquire() is True
    assert analyser.acquire() is True


def test_the_same_service_on_another_net_is_a_separate_lock():
    """Separation per net comes from the collection, which is per-net."""
    client = mongomock.MongoClient()
    clock = _Clock()
    mainnet = _lock(client.concordium_mainnet.helpers, holder="A", clock=clock)
    testnet = _lock(client.concordium_testnet.helpers, holder="A", clock=clock)

    assert mainnet.acquire() is True
    assert testnet.acquire() is True


# --- renewing -------------------------------------------------------------


def test_renew_extends_the_lease(collection):
    clock = _Clock()
    lock = _lock(collection, lease=60.0, clock=clock)
    lock.acquire()

    clock.advance(30)
    assert lock.renew() is True

    clock.advance(31)  # past the original expiry, inside the renewed one
    competitor = _lock(collection, holder="other", clock=clock)
    assert competitor.acquire() is False


def test_renew_fails_once_another_instance_has_taken_over(collection):
    """What a service needs to hear so it can stand down."""
    clock = _Clock()
    original = _lock(collection, holder="ccd-1", lease=60.0, clock=clock)
    original.acquire()

    clock.advance(61)
    assert _lock(collection, holder="ccd-100", clock=clock).acquire() is True

    assert original.renew() is False


def test_renew_without_acquiring_does_not_create_a_lock(collection):
    lock = _lock(collection)

    assert lock.renew() is False
    assert collection.count_documents({}) == 0


# --- releasing ------------------------------------------------------------


def test_release_frees_it_immediately(collection):
    clock = _Clock()
    first = _lock(collection, holder="ccd-1", clock=clock)
    first.acquire()
    first.release()

    assert _lock(collection, holder="ccd-100", clock=clock).acquire() is True


def test_release_by_a_non_holder_leaves_the_lock_alone(collection):
    clock = _Clock()
    holder = _lock(collection, holder="ccd-1", clock=clock)
    holder.acquire()

    _lock(collection, holder="ccd-100", clock=clock).release()

    assert holder.renew() is True


# --- the renewal thread ---------------------------------------------------


def test_the_renewal_thread_keeps_a_short_lease_alive(collection):
    """Real time and a real thread: the lease outlives its own duration."""
    lock = _lock(collection, holder="ccd-1", lease=0.3)
    assert lock.acquire() is True

    lock.start_renewing()
    try:
        time.sleep(0.5)  # well past the original 0.3s lease
        assert _lock(collection, holder="ccd-100").acquire() is False
    finally:
        lock.stop_renewing()


def test_a_lost_lease_calls_back_so_the_service_can_exit(collection):
    """The renewer's only job on failure is to tell the service."""
    lost = threading.Event()
    lock = _lock(collection, holder="ccd-1", lease=0.2)
    lock.acquire()
    collection.delete_many({})  # someone else cleared it out from under us
    _lock(collection, holder="ccd-100").acquire()

    lock.start_renewing(on_lost=lost.set)
    try:
        assert lost.wait(timeout=3) is True
    finally:
        lock.stop_renewing()


def test_hold_releases_on_the_way_out(collection):
    clock = _Clock()
    with _lock(collection, holder="ccd-1", clock=clock).hold() as acquired:
        assert acquired is True

    assert _lock(collection, holder="ccd-100", clock=clock).acquire() is True


def test_hold_yields_false_when_another_instance_has_it(collection):
    """The caller decides what to do; hold does not raise."""
    clock = _Clock()
    _lock(collection, holder="ccd-1", clock=clock).acquire()

    with _lock(collection, holder="ccd-100", clock=clock).hold() as acquired:
        assert acquired is False
