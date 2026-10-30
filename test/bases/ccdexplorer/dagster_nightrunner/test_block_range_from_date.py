"""The end of a block range has to be a real height, not a sentinel.

`blocks_per_day` only gets a row once a day is complete, so a lookup for today
finds nothing. Every weekly and monthly run lands there too, on purpose:
week_end and month_end always sit ahead of the date being processed. The old
fallback returned 1_000_000_000, which is not a bound -- the $match it feeds
runs to the head of the chain and well past it.

That is how j_unique_addresses came to scan the whole month every night. On
2026-10-30 at 00:14 UTC the monthly run matched blocks 52,363,293 to
1,000,000,000 over impacted_addresses: 1,417,361 documents examined to return
two numbers, 10.5 seconds of solid work. mongod stopped answering replication
heartbeats, ccd-10 and the mini elected a new primary, and ccd-100 rolled back
eleven seconds of writes across twelve collections.

Reading the real head costs one indexed lookup -- blocks is indexed on height
descending. It does not shrink the scan on its own, because there are no
documents above the head; it stops the range claiming heights that cannot
exist, which is what the covering index then has to be trusted with.
"""

from ccdexplorer.dagster_nightrunner.nightrunner.utils import get_start_end_block_from_date
from ccdexplorer.mongodb import Collections

SENTINEL = 1_000_000_000


class _Collection:
    """Just enough of a pymongo collection for this one lookup."""

    def __init__(self, docs):
        self.docs = docs

    def find_one(self, query=None, sort=None):
        matches = [d for d in self.docs if all(d.get(k) == v for k, v in (query or {}).items())]
        for key, direction in reversed(sort or []):
            matches.sort(key=lambda d: d[key], reverse=direction < 0)
        return matches[0] if matches else None


class _Mongo:
    def __init__(self, days=(), blocks=()):
        self.mainnet = {
            Collections.blocks_per_day: _Collection(list(days)),
            Collections.blocks: _Collection(list(blocks)),
        }


def _day(date, first, last):
    return {"date": date, "height_for_first_block": first, "height_for_last_block": last}


# --- the day is complete -------------------------------------------------


def test_a_finished_day_is_read_straight_off_its_row():
    mongo = _Mongo(
        days=[_day("2026-10-29", 53_500_000, 53_510_000)], blocks=[{"height": 53_600_000}]
    )

    assert get_start_end_block_from_date(mongo, "2026-10-29") == (53_500_000, 53_510_000)


# --- the day is not finished ---------------------------------------------


def test_an_unfinished_day_ends_at_the_head_of_the_chain():
    """The whole point. 1_000_000_000 is roughly twenty times the real height."""
    mongo = _Mongo(
        days=[_day("2026-10-29", 53_500_000, 53_510_000)],
        blocks=[{"height": 53_512_345}, {"height": 53_512_000}],
    )

    start, end = get_start_end_block_from_date(mongo, "2026-10-30")

    assert start == 53_510_001
    assert end == 53_512_345


def test_a_month_end_in_the_future_is_bounded_too():
    """month_end is always ahead of the date being processed, so this is the
    path the monthly run takes every single night."""
    mongo = _Mongo(
        days=[_day("2026-10-29", 53_500_000, 53_510_000)],
        blocks=[{"height": 53_512_345}],
    )

    _, end = get_start_end_block_from_date(mongo, "2026-10-31")

    assert end != SENTINEL
    assert end == 53_512_345


def test_a_date_with_nothing_before_it_starts_at_zero():
    mongo = _Mongo(days=[], blocks=[{"height": 1_234}])

    assert get_start_end_block_from_date(mongo, "2026-10-30") == (0, 1_234)


def test_an_empty_chain_falls_back_rather_than_crashing():
    """A devnet that has been dropped has no blocks at all. Returning the old
    sentinel there is harmless -- nothing can match it."""
    mongo = _Mongo(days=[], blocks=[])

    assert get_start_end_block_from_date(mongo, "2026-10-30") == (0, SENTINEL)


def test_the_head_is_fetched_by_the_indexed_sort():
    """blocks is indexed on height descending. Scanning it any other way on a
    collection this size would cost more than the bug it fixes."""
    seen = {}

    class _Recorder(_Collection):
        def find_one(self, query=None, sort=None):
            seen["query"], seen["sort"] = query, sort
            return {"height": 7}

    mongo = _Mongo(days=[], blocks=[])
    mongo.mainnet[Collections.blocks] = _Recorder([])

    get_start_end_block_from_date(mongo, "2026-10-30")

    assert seen["query"] == {}
    assert seen["sort"] == [("height", -1)]
