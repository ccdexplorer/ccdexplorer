"""The CCD price series the charts are drawn from.

The chain sets a CCD/EUR rate by update transaction every thirty minutes and
has done since 2021, using it to price transaction fees. It is not a market
feed -- it is governance-set -- but it tracks one to within a fraction of a
percent, and it is the only complete intraday history that exists: the daily
forex job writes one point a day, and that point is a snapshot taken shortly
after midnight, so by mid-morning it is hours stale.

The arithmetic is the part worth pinning. The chain stores microCCD per euro
as a pair of strings, because the numerator outgrows 64 bits, and the price is
its reciprocal scaled by a million. Getting that wrong by a factor of a million
would still look plausible on a chart.
"""

import datetime as dt

import pytest

from ccdexplorer.ccdexplorer_api.app.routers.v2.misc_v2 import (
    CCD_PRICE_INTRADAY_MAX_HOURS,
    CCD_PRICE_MAX_HOURS,
    CCD_PRICE_MAX_POINTS,
    _append_spot,
    _eur_per_ccd,
    _thin,
)


def test_a_real_update_gives_the_price_it_gave_on_chain():
    """Transaction 5e27e5bf…62665, mainnet, 2026-09-24 10:55:09 UTC.

    Whose payload the explorer renders as 0.00307535 EUR per CCD.
    """
    price = _eur_per_ccd({"numerator": "2253292361387868160", "denominator": "6929673929"})
    # Pinned at full precision. 0.00307535 is what the site rounds it to, and
    # asserting against the rounded figure at a tight tolerance fails on the
    # rounding rather than on the arithmetic.
    assert price == pytest.approx(0.003075354999531358, rel=1e-12)
    assert round(price, 8) == 0.00307535


def test_the_first_rate_the_chain_ever_had():
    """2021-09-17: 2,777,778 microCCD to the euro, so 0.36 EUR per CCD."""
    assert _eur_per_ccd({"numerator": "2777778", "denominator": "1"}) == pytest.approx(
        0.36, rel=1e-4
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"numerator": "0", "denominator": "1"},
        {"numerator": "-5", "denominator": "1"},
        {"numerator": "abc", "denominator": "1"},
        {"denominator": "1"},
        {"numerator": "1"},
        {},
        None,
    ],
)
def test_a_payload_that_cannot_be_a_price_is_refused(payload):
    """A zero or missing numerator would divide by zero or invent a number;
    the point is skipped instead."""
    assert _eur_per_ccd(payload or {}) is None


# --- thinning -------------------------------------------------------------


def test_a_short_series_is_left_alone():
    points = [{"at": n} for n in range(10)]
    assert _thin(points, 100) == points


def test_a_long_series_is_capped():
    points = [{"at": n} for n in range(5000)]
    assert len(_thin(points, CCD_PRICE_MAX_POINTS)) == CCD_PRICE_MAX_POINTS


def test_thinning_always_keeps_the_newest_point():
    """It is what the headline change is measured against, so dropping it
    would make the chart disagree with the number printed above it."""
    points = [{"at": n} for n in range(5000)]
    assert _thin(points, 500)[-1] is points[-1]


def test_thinning_keeps_the_oldest_point_too():
    points = [{"at": n} for n in range(5000)]
    assert _thin(points, 500)[0] is points[0]


def test_thinning_preserves_order():
    points = [{"at": n} for n in range(5000)]
    thinned = _thin(points, 500)
    assert [p["at"] for p in thinned] == sorted(p["at"] for p in thinned)


# --- the window -----------------------------------------------------------


def test_intraday_gives_way_to_daily_before_the_series_gets_silly():
    """Thirty-minute resolution over a year is seventeen thousand points."""
    assert CCD_PRICE_INTRADAY_MAX_HOURS * 2 < CCD_PRICE_MAX_POINTS * 2
    assert CCD_PRICE_INTRADAY_MAX_HOURS < CCD_PRICE_MAX_HOURS


def test_a_year_is_reachable():
    assert CCD_PRICE_MAX_HOURS >= 365 * 24


# --- ending on the spot ---------------------------------------------------
#
# Every price chart ends on the current market price, in the line as well as
# the headline. The chain rate is up to thirty minutes old and is a fee rate
# besides, so a chart drawn only from it stops short of the price a reader
# would look up -- and the headline would then disagree with the end of its
# own line.

UTC = dt.timezone.utc


def _points(*hours_ago):
    now = dt.datetime(2026, 9, 24, 16, 0, tzinfo=UTC)
    return [
        {"at": now - dt.timedelta(hours=h), "eur": 0.003 + h / 10000}
        for h in sorted(hours_ago, reverse=True)
    ]


def test_the_spot_closes_the_series():
    points = _points(2, 1)
    at = dt.datetime(2026, 9, 24, 15, 50, tzinfo=UTC)
    assert _append_spot(points, 0.0035, at, 1.1426) is True
    assert points[-1]["usd"] == 0.0035
    assert points[-1]["at"] == at


def test_the_spot_point_carries_its_own_usd_price():
    """The chain points are converted from EUR; this one is already USD, and
    converting it through EUR and back would only lose precision."""
    points = _points(1)
    _append_spot(points, 0.0035, dt.datetime(2026, 9, 24, 15, 50, tzinfo=UTC), 1.1426)
    assert points[-1]["usd"] == 0.0035
    assert points[-1]["eur"] == pytest.approx(0.0035 / 1.1426)


def test_a_naive_spot_timestamp_is_read_as_utc():
    """Mongo hands these back naive; reading one as local time would place it
    hours away and reorder the end of the series."""
    points = _points(1)
    assert _append_spot(points, 0.0035, dt.datetime(2026, 9, 24, 15, 50), 1.1426) is True
    assert points[-1]["at"].tzinfo is not None


def test_a_spot_older_than_the_chain_is_not_appended():
    """It would bend the last segment backwards in time."""
    points = _points(2, 1)
    stale = dt.datetime(2026, 9, 24, 12, 0, tzinfo=UTC)
    assert _append_spot(points, 0.0035, stale, 1.1426) is False
    assert len(points) == 2


def test_a_missing_spot_leaves_the_series_alone():
    points = _points(1)
    assert _append_spot(points, None, dt.datetime.now(UTC), 1.1426) is False
    assert _append_spot(points, 0.0035, None, 1.1426) is False
    assert len(points) == 1


def test_the_spot_survives_thinning():
    """It is the point the headline is measured against; thinning it away
    would make the chart disagree with its own header."""
    points = _points(*range(1, 900))
    at = dt.datetime(2026, 9, 24, 15, 59, tzinfo=UTC)
    _append_spot(points, 0.0035, at, 1.1426)
    thinned = _thin(points, CCD_PRICE_MAX_POINTS)
    assert thinned[-1]["usd"] == 0.0035
    assert thinned[-1]["at"] == at


def test_the_spot_still_lands_on_an_empty_series():
    points = []
    assert _append_spot(points, 0.0035, dt.datetime.now(UTC), 1.1426) is True
    assert len(points) == 1
