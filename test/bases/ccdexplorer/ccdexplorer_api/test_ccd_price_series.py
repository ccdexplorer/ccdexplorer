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

import pytest

from ccdexplorer.ccdexplorer_api.app.routers.v2.misc_v2 import (
    CCD_PRICE_INTRADAY_MAX_HOURS,
    CCD_PRICE_MAX_HOURS,
    CCD_PRICE_MAX_POINTS,
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
