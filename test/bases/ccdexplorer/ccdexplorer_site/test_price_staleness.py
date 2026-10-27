"""Saying so when the front page's CCD price is out of date.

The price comes from a Mongo document that dagster's recurring job refreshes
every ten minutes. When that job stops, the route keeps rendering the last
number written with nothing to mark it, and a stale price looks exactly like a
current one -- which is worse than showing nothing. During the ccd-1 outage of
2026-10-26 the page served a price eighteen hours old that way: `ko-recurring`
was up but pointed at a postgres and redis that had gone with the host, so it
reported nothing and the page said nothing.

Thirty minutes is the threshold: three missed runs, late enough to mean broken
rather than slow.
"""

import datetime as dt

import pytest

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 27, 8, 0, tzinfo=UTC)


def _utils():
    """Resolved per call, never bound at collection time.

    A full-suite run re-imports the site app package, so a reference captured
    at import time belongs to an orphaned module object.
    """
    from ccdexplorer.ccdexplorer_site.app import utils

    return utils


# --- the threshold --------------------------------------------------------


def test_a_fresh_price_is_not_stale():
    u = _utils()

    f = u.price_freshness(NOW - dt.timedelta(minutes=4), now=NOW)

    assert f.is_stale is False
    assert f.age_text == "4 minutes ago"


def test_one_missed_run_is_not_yet_stale():
    """A single late run is normal; it must not cry wolf."""
    u = _utils()

    assert u.price_freshness(NOW - dt.timedelta(minutes=12), now=NOW).is_stale is False


def test_exactly_the_threshold_is_not_stale():
    u = _utils()

    assert u.price_freshness(NOW - u.PRICE_STALE_AFTER, now=NOW).is_stale is False


def test_past_the_threshold_is_stale():
    u = _utils()

    f = u.price_freshness(NOW - dt.timedelta(minutes=31), now=NOW)

    assert f.is_stale is True


def test_the_outage_case():
    """What the page was actually serving on 2026-10-27."""
    u = _utils()

    f = u.price_freshness("2026-10-26T14:29:00.000Z", now=NOW)

    assert f.is_stale is True
    assert f.age_text == "17 hours ago"


# --- what it does with a timestamp it cannot use --------------------------


@pytest.mark.parametrize("value", [None, "", "not a date", "2026-13-45T99:99:99Z", 1761500000])
def test_an_unusable_timestamp_counts_as_stale(value):
    """The number came from somewhere. If we cannot say when, do not imply now.

    Includes the zeroed fallback the route builds when the api cannot be
    reached: it carries no timestamp at all, and a price of 0 is not current.
    """
    u = _utils()

    f = u.price_freshness(value, now=NOW)

    assert f.is_stale is True
    assert f.age_text == "unknown"
    assert f.as_of is None


def test_a_future_timestamp_is_treated_as_current():
    """Clock skew against CoinMarketCap is not a reader's problem."""
    u = _utils()

    f = u.price_freshness(NOW + dt.timedelta(minutes=5), now=NOW)

    assert f.is_stale is False
    assert f.age == dt.timedelta(0)


def test_a_naive_timestamp_is_read_as_utc():
    u = _utils()

    f = u.price_freshness(dt.datetime(2026, 10, 27, 7, 50), now=NOW)

    assert f.is_stale is False
    assert f.age_text == "10 minutes ago"


# --- the wording ----------------------------------------------------------


@pytest.mark.parametrize(
    "delta,expected",
    [
        (dt.timedelta(seconds=20), "just now"),
        (dt.timedelta(minutes=1), "1 minute ago"),
        (dt.timedelta(minutes=45), "45 minutes ago"),
        (dt.timedelta(minutes=119), "119 minutes ago"),
        (dt.timedelta(hours=2), "2 hours ago"),
        (dt.timedelta(hours=47), "47 hours ago"),
        (dt.timedelta(hours=48), "2 days ago"),
        (dt.timedelta(days=9), "9 days ago"),
    ],
)
def test_the_age_reads_in_the_unit_that_fits(delta, expected):
    u = _utils()

    assert u.price_freshness(NOW - delta, now=NOW).age_text == expected


def test_it_does_not_fall_back_to_calendar_days():
    """humanize_age calls anything under a day "today".

    That is the one answer this must never give, because an eighteen-hour-old
    price reading as "today" is indistinguishable from a current one.
    """
    u = _utils()

    # Seven hours before 08:00 is still the same calendar day, so the general
    # helper says "today" for a price that is seven hours out of date.
    seven_hours = NOW - dt.timedelta(hours=7)
    assert u.humanize_age(seven_hours, now=NOW) == "today"
    assert u.price_freshness(seven_hours, now=NOW).age_text == "7 hours ago"
    assert u.price_freshness(seven_hours, now=NOW).is_stale is True


# --- the page -------------------------------------------------------------


def _render(freshness):
    """Render the real dashboard template, not a stand-in for it."""
    from pathlib import Path

    from jinja2 import Environment, FileSystemLoader

    here = Path(__file__).resolve()
    for candidate in here.parents:
        templates = candidate / "projects" / "ccdexplorer_site" / "templates"
        if (templates / "home" / "crypto_dashboard.html").exists():
            break
    else:  # pragma: no cover - the template is in the repo
        pytest.skip("crypto_dashboard.html not found in the repo")

    env = Environment(loader=FileSystemLoader(str(templates)))
    env.filters["round_x_decimal_with_comma"] = lambda v, n: f"{v:,.{n}f}"
    return env.get_template("home/crypto_dashboard.html").render(
        theme="dark",
        ccd_price="0.00306",
        ccd_change=-1.4,
        market_cap="45.0M",
        cmc_rank=1,
        tps_h=0,
        total_txs="1",
        total_validators=1,
        total_accounts="1",
        total_tokens="1.0M",
        price_freshness=freshness,
    )


def test_a_stale_price_is_marked_on_the_page():
    u = _utils()

    html = _render(u.price_freshness("2026-10-26T14:29:00.000Z", now=NOW))

    assert "bi-exclamation-triangle-fill" in html
    assert "17 hours ago" in html
    assert "This price is out of date" in html
    # The price itself still shows; the mark is an addition, not a removal.
    assert "0.00306" in html


def test_a_fresh_price_is_not_marked():
    u = _utils()

    html = _render(u.price_freshness(NOW - dt.timedelta(minutes=3), now=NOW))

    assert "bi-exclamation-triangle-fill" not in html
    assert "0.00306" in html


def test_the_page_renders_without_the_variable_at_all():
    """Other routes render this template; a missing value must not 500."""
    html = _render(None)

    assert "bi-exclamation-triangle-fill" not in html
    assert "0.00306" in html
