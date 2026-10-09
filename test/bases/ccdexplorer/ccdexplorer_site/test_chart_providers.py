"""The figures for the two charts whose data is live.

Both had a hand-written route and no page, because the generated page only
knew how to offer a grouping and a date range over a collection. These test
what the provider draws; the page that offers the control is
test_generated_live_charts.
"""

import datetime as dt

from ccdexplorer.ccdexplorer_site.app.routers.charts.providers import (
    FIGURES,
    ROWS,
    build_cooldown_schedule_figure,
    build_kraken_figure,
    cooldown_schedule_days,
)

TODAY = dt.date(2026, 10, 9)


# --- the seven-day horizon -------------------------------------------------


def test_the_horizon_is_always_seven_days():
    """Days with no release were absent, which read as a schedule that ends
    early rather than as a quiet week."""
    per_day = cooldown_schedule_days({"2026-10-11": 500}, TODAY, 7)

    assert list(per_day) == [
        "2026-10-09",
        "2026-10-10",
        "2026-10-11",
        "2026-10-12",
        "2026-10-13",
        "2026-10-14",
        "2026-10-15",
    ]
    assert per_day["2026-10-11"] == 500
    assert per_day["2026-10-09"] == 0


def test_an_empty_schedule_still_draws_seven_days():
    per_day = cooldown_schedule_days({}, TODAY, 7)

    assert len(per_day) == 7
    assert set(per_day.values()) == {0}


def test_releases_beyond_the_horizon_are_not_drawn():
    """Clipped, not folded into the last bar: a bar labelled the 15th that
    held three months of releases would be the chart's largest and would
    mean nothing."""
    per_day = cooldown_schedule_days({"2026-10-10": 100, "2026-12-01": 999_999}, TODAY, 7)

    assert sum(per_day.values()) == 100


def test_a_release_earlier_today_is_still_in_the_schedule():
    """Day one is today. A cooldown expiring at 14:00 has not expired yet."""
    assert cooldown_schedule_days({"2026-10-09": 7}, TODAY, 7)["2026-10-09"] == 7


def test_a_release_yesterday_is_not():
    assert "2026-10-08" not in cooldown_schedule_days({"2026-10-08": 7}, TODAY, 7)


# --- what the figure says --------------------------------------------------


def _figure(schedule, average=None, locked=0):
    return build_cooldown_schedule_figure(
        cooldown_schedule_days(schedule, TODAY, 7), "light", average=average, locked_total=locked
    )


def test_seven_bars_are_drawn_even_for_one_release():
    figure = _figure({"2026-10-10": 2_000_000})

    assert len(figure.data) == 1
    assert len(figure.data[0].x) == 7


def test_the_subtitle_separates_the_week_from_the_whole():
    """It printed the sum of the bars as "CCD locked" while the bars were the
    whole schedule. Clipped to seven days that sum is no longer what is
    locked, and saying so would understate it."""
    figure = _figure({"2026-10-10": 2_000_000}, locked=50_000_000)

    assert "2 CCD releasing in the next 7 days" in figure.layout.title.text
    assert "50 CCD locked in all" in figure.layout.title.text


def test_an_empty_schedule_says_so_rather_than_drawing_nothing():
    """Seven zero bars and a total of zero, which is the honest answer to
    "when does stake come back" when none is locked."""
    figure = _figure({}, locked=0)

    assert "0 CCD releasing in the next 7 days" in figure.layout.title.text
    assert len(figure.data[0].x) == 7


def test_an_average_is_drawn_as_a_reference_line():
    figure = _figure({"2026-10-10": 2_000_000}, average=1_000_000)

    lines = [s for s in figure.layout.shapes if s.type == "line"]
    assert lines, "the average had no line"
    assert lines[0].y0 == 1.0, "drawn in CCD, like the bars"


def test_no_average_draws_no_reference_line():
    """A lookback too short to see a fall has no average, and a line at zero
    would claim it was measured."""
    figure = _figure({"2026-10-10": 2_000_000}, average=None)

    assert not [s for s in figure.layout.shapes if s.type == "line"]


# --- Kraken ----------------------------------------------------------------


def _candle(at, close, high=None, low=None, open_=None, volume=10.0, trades=3):
    return {
        "at": at,
        "open": close if open_ is None else open_,
        "high": close if high is None else high,
        "low": close if low is None else low,
        "close": close,
        "volume": volume,
        "trades": trades,
    }


def test_kraken_draws_candles_and_volume():
    payload = {
        "candles": [
            _candle("2026-10-09T08:00:00Z", 0.0035),
            _candle("2026-10-09T12:00:00Z", 0.0036),
        ],
        "bars": 2,
        "change_pct": 2.5,
    }

    figure = build_kraken_figure(payload, "4h", "light")

    kinds = [trace.type for trace in figure.data]
    assert "candlestick" in kinds
    assert "bar" in kinds, "the volume subplot is the point of using Kraken"


def test_kraken_names_the_interval_it_drew():
    """One page serves seven intervals, so the figure has to say which."""
    payload = {"candles": [_candle("2026-10-09T08:00:00Z", 0.0035)], "bars": 1, "change_pct": 0}

    assert "1m" in build_kraken_figure(payload, "1m", "light").layout.title.text


def test_kraken_being_down_draws_the_empty_state():
    """The api answers 503 when it has no candles. A bare figure would read
    as a flat line at zero rather than as an absence."""
    figure = build_kraken_figure({}, "4h", "light")

    assert not figure.data
    assert "No candles available from Kraken" in figure.layout.annotations[0].text


def test_a_missing_payload_is_not_a_crash():
    assert not build_kraken_figure(None, "4h", "light").data


# --- the registry both are reached through ---------------------------------


def test_both_providers_are_registered_under_the_names_the_specs_use():
    assert set(FIGURES) == {"cooldown_schedule", "kraken_ohlc"}
    assert set(ROWS) == set(FIGURES), "a chart with a figure and no rows has no csv"
