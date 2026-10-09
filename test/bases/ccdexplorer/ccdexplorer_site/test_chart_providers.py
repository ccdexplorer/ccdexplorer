"""The figures for the two charts whose data is live.

Both had a hand-written route and no page, because the generated page only
knew how to offer a grouping and a date range over a collection. These test
what the provider draws; the page that offers the control is
test_generated_live_charts.
"""

import datetime as dt

from ccdexplorer.ccdexplorer_site.app.routers.charts.providers import (
    FIGURES,
    OTHERS_LABEL,
    ROWS,
    build_cooldown_schedule_figure,
    build_kraken_figure,
    releases_by_day,
    schedule_span,
    stacked_segments,
)

TODAY = dt.date(2026, 10, 9)


# --- the span the data decides -------------------------------------------


def _accounts(*entries):
    """Accounts as the api returns them: (index, end_time, amount) each."""
    rows = {}
    for index, end_time, amount in entries:
        rows.setdefault(index, []).append({"end_time": end_time, "amount": amount})
    return [
        {"account_index": index, "account_cooldowns": cooldowns}
        for index, cooldowns in rows.items()
    ]


def test_the_span_runs_from_today_to_the_farthest_release():
    """No fixed window. A length chosen in advance was wrong either way:
    seven dates clipped a release the accounts-cooldown table placed six
    days out, and a longer one would draw empty weeks past the end of the
    schedule."""
    by_day = releases_by_day(_accounts((1, "2026-10-16T09:00:00Z", 500)))

    assert schedule_span(by_day, TODAY) == [
        "2026-10-09",
        "2026-10-10",
        "2026-10-11",
        "2026-10-12",
        "2026-10-13",
        "2026-10-14",
        "2026-10-15",
        "2026-10-16",
    ]


def test_the_span_stops_at_the_last_release_rather_than_a_round_number():
    by_day = releases_by_day(_accounts((1, "2026-10-11T09:00:00Z", 500)))

    assert schedule_span(by_day, TODAY) == ["2026-10-09", "2026-10-10", "2026-10-11"]


def test_a_quiet_day_inside_the_span_is_still_drawn():
    """Absent, a quiet stretch reads as a schedule that ends at the last
    busy day."""
    by_day = releases_by_day(
        _accounts((1, "2026-10-10T09:00:00Z", 100), (1, "2026-10-13T09:00:00Z", 100))
    )
    span = schedule_span(by_day, TODAY)

    assert span == ["2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13"]
    assert by_day.get("2026-10-11", {}) == {}, "nothing releases that day"


def test_today_is_the_first_day_of_the_span():
    """A cooldown expiring at 14:00 today has not expired yet."""
    by_day = releases_by_day(_accounts((1, "2026-10-09T14:00:00Z", 7)))

    assert schedule_span(by_day, TODAY) == ["2026-10-09"]


def test_a_release_already_past_does_not_extend_the_span_backwards():
    """That stake has come back. This is a chart of what has not."""
    by_day = releases_by_day(
        _accounts((1, "2026-10-01T09:00:00Z", 100), (1, "2026-10-10T09:00:00Z", 100))
    )

    assert schedule_span(by_day, TODAY) == ["2026-10-09", "2026-10-10"]


def test_nothing_ahead_is_an_empty_span():
    by_day = releases_by_day(_accounts((1, "2026-10-01T09:00:00Z", 100)))

    assert schedule_span(by_day, TODAY) == []


def test_no_cooldowns_at_all_is_an_empty_span():
    assert schedule_span(releases_by_day([]), TODAY) == []


# --- whose stake it is ----------------------------------------------------


def test_a_day_is_split_per_account():
    """The whole point of the stack: a bar is often several accounts and
    sometimes one large one, and a single total cannot say which."""
    by_day = releases_by_day(
        _accounts(
            (101031, "2026-10-10T09:00:00Z", 300),
            (202042, "2026-10-10T11:00:00Z", 200),
            (303053, "2026-10-11T09:00:00Z", 100),
        )
    )

    assert by_day["2026-10-10"] == {"101031": 300, "202042": 200}
    assert by_day["2026-10-11"] == {"303053": 100}


def test_two_cooldowns_of_one_account_on_one_day_are_one_band():
    by_day = releases_by_day(
        _accounts((1, "2026-10-10T09:00:00Z", 300), (1, "2026-10-10T18:00:00Z", 200))
    )

    assert by_day["2026-10-10"] == {"1": 500}


def test_the_bands_are_ordered_largest_first():
    """So the biggest holder is nearest the axis and the stack reads the
    same way on every day."""
    by_day = releases_by_day(
        _accounts(
            (1, "2026-10-10T09:00:00Z", 100),
            (2, "2026-10-10T09:00:00Z", 900),
            (3, "2026-10-11T09:00:00Z", 500),
        )
    )
    segments = stacked_segments(by_day, schedule_span(by_day, TODAY), limit=5)

    assert [label for label, _ in segments] == ["2", "3", "1"]


def test_a_band_carries_a_zero_for_a_day_it_does_not_release_on():
    """Plotly stacks by position, so every band needs a value per day."""
    by_day = releases_by_day(
        _accounts((1, "2026-10-10T09:00:00Z", 100), (2, "2026-10-11T09:00:00Z", 900))
    )
    span = schedule_span(by_day, TODAY)
    segments = dict(stacked_segments(by_day, span, limit=5))

    assert len(segments["1"]) == len(span)
    assert segments["1"] == [0, 100, 0]


def test_accounts_past_the_limit_are_grouped_rather_than_dropped():
    """The bands are told apart by colour, so more of them than the
    template has colours is a legend with two identical entries. Grouped,
    not dropped: the day's total has to stay the day's total."""
    by_day = (
        releases_by_day(
            *[],
            **{},
        )
        if False
        else releases_by_day(
            _accounts(*[(index, "2026-10-10T09:00:00Z", 100 * index) for index in range(1, 8)])
        )
    )
    span = schedule_span(by_day, TODAY)
    segments = stacked_segments(by_day, span, limit=3)

    assert [label for label, _ in segments][:3] == ["7", "6", "5"]
    assert segments[-1][0] == OTHERS_LABEL
    assert sum(sum(values) for _, values in segments) == sum(by_day["2026-10-10"].values())


def test_nothing_is_grouped_when_everything_fits():
    by_day = releases_by_day(_accounts((1, "2026-10-10T09:00:00Z", 100)))
    segments = stacked_segments(by_day, schedule_span(by_day, TODAY), limit=3)

    assert [label for label, _ in segments] == ["1"]


# --- what the figure says -------------------------------------------------


def _figure(accounts, average=None, locked=None):
    by_day = releases_by_day(accounts)
    total = sum(sum(day.values()) for day in by_day.values())
    return build_cooldown_schedule_figure(
        by_day,
        schedule_span(by_day, TODAY),
        "light",
        average=average,
        locked_total=total if locked is None else locked,
    )


def test_a_band_is_drawn_per_account():
    figure = _figure(
        _accounts(
            (101031, "2026-10-10T09:00:00Z", 300_000_000),
            (202042, "2026-10-10T11:00:00Z", 200_000_000),
        )
    )

    assert len(figure.data) == 2
    assert figure.layout.barmode == "stack"


def test_a_band_is_named_for_its_account():
    figure = _figure(_accounts((101031, "2026-10-10T09:00:00Z", 300_000_000)))

    assert figure.data[0].name == "#101031"


def test_the_hover_says_which_account():
    """A band's colour alone does not identify it on a png."""
    figure = _figure(_accounts((101031, "2026-10-10T09:00:00Z", 300_000_000)))

    assert "101031" in figure.data[0].hovertemplate


def test_no_legend_however_many_bands_there_are():
    """A list of account indexes is not something a reader recognises, and
    on a day of seven bands it took a third of the chart to say so. The
    hover carries the account instead, where it is asked for."""
    figure = _figure(
        _accounts(
            (1, "2026-10-10T09:00:00Z", 300_000_000), (2, "2026-10-10T09:00:00Z", 200_000_000)
        )
    )

    assert figure.layout.showlegend is False


def test_the_bands_are_drawn_in_ccd_not_microccd():
    figure = _figure(_accounts((1, "2026-10-10T09:00:00Z", 2_000_000)))

    assert max(figure.data[0].y) == 2


def test_the_subtitle_is_the_total_locked():
    """Just the amount locked. The window is the whole schedule now, so
    there is no second figure to set against it."""
    figure = _figure(_accounts((1, "2026-10-10T09:00:00Z", 1_481_469_000_000)))

    assert "1,481,469 CCD locked" in figure.layout.title.text
    assert "releasing" not in figure.layout.title.text


def test_stake_dated_before_today_still_counts_as_locked():
    """The node has not released it, so it is still in cooldown -- it just
    has no bar ahead of today to sit on."""
    figure = _figure(
        _accounts((1, "2026-10-01T09:00:00Z", 1_000_000), (2, "2026-10-10T09:00:00Z", 1_000_000))
    )

    assert "2 CCD locked" in figure.layout.title.text


def test_nothing_in_cooldown_says_so():
    figure = _figure([])

    assert not figure.data
    assert "No stake is in cooldown" in figure.layout.annotations[0].text


def test_an_average_is_drawn_as_a_reference_line():
    figure = _figure(_accounts((1, "2026-10-10T09:00:00Z", 2_000_000)), average=1_000_000)

    lines = [shape for shape in figure.layout.shapes if shape.type == "line"]
    assert lines, "the average had no line"
    assert lines[0].y0 == 1.0, "drawn in CCD, like the bands"


def test_no_average_draws_no_reference_line():
    """A lookback too short to see a fall has no average, and a line at
    zero would claim it was measured."""
    figure = _figure(_accounts((1, "2026-10-10T09:00:00Z", 2_000_000)), average=None)

    assert not [shape for shape in figure.layout.shapes if shape.type == "line"]


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
