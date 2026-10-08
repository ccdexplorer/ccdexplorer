"""When locked stake comes back, as a chart rather than a table.

The accounts-cooldown page listed each release moment in a table. A reader
wanting to know whether anything large is about to unlock had to read down
a column of numbers and compare them.

The data is live -- it comes from the node through the api, not from a
stored series -- so this is one chart of the schedule as it stands, not a
history. The history is the cooldowns chart, which is a different question.
"""

import datetime as dt

import pytest

from ccdexplorer.ccdexplorer_site.app.routers.tools import (
    cooldown_schedule_by_day,
    cooldown_summary,
)


def _account(*cooldowns):
    return {"account_cooldowns": [{"end_time": t, "amount": a} for t, a in cooldowns]}


def test_amounts_are_totalled_per_release_moment():
    accounts = [
        _account(("2026-10-09T09:00:00Z", 100)),
        _account(("2026-10-09T09:00:00Z", 50)),
    ]

    summary = cooldown_summary(accounts)

    assert len(summary) == 1
    assert summary[0]["total_amount"] == 150
    assert summary[0]["count"] == 2


def test_the_summary_reads_in_release_order():
    accounts = [_account(("2026-12-01T09:00:00Z", 1), ("2026-10-09T09:00:00Z", 2))]

    summary = cooldown_summary(accounts)

    assert [row["end_time"] for row in summary] == [
        "2026-10-09T09:00:00Z",
        "2026-12-01T09:00:00Z",
    ]


def test_the_chart_groups_by_day():
    """A cooldown expires at a precise moment. A bar per moment is a row of
    hairlines at arbitrary offsets; the question is which day it comes
    back."""
    accounts = [
        _account(("2026-10-09T09:00:00Z", 100)),
        _account(("2026-10-09T17:30:00Z", 25)),
        _account(("2026-10-11T09:00:00Z", 7)),
    ]

    per_day = cooldown_schedule_by_day(cooldown_summary(accounts))

    assert per_day == {"2026-10-09": 125, "2026-10-11": 7}


def test_the_days_come_out_in_order():
    accounts = [_account(("2026-12-01T09:00:00Z", 1), ("2026-10-09T09:00:00Z", 2))]

    per_day = cooldown_schedule_by_day(cooldown_summary(accounts))

    assert list(per_day) == ["2026-10-09", "2026-12-01"]


def test_nothing_in_cooldown_is_an_empty_schedule():
    assert cooldown_summary([]) == []
    assert cooldown_schedule_by_day([]) == {}


def test_a_datetime_end_time_reads_the_same_as_a_string():
    """The api serialises it, but the grpc model carries a datetime."""
    when = dt.datetime(2026, 10, 9, 9, 0, tzinfo=dt.timezone.utc)

    per_day = cooldown_schedule_by_day(cooldown_summary([_account((when, 5))]))

    assert per_day == {"2026-10-09": 5}


# --- the reference line ----------------------------------------------------
#
# Bars alone say when stake comes back but not whether that is a lot. The
# dashed line is what the history says a day's release usually looks like.


from ccdexplorer.ccdexplorer_site.app.routers.charts.sc_cooldown_schedule import (  # noqa: E402
    average_daily_release,
)


def _history(*totals):
    return [{"date": f"2026-01-{i + 1:02d}", "total_amount": t} for i, t in enumerate(totals)]


def test_a_fall_in_the_balance_is_a_release():
    """Nothing stores what was released; what is stored is how much stood
    in cooldown each day. A day where the balance fell is a day stake came
    back, and by how much."""
    assert average_daily_release(_history(100, 60)) == 40


def test_a_rise_is_not_a_negative_release():
    """New stake entering cooldown lifts the balance. Counting that as a
    negative release would net it off and understate the average."""
    assert average_daily_release(_history(100, 140, 100)) == 20


def test_the_average_is_over_every_day_not_only_moving_ones():
    """Most days nothing is released. A mean over only the days something
    moved describes a different, rarer thing, and would sit far above the
    bars."""
    assert average_daily_release(_history(100, 100, 100, 50)) == pytest.approx(50 / 3)


def test_no_history_has_no_average():
    """Before the asset has run there is nothing to compare against, and a
    line at zero would read as a measurement."""
    assert average_daily_release([]) is None
    assert average_daily_release(_history(100)) is None


def test_a_day_missing_its_total_is_skipped_rather_than_read_as_zero():
    """An older document, or one the asset failed on, would otherwise look
    like the entire balance being released and then restored."""
    history = [
        {"date": "2026-01-01", "total_amount": 100},
        {"date": "2026-01-02"},
        {"date": "2026-01-03", "total_amount": 80},
    ]

    assert average_daily_release(history) == 20
