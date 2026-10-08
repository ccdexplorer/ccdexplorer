"""Some charts have no grouping to choose, so they are not asked.

Grouping earns its control where it changes what the number is: a week of
fees is not a day of fees, a week of growth is not a day of growth, and a
week's distinct addresses cannot be built out of seven daily counts. For a
closing value it changes nothing at all -- the same measurement, fewer
points -- so the question is about resolution, which the chart can answer
from the span better than the reader can.

It answered badly when asked. Fee stabilization over thirty days, grouped
monthly, is a two-point chart, and nothing stopped anyone asking for it.
Over the full range daily is 1943 points drawn into a 720-pixel image.

The url keeps its grouping segment either way: it records what was drawn,
so a shared link still says which resolution the picture is at.
"""

import datetime as dt

import pytest

from ccdexplorer.charts import Agg, ChartState, Grouping
from ccdexplorer.charts.registry import ALL_SPECS, BY_NAME
from ccdexplorer.charts.state import resolution_for


def _span(days):
    end = dt.date(2026, 10, 3)
    return end - dt.timedelta(days=days), end


# --- which charts stop asking ---------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "fee_stabilization",
        "holders",
        "staking_open_pool_count",
        "staking_delegator_count",
        "staking_percentage_staked",
        "realized_prices",
        "daily_limits",
        "exchange_wallets",
        "ccd_on_exchanges",
        "staking_validator_count",
    ],
)
def test_a_closing_value_picks_its_own_resolution(name):
    assert BY_NAME[name].automatic_grouping, name


@pytest.mark.parametrize(
    "name, why",
    [
        ("transaction_fees", "a week of fees is not a day of fees"),
        ("transactions_count", "summed"),
        ("agent_registries", "summed"),
        ("staking_distribution_of_rewards", "summed"),
        ("accounts_growth", "a closing total beside a difference"),
        ("active_addresses", "distinct counts do not compose"),
        ("network_activity", "a sum beside a mean"),
        ("staking_avg_delegator_stake", "a mean is a smoothing choice"),
    ],
)
def test_a_chart_whose_grouping_means_something_keeps_asking(name, why):
    assert not BY_NAME[name].automatic_grouping, f"{name}: {why}"


def test_the_rule_is_read_off_the_aggregations():
    """Not a hand-kept list, so a chart that changes its mind is covered."""
    for spec in ALL_SPECS:
        if not spec.display_series:
            continue
        all_levels = all(s.agg is Agg.LAST for s in spec.display_series)
        expected = all_levels and not spec.source_by_grouping
        assert spec.automatic_grouping is expected, spec.name


# --- what it picks --------------------------------------------------------


@pytest.mark.parametrize(
    "days, grouping",
    [
        (1, Grouping.DAILY),
        (30, Grouping.DAILY),
        (90, Grouping.DAILY),
        (182, Grouping.DAILY),
        (183, Grouping.WEEKLY),
        (365, Grouping.WEEKLY),
        (1095, Grouping.WEEKLY),
        (1096, Grouping.MONTHLY),
        (3650, Grouping.MONTHLY),
    ],
)
def test_the_resolution_follows_the_span(days, grouping):
    start, end = _span(days)
    assert resolution_for(start, end) is grouping


def test_no_span_is_collapsed_into_a_chart_of_two_points():
    """The failure the control allowed: thirty days grouped monthly.

    From a week up. A one-day range is two points however it is drawn --
    that is the data, and no choice of resolution can add to it.
    """
    for days in (7, 30, 60, 90):
        start, end = _span(days)
        buckets = {
            Grouping.DAILY: days + 1,
            Grouping.WEEKLY: days // 7 + 1,
            Grouping.MONTHLY: days // 30 + 1,
        }[resolution_for(start, end)]
        assert buckets >= 5, f"{days} days gives {buckets} points"


# --- the state uses it ----------------------------------------------------


def test_an_automatic_chart_ignores_a_grouping_it_was_handed():
    """Including one in a url somebody crafted or kept from before."""
    spec = BY_NAME["fee_stabilization"]
    start, end = _span(30)
    state = ChartState.from_query(
        spec, {"grouping": "monthly", "from": start.isoformat(), "to": end.isoformat()}
    )
    assert state.grouping is Grouping.DAILY


def test_a_chart_that_asks_is_given_what_it_was_asked_for():
    spec = BY_NAME["transaction_fees"]
    start, end = _span(30)
    state = ChartState.from_query(
        spec, {"grouping": "monthly", "from": start.isoformat(), "to": end.isoformat()}
    )
    assert state.grouping is Grouping.MONTHLY


def test_the_url_still_records_the_resolution():
    """So a shared link says which picture it is, even with no control."""
    from ccdexplorer.charts.paths import chart_path

    spec = BY_NAME["fee_stabilization"]
    start, end = _span(30)
    state = ChartState.from_query(spec, {"from": start.isoformat(), "to": end.isoformat()})
    assert "/daily/" in chart_path(spec, state, "mainnet")


# --- a chart starts when its data does ------------------------------------


def test_ccd_on_exchanges_starts_when_the_exchanges_do():
    """Same dead stretch, a different collection, and two days later:
    statistics_ccd_classified first records CCD on an exchange on the
    30th, where statistics_exchange_wallets records a wallet on the 28th.
    Each chart starts where its own source does rather than sharing a
    date that would be wrong for one of them.
    """
    assert BY_NAME["ccd_on_exchanges"].chain_start == dt.date(2022, 1, 30)


def test_exchange_wallets_starts_when_the_exchanges_do():
    """It read from chain start, where the collection has rows but every
    wallet is empty: 233 days of flat zero before the first exchange held
    any CCD, which is a third of a year of chart saying nothing.

    chain_start drives the slider's left stop and what "all" resolves to,
    so both began in the dead stretch.
    """
    spec = BY_NAME["exchange_wallets"]
    assert spec.chain_start == dt.date(2022, 1, 28)


def test_the_other_charts_still_start_at_the_chain():
    """Only the ones whose data starts later are moved."""
    assert BY_NAME["transaction_fees"].chain_start == dt.date(2021, 6, 9)


def test_all_resolves_to_the_charts_own_start():
    from ccdexplorer.charts import Window
    from ccdexplorer.charts.state import resolve_window

    spec = BY_NAME["exchange_wallets"]
    start, _end = resolve_window(spec, Window.ALL, dt.date(2026, 10, 3))
    assert start == spec.chain_start


# --- stake in cooldown -----------------------------------------------------


def test_cooldowns_starts_at_protocol_version_7():
    """Not at genesis and not at the staking start. Cooldowns in this form
    arrived with P7 on 30 October 2024; before that the node's cooldown
    streams are empty by definition, so the slider's left stop and "all"
    would both begin in sixteen months of flat zero."""
    assert BY_NAME["cooldowns"].chain_start == dt.date(2024, 10, 30)


def test_cooldowns_picks_its_own_resolution():
    """Every state is a standing balance, so grouping changes nothing
    about the number."""
    assert BY_NAME["cooldowns"].automatic_grouping


def test_cooldowns_draws_the_whole_queue():
    """Three states, stacked: stake enters pre-pre-cooldown, reaches
    pre-cooldown at the snapshot epoch and cooldown at the next payday.
    None of it is spendable, so the stack is what is locked."""
    spec = BY_NAME["cooldowns"]
    assert [s.key for s in spec.display_series] == [
        "cooldown_amount",
        "pre_cooldown_amount",
        "pre_pre_cooldown_amount",
    ]
    assert all(s.scale == 1_000_000 for s in spec.series), "amounts are stored in microCCD"


def test_both_cooldown_charts_are_in_staking():
    """One asks how much was locked on a day, the other asks which day it
    comes back. Different questions, same part of the site."""
    for name in ("cooldowns", "cooldown_schedule"):
        assert BY_NAME[name].category == "staking", name
        assert BY_NAME[name].listed, name


def test_the_schedule_has_no_series_to_generate_from():
    """It is the node's current state, not a date-keyed collection, so it
    keeps its own route in charts/sc_cooldown_schedule.py and must not be
    handed a generated page that would draw nothing."""
    from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import can_be_generated

    spec = BY_NAME["cooldown_schedule"]
    assert spec.series == ()
    assert not can_be_generated(spec)
    assert not spec.has_page
