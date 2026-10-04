"""The six charts that are already configurable.

Every series needs the right aggregation rule, and "right" is not something a
test can derive -- it is a fact about what the field means. So they are
asserted literally here, and getting one wrong is a visible diff rather than a
chart that is quietly seven times too large.
"""

from ccdexplorer.charts import Agg, Grouping, Kind, Window
from ccdexplorer.charts.registry import ALL_SPECS, BY_NAME, BY_SLUG, BY_SOURCE


def test_the_originally_configurable_charts_are_registered():
    assert {
        "transactions_count",
        "accounts_growth",
        "active_addresses",
        "holders",
        "agent_registries",
        "plt_tvl",
    } <= set(BY_NAME)


def test_every_spec_opens_weekly_over_a_year():
    for spec in ALL_SPECS:
        assert spec.default_grouping is Grouping.WEEKLY, spec.name
        assert spec.default_window is Window.Y1, spec.name


def test_transaction_counts_are_flows():
    for series in BY_NAME["transactions_count"].series:
        assert series.agg is Agg.SUM, series.key


def test_holder_counts_are_levels():
    """A count of accounts holding >1M CCD is a snapshot, not a daily tally."""
    for series in BY_NAME["holders"].series:
        assert series.agg is Agg.LAST, series.key


def test_account_growth_is_a_delta_of_a_cumulative_level():
    series = {s.key: s for s in BY_NAME["accounts_growth"].series}
    assert series["account_count"].agg is Agg.DELTA_OF_LAST


def test_tvl_is_a_level():
    for series in BY_NAME["plt_tvl"].series:
        assert series.agg is Agg.LAST, series.key


def test_agent_registrations_are_flows():
    for series in BY_NAME["agent_registries"].series:
        assert series.agg is Agg.SUM, series.key


def test_active_addresses_reads_a_pre_grouped_source():
    """The nightly job already writes daily, weekly and monthly variants."""
    spec = BY_NAME["active_addresses"]
    assert spec.source_for(Grouping.DAILY) == "statistics_unique_addresses_v2_daily"
    assert spec.source_for(Grouping.WEEKLY) == "statistics_unique_addresses_v2_weekly"
    assert spec.source_for(Grouping.MONTHLY) == "statistics_unique_addresses_v2_monthly"


def test_slugs_and_names_are_unique_and_url_safe():
    assert len(BY_SLUG) == len(ALL_SPECS)
    assert len(BY_NAME) == len(ALL_SPECS)
    for spec in ALL_SPECS:
        assert "_" not in spec.slug, spec.slug
        assert "-" not in spec.name, spec.name


def test_by_source_merges_every_chart_reading_that_source():
    """Several charts read one collection -- three of the staking charts come
    out of statistics_classified_pools. The API groups by the mongo `type`,
    not by chart, so the rules for a source are the union of what every chart
    drawing from it needs.
    """
    for spec in ALL_SPECS:
        if not spec.source:
            continue
        merged = BY_SOURCE[spec.source]
        assert merged.source == spec.source
        keys = {s.key for s in merged.series}
        assert {s.key for s in spec.series} <= keys, spec.name


def test_two_charts_never_disagree_about_how_a_field_groups():
    """The merge is only safe while they agree. One chart summing a field
    another takes the last of would make the grouping depend on which spec
    happened to be registered first."""
    rules: dict[tuple[str, str], object] = {}
    for spec in ALL_SPECS:
        for series in spec.series:
            key = (spec.source, series.key)
            if key in rules:
                assert rules[key] == (series.agg, series.empty_bucket_is_zero), (
                    f"{spec.name} disagrees about {series.key} in {spec.source}"
                )
            rules[key] = (series.agg, series.empty_bucket_is_zero)


def test_a_source_read_by_several_charts_carries_all_their_fields():
    pools = BY_SOURCE["statistics_classified_pools"]
    assert {s.key for s in pools.series} == {
        "open_pool_count",
        "delegator_count",
        "delegator_avg_count_per_pool",
        "delegator_avg_stake",
    }


def test_active_addresses_reads_the_nested_fields_it_actually_has():
    """The documents hold unique_impacted_address_count.{address,contract,
    public_key}, not a flat `count`. A series naming a field that is not there
    draws an empty chart rather than failing, so the paths are asserted.
    """
    spec = BY_NAME["active_addresses"]
    paths = {s.source_fields[0] for s in spec.series}
    assert paths == {
        "unique_impacted_address_count.address",
        "unique_impacted_address_count.contract",
        "unique_impacted_address_count.public_key",
    }


def test_active_addresses_output_keys_carry_no_dots():
    """A $group output key containing '.' is rejected by Mongo."""
    for series in BY_NAME["active_addresses"].series:
        assert "." not in series.key, series.key


def test_active_addresses_are_levels_not_sums():
    """The source is already grouped by period, so a bucket holds one
    document; summing would be right only by accident."""
    for series in BY_NAME["active_addresses"].series:
        assert series.agg is Agg.LAST, series.key


# --- staking (migrated from statistics.py) ---------------------------------


STAKING = (
    "staking_validator_count",
    "staking_open_pool_count",
    "staking_delegator_count",
    "staking_avg_delegator_per_pool_count",
    "staking_avg_delegator_stake",
    "staking_percentage_staked",
)


def test_the_staking_charts_are_registered():
    for name in STAKING:
        assert name in BY_NAME, name
        assert BY_NAME[name].category == "staking"


def test_validator_and_pool_counts_are_levels():
    """A count of validators is a snapshot at day's end. Summing a week of
    them would report seven times the validators that exist."""
    for name in (
        "staking_validator_count",
        "staking_open_pool_count",
        "staking_delegator_count",
    ):
        for series in BY_NAME[name].series:
            assert series.agg is Agg.LAST, f"{name}.{series.key}"


def test_the_averages_are_averaged_not_summed_or_snapshotted():
    """An average stake over a week is the week's mean, not Sunday's value --
    and certainly not seven days added together."""
    for name in ("staking_avg_delegator_per_pool_count", "staking_avg_delegator_stake"):
        for series in BY_NAME[name].series:
            assert series.agg is Agg.MEAN, f"{name}.{series.key}"


def test_percentage_staked_is_a_level():
    for series in BY_NAME["staking_percentage_staked"].series:
        assert series.agg is Agg.LAST, series.key


def test_no_staking_series_is_summed():
    """Nothing in staking is a flow. If a future series here is SUM, it is
    almost certainly a mistake -- say so loudly rather than drawing it."""
    for name in STAKING:
        for series in BY_NAME[name].series:
            assert series.agg is not Agg.SUM, f"{name}.{series.key} sums a level"


def test_the_staking_fields_are_the_ones_the_old_handlers_read():
    assert {s.key for s in BY_NAME["staking_validator_count"].series} == {
        "validator_count",
        "suspended_count",
    }
    assert {s.key for s in BY_NAME["staking_open_pool_count"].series} == {"open_pool_count"}
    assert {s.key for s in BY_NAME["staking_delegator_count"].series} == {"delegator_count"}
    assert {s.key for s in BY_NAME["staking_avg_delegator_stake"].series} == {"delegator_avg_stake"}
    assert {s.key for s in BY_NAME["staking_percentage_staked"].series} == {
        "staked",
        "total_supply",
    }


def test_a_chart_that_can_be_grouped_has_a_page():
    """Every spec with a mongo source now gets a generated page; the flag is
    what the gallery links on and what the route guard checks, so it has to
    track the routes rather than being assumed."""
    assert BY_NAME["transactions_count"].has_page is True
    assert BY_NAME["staking_validator_count"].has_page is True


def test_an_intraday_chart_has_no_page_to_configure():
    """No series, no source, nothing to group -- so nothing a settings panel
    could change."""
    assert BY_NAME["ccd_kraken_1h"].has_page is False
    assert BY_NAME["ccd_price_24h"].has_page is False


def test_every_spec_says_it_has_an_image_route():
    """The flag started out as a per-chart claim because only a handful of
    charts had a /plots route. They all have one now -- the generated routes
    cover every spec -- so a spec saying otherwise is a spec the gallery and
    the bot would both skip for no reason.

    That the flag is true of the routes, not only of itself, is checked in
    test_generated_image_routes.py against the registered paths.
    """
    missing = [spec.name for spec in ALL_SPECS if not spec.has_image]
    assert missing == []


def test_every_staking_spec_has_both_a_page_and_an_image():
    """statistics.py serves their pngs and the generated routes give them a
    configurable page."""
    for name in STAKING:
        assert BY_NAME[name].has_page is True, name
        assert BY_NAME[name].has_image is True, name


# --- chain, market and exchanges (migrated from statistics.py) -------------


EXCHANGES = (
    "bitfinex",
    "bitglobal",
    "mexc",
    "ascendex",
    "kucoin",
    "coinex",
    "lcx",
    "gate_io",
    "bitmart",
    "kraken",
)


def test_the_chain_and_market_charts_are_registered():
    for name in (
        "transaction_fees",
        "fee_stabilization",
        "network_activity",
        "daily_limits",
        "realized_prices",
        "exchange_wallets",
        "ccd_on_exchanges",
    ):
        assert name in BY_NAME, name


def test_fees_and_activity_are_flows():
    """Fees paid and CCD moved accumulate through the day; a week is their
    total."""
    for series in BY_NAME["transaction_fees"].series:
        assert series.agg is Agg.SUM, series.key
    for series in BY_NAME["network_activity"].series:
        assert series.agg is Agg.SUM, series.key


def test_limits_prices_and_balances_are_levels():
    """What it takes to reach the top 100, what coins last moved at, and what
    sits on an exchange are all snapshots. Summed, each would be sevenfold."""
    for name in ("daily_limits", "realized_prices", "exchange_wallets", "ccd_on_exchanges"):
        for series in BY_NAME[name].series:
            assert series.agg is Agg.LAST, f"{name}.{series.key}"


def test_fee_stabilization_casts_its_string_fields():
    """statistics_microccd is the one source that stores numbers as strings;
    $sum over a string silently contributes nothing."""
    spec = BY_NAME["fee_stabilization"]
    assert spec.series, "no series"
    for series in spec.series:
        assert series.cast_to_double, series.key
        assert series.agg is Agg.LAST, series.key


def test_the_exchange_charts_cover_every_exchange():
    for name in ("exchange_wallets", "ccd_on_exchanges"):
        assert {s.key for s in BY_NAME[name].series} == set(EXCHANGES), name


def test_gate_io_is_read_through_source_fields():
    """Its mongo field has a dot in it, and a $group output key may not."""
    for name in ("exchange_wallets", "ccd_on_exchanges"):
        gate = next(s for s in BY_NAME[name].series if s.key == "gate_io")
        assert gate.source_fields == ("gate.io",), name
        assert "." not in gate.key


def test_transaction_types_gets_no_spec_of_its_own():
    """It draws the same statistics_mongo_transactions categories as
    transactions_count. Two specs for one chart would be two places to get
    the aggregation rules wrong."""
    assert "transaction_types" not in BY_NAME


def test_nothing_new_shares_a_source_with_a_conflicting_rule():
    """ccd_on_exchanges and staking_percentage_staked both read
    statistics_ccd_classified."""
    merged = BY_SOURCE["statistics_ccd_classified"]
    keys = {s.key for s in merged.series}
    assert {"staked", "total_supply"} <= keys
    assert set(EXCHANGES) <= keys


# --- rewards and intraday --------------------------------------------------


INTRADAY = (
    "ccd_kraken_1m",
    "ccd_kraken_5m",
    "ccd_kraken_15m",
    "ccd_kraken_30m",
    "ccd_kraken_1h",
    "ccd_kraken_4h",
    "ccd_kraken_1d",
    "ccd_price_24h",
    "ccd_price_90d",
    "ccd_price_1y",
)


def test_reward_totals_are_flows():
    """A payday's rewards are paid that day; a week is their total."""
    for series in BY_NAME["staking_distribution_of_rewards"].series:
        assert series.agg is Agg.SUM, series.key


def test_the_restaked_share_is_averaged():
    """It is already a percentage. Summing seven of them gives up to 700%,
    and taking Sunday's throws the other six days away."""
    for series in BY_NAME["staking_restaked_rewards"].series:
        assert series.agg is Agg.MEAN, series.key


def test_every_intraday_chart_is_registered():
    for name in INTRADAY:
        assert name in BY_NAME, name


def test_an_intraday_chart_offers_no_calendar_grouping():
    """Their x-axis unit IS the interval. 'All charts start weekly' cannot
    apply to a one-minute candle chart, so there is no grouping row to draw.
    """
    for name in INTRADAY:
        spec = BY_NAME[name]
        assert spec.groupings == (), name
        assert spec.windows == (), name


def test_an_intraday_chart_has_no_mongo_source_to_group():
    """They come from Kraken's order book and the chain's own rate, not from
    the statistics collection."""
    for name in INTRADAY:
        assert BY_NAME[name].source == "", name
        assert BY_NAME[name].series == (), name


def test_the_kraken_charts_are_candles_and_the_price_charts_are_lines():
    for name in INTRADAY:
        expected = Kind.CANDLE if name.startswith("ccd_kraken") else Kind.LINE
        assert BY_NAME[name].kind is expected, name


def test_the_intraday_charts_have_images_but_no_page():
    for name in INTRADAY:
        assert BY_NAME[name].has_image is True, name
        assert BY_NAME[name].has_page is False, name


def test_an_intraday_spec_is_never_handed_to_the_grouping_pipeline():
    """A spec with no source would otherwise build a $match on type:"" and
    return nothing, quietly."""
    from ccdexplorer.charts.registry import BY_SOURCE

    for name in INTRADAY:
        assert BY_NAME[name].source not in BY_SOURCE


def test_by_source_covers_every_collection_a_spec_can_read():
    """A spec may read a different collection per grouping.

    active_addresses does: the nightly job writes daily, weekly and monthly
    variants, and source_for() picks between them. Keyed on spec.source
    alone, the weekly and monthly collections were absent from BY_SOURCE --
    so the API refused to group them and the page answered 422 for two of its
    three groupings.
    """
    from ccdexplorer.charts import Grouping as G

    for spec in ALL_SPECS:
        for grouping in spec.groupings:
            source = spec.source_for(grouping)
            if source:
                assert source in BY_SOURCE, f"{spec.name} at {grouping.value}: {source}"

    spec = BY_NAME["active_addresses"]
    for grouping in (G.DAILY, G.WEEKLY, G.MONTHLY):
        assert spec.source_for(grouping) in BY_SOURCE


def test_each_pre_grouped_variant_carries_the_same_series():
    """Whichever variant is read, the fields are the same ones."""
    spec = BY_NAME["active_addresses"]
    keys = {s.key for s in spec.series}
    for grouping in spec.groupings:
        merged = BY_SOURCE[spec.source_for(grouping)]
        assert {s.key for s in merged.series} >= keys, grouping


def test_a_spec_whose_pipeline_refuses_still_has_a_handwritten_page():
    """has_page means a page exists, not that one was generated.

    statistics_plt cannot be grouped generically, so it gets no generated
    page -- but sc_plt_transfers has always drawn it, and the gallery should
    still link there. The site decides what to generate; the flag only says
    whether a link would land somewhere.
    """
    assert BY_NAME["plt_tvl"].has_page is True


# --- legacy route names ----------------------------------------------------


def test_a_spec_claims_the_route_names_it_supersedes():
    """Three dashboard tiles name a route that became a spec under a
    different name. Without saying so, those tiles cannot find the page that
    replaced them and would stay unlinked."""
    from ccdexplorer.charts.registry import spec_for_plot

    assert spec_for_plot("accounts_per_day").name == "accounts_growth"
    assert spec_for_plot("network_activity_tps").name == "network_activity"
    assert spec_for_plot("transaction_types").name == "transactions_count"


def test_a_specs_own_name_still_resolves():
    from ccdexplorer.charts.registry import spec_for_plot

    assert spec_for_plot("transaction_fees").name == "transaction_fees"


def test_a_route_with_no_spec_resolves_to_nothing():
    """staking_validator_staked_amounts has no spec; the tile stays
    unlinked rather than pointing somewhere wrong."""
    from ccdexplorer.charts.registry import spec_for_plot

    assert spec_for_plot("staking_validator_staked_amounts") is None
    assert spec_for_plot("not_a_chart") is None


def test_no_two_specs_claim_the_same_route_name():
    """An alias resolving to two charts would resolve to whichever was
    registered first."""
    seen: dict[str, str] = {}
    for spec in ALL_SPECS:
        for alias in (spec.name, *spec.aliases):
            assert alias not in seen, f"{alias} claimed by {seen.get(alias)} and {spec.name}"
            seen[alias] = spec.name


def test_a_second_collections_fields_reach_its_grouping_spec():
    """network_activity reads account_transaction out of
    statistics_mongo_transactions. Merged from the main series only, that
    field never reached the pipeline and the TPS line stayed missing."""
    merged = BY_SOURCE["statistics_mongo_transactions"]
    assert "account_transaction" in {s.key for s in merged.series}


def test_every_extra_source_is_itself_in_by_source():
    for spec in ALL_SPECS:
        if spec.extra_source:
            assert spec.extra_source in BY_SOURCE, spec.name
