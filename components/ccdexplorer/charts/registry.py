"""The charts, as data.

Plan 1 registers the six that were already configurable. The twenty-five fixed
charts in statistics.py arrive in Plan 2, one spec at a time, each deleting the
handler it replaces.

The aggregation rules are the content here. A flow accumulates during the day
and sums over a week; a level is a snapshot and takes the week's last day.
Getting that backwards draws a chart that looks completely normal.
"""

import datetime as dt

from .models import Agg, ChartSpec, Grouping, Kind, Series

CHAIN_START = dt.date(2021, 6, 9)

#: The first day each exchange source records anything. Before these the
#: rows exist and every value in them is zero -- a third of a year of chart
#: saying nothing, which is where the slider's left stop used to be.
#:
#: Two dates because they are two collections, read off each: the wallet
#: count appears two days before the balances do.
EXCHANGE_WALLETS_START = dt.date(2022, 1, 28)
CCD_ON_EXCHANGES_START = dt.date(2022, 1, 30)

#: Lifted from TX_CATEGORIES in sc_transactions_count.py so the colours and the
#: rollups stay the ones already published.
_TX_CATEGORIES = (
    (
        "account",
        "Account",
        "#EE9B54",
        ("account_creation", "credential_keys_updated", "credentials_updated"),
    ),
    (
        "transfer",
        "Transfer",
        "#F7D30A",
        (
            "account_transfer",
            "transferred_to_encrypted",
            "transferred_to_public",
            "encrypted_amount_transferred",
            "transferred_with_schedule",
        ),
    ),
    (
        "smart ctr",
        "Smart Contracts",
        "#6E97F7",
        ("contract_initialized", "contract_update_issued", "module_deployed"),
    ),
    (
        "staking",
        "Staking",
        "#F36F85",
        (
            "baker_configured",
            "baker_added",
            "baker_removed",
            "baker_keys_updated",
            "baker_restake_earnings_updated",
            "baker_stake_updated",
            "delegation_configured",
        ),
    ),
    ("register data", "Data", "#AE7CF7", ("data_registered",)),
)

#: Two of the category keys have spaces in them, which a url cannot carry,
#: and the derived forms ("smartctr", "registerdata") read badly. These are
#: what the path says.
_TX_URL_NAMES = {"smart ctr": "contracts", "register data": "data"}

TRANSACTIONS_COUNT = ChartSpec(
    name="transactions_count",
    slug="transactions-count",
    title="Transactions",
    description="Account transactions on the chain, by high-level category.",
    blurb="Transactions by category",
    keywords=("txs", "tx", "transactions", "count", "volume", "activity"),
    claims=("txs",),
    category="chain",
    source="statistics_mongo_transactions",
    series=tuple(
        Series(
            key=key,
            label=label,
            colour=colour,
            agg=Agg.SUM,
            source_fields=fields,
            url_name=_TX_URL_NAMES.get(key, ""),
        )
        for key, label, colour, fields in _TX_CATEGORIES
    ),
    kind=Kind.STACKED_BAR,
    docs_path="charts/transactions_count/",
    chain_start=CHAIN_START,
    has_page=True,
    has_image=True,
    aliases=("transaction_types",),
)

ACCOUNTS_GROWTH = ChartSpec(
    name="accounts_growth",
    slug="accounts-growth",
    title="Accounts growth",
    description="New accounts created, derived from the cumulative account count.",
    blurb="New accounts created each period",
    keywords=("accounts", "new", "growth", "signups", "users", "adoption"),
    category="accounts",
    source="statistics_network_summary",
    series=(
        # The same mongo field twice under two keys: the chart draws the
        # running total and the day's growth, and the pipeline cannot give
        # one output column two aggregations.
        Series(
            key="account_level",
            url_name="total",
            label="Accounts On Chain",
            colour="#8A8F98",
            agg=Agg.LAST,
            source_fields=("account_count",),
        ),
        Series(
            key="account_count",
            url_name="accounts",
            label="Account Growth",
            colour="#549FF2",
            agg=Agg.DELTA_OF_LAST,
        ),
    ),
    chain_start=CHAIN_START,
    has_page=True,
    # Generated image route; see routers/charts/generated.py.
    has_image=True,
    aliases=("accounts_per_day",),
    derived="accounts_level_and_growth",
    derived_series=(
        Series(
            key="level",
            url_name="total",
            label="Accounts On Chain",
            colour="#8A8F98",
            agg=Agg.LAST,
            kind=Kind.LINE,
            secondary_y=True,
        ),
        Series(
            key="growth",
            url_name="growth",
            label="Account Growth",
            colour="#549FF2",
            agg=Agg.DELTA_OF_LAST,
        ),
    ),
    docs_path="charts/accounts_growth/",
)

ACTIVE_ADDRESSES = ChartSpec(
    name="active_addresses",
    slug="active-addresses",
    title="Active addresses",
    description="Distinct addresses that sent or received in the period.",
    blurb="Distinct active addresses",
    keywords=("active", "addresses", "unique", "users", "wallets"),
    category="accounts",
    source="statistics_unique_addresses_v2_daily",
    source_by_grouping={
        Grouping.DAILY: "statistics_unique_addresses_v2_daily",
        Grouping.WEEKLY: "statistics_unique_addresses_v2_weekly",
        Grouping.MONTHLY: "statistics_unique_addresses_v2_monthly",
    },
    # The documents nest these under unique_impacted_address_count, and a
    # $group output key may not contain a dot -- so the path is the source and
    # the key is the name the chart draws under. LAST, not SUM: the source is
    # already grouped by period, so a bucket holds exactly one document.
    series=(
        Series(
            key="address",
            label="Native",
            colour="#549FF2",
            agg=Agg.LAST,
            source_fields=("unique_impacted_address_count.address",),
            # A per-period count, not a level: a week the nightly job
            # missed is zero, never a repeat of the week before.
            fills_with_zero=True,
        ),
        Series(
            key="contract",
            label="Contracts",
            colour="#AE7CF7",
            agg=Agg.LAST,
            source_fields=("unique_impacted_address_count.contract",),
            # A per-period count, not a level: a week the nightly job
            # missed is zero, never a repeat of the week before.
            fills_with_zero=True,
        ),
        Series(
            key="public_key",
            url_name="publickey",
            label="CIS-5",
            colour="#70B785",
            agg=Agg.LAST,
            source_fields=("unique_impacted_address_count.public_key",),
            # A per-period count, not a level: a week the nightly job
            # missed is zero, never a repeat of the week before.
            fills_with_zero=True,
        ),
    ),
    kind=Kind.BAR,
    chain_start=CHAIN_START,
    has_page=True,
    # Generated image route; see routers/charts/generated.py.
    has_image=True,
    docs_path="charts/active_addresses/",
)

HOLDERS = ChartSpec(
    name="holders",
    slug="holders",
    title="Holders",
    description="Accounts holding at least a given amount of CCD.",
    blurb="Accounts above a balance threshold",
    keywords=("holders", "rich", "whales", "distribution", "balances"),
    category="accounts",
    source="statistics_daily_holders",
    series=(
        Series(
            key="count_>=1000000",
            url_name="above1m",
            label="> 1M CCD",
            colour="#549FF2",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    has_page=True,
    # Generated image route; see routers/charts/generated.py.
    has_image=True,
    # Off the gallery, like the Kraken intervals and the price charts: it
    # plots one threshold of a distribution the accounts category already
    # covers, and its page and image stay reachable for anyone holding a
    # link.
    listed=False,
    docs_path="charts/holders/",
)

AGENT_REGISTRIES = ChartSpec(
    name="agent_registries",
    slug="agent-registries",
    title="Agent registries",
    description="Agents registered on CIS-8004 contracts.",
    blurb="Agents registered on CIS-8004 contracts",
    keywords=("agents", "agent", "registry", "registries", "cis8004", "registered"),
    claims=("agents", "registries"),
    category="agents",
    source="statistics_agent_registry",
    series=(
        Series(
            key="agents_registered",
            url_name="agents",
            label="Agents",
            colour="#549FF2",
            agg=Agg.SUM,
        ),
    ),
    chain_start=dt.date(2026, 5, 27),
    has_page=True,
    has_image=True,
    docs_path="charts/agent_registries/",
)

#: Protocol version 7 reached mainnet on 30 October 2024. Cooldowns in this
#: form are a P7 feature: before it the node's cooldown streams are empty by
#: definition, so the chart would open on sixteen months of flat zero.
COOLDOWNS_START = dt.date(2024, 10, 30)

COOLDOWNS = ChartSpec(
    name="cooldowns",
    slug="cooldowns",
    title="Stake in cooldown",
    description="Stake locked after a validator or delegator reduced it.",
    blurb="Stake locked while it waits to be released",
    keywords=(
        "cooldown",
        "cooldowns",
        "locked",
        "unstaking",
        "unbonding",
        "released",
        "pending",
    ),
    claims=("cooldown", "cooldowns"),
    category="staking",
    source="statistics_cooldowns",
    series=(
        # The three states are one queue: stake enters pre-pre-cooldown,
        # reaches pre-cooldown at the snapshot epoch and cooldown at the
        # following payday. Stacked, because together they are what is
        # locked and none of them is spendable.
        Series(
            key="cooldown_amount",
            url_name="cooldown",
            label="Cooldown",
            colour="#549FF2",
            agg=Agg.LAST,
            scale=1_000_000,
        ),
        Series(
            key="pre_cooldown_amount",
            url_name="pre",
            label="Pre-cooldown",
            colour="#E87E90",
            agg=Agg.LAST,
            scale=1_000_000,
        ),
        Series(
            key="pre_pre_cooldown_amount",
            url_name="prepre",
            label="Pre-pre-cooldown",
            colour="#F6DB9A",
            agg=Agg.LAST,
            scale=1_000_000,
        ),
    ),
    kind=Kind.AREA,
    chain_start=COOLDOWNS_START,
    has_page=True,
    has_image=True,
)

PLT_TVL = ChartSpec(
    name="plt_tvl",
    # The page at this slug stacks a bar per token; the image this spec names
    # draws the total as one line. Same data, different chart -- but it is the
    # page a reader following the chart should land on, and /charts/plt-tvl
    # does not exist.
    slug="plt-transfers",
    title="PLT stablecoin TVL",
    description="Total value locked in PLT stablecoins, in USD.",
    blurb="Total value locked in PLT stablecoins, in USD",
    keywords=("tvl", "locked", "stablecoin", "stablecoins", "plt", "supply"),
    claims=("tvl",),
    category="plt",
    source="statistics_plt",
    series=(
        Series(
            key="total_supply_usd",
            url_name="tvl",
            label="TVL (USD)",
            colour="#549FF2",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=dt.date(2025, 9, 22),
    # sc_plt_transfers draws this one; build_grouping_pipeline refuses
    # statistics_plt, so it gets no generated page. It still has a page.
    has_page=True,
    has_image=True,
    docs_path="charts/plt_tvl/",
)


# --- staking ---------------------------------------------------------------
#
# Nothing here is a flow. Every one of these is a snapshot of the world at
# day's end -- how many validators exist, how much the average delegator has
# staked -- so a week's bucket takes its closing day, or its mean where the
# number is already an average. Summed, they would each report roughly seven
# times the chain that exists, on a chart that looks entirely ordinary.

STAKING_VALIDATOR_COUNT = ChartSpec(
    name="staking_validator_count",
    slug="staking-validator-count",
    title="Validator count",
    description="Registered active and suspended validators over time.",
    blurb="Validators over time",
    keywords=("validators", "bakers", "nodes", "count", "staking"),
    category="staking",
    source="statistics_network_summary",
    series=(
        # The published chart draws active = validator_count - suspended_count
        # and suspended as its own bar. Both halves are carried so the builder
        # can derive the difference without a second source.
        Series(
            key="validator_count",
            url_name="validators",
            label="Validators",
            colour="#70B785",
            agg=Agg.LAST,
        ),
        Series(
            key="suspended_count",
            url_name="suspended",
            label="Suspended",
            colour="#AE7CF7",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    derived="active_validators",
    derived_series=(
        Series(
            key="active",
            url_name="active",
            label="Active Validators",
            colour="#70B785",
            agg=Agg.LAST,
            kind=Kind.LINE,
        ),
        # Bars against the line: a count and a subset of it, not two
        # comparable series.
        Series(
            key="suspended",
            url_name="suspended",
            label="Suspended Validators",
            colour="#AE7CF7",
            kind=Kind.BAR,
            agg=Agg.LAST,
        ),
    ),
    docs_path="charts/staking_validator_count/",
)

STAKING_OPEN_POOL_COUNT = ChartSpec(
    name="staking_open_pool_count",
    slug="staking-open-pool-count",
    title="Open pools",
    description="Pools open for delegation over time.",
    blurb="Pools open for delegation over time",
    keywords=("pools", "open", "delegation", "staking"),
    category="staking",
    source="statistics_classified_pools",
    series=(
        Series(
            key="open_pool_count",
            url_name="openpools",
            label="Open pools",
            colour="#80B589",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/staking_open_pool_count/",
)

STAKING_DELEGATOR_COUNT = ChartSpec(
    name="staking_delegator_count",
    slug="staking-delegator-count",
    title="Delegator count",
    description="Delegators over time.",
    blurb="Delegators over time",
    keywords=("delegators", "delegation", "count", "staking"),
    category="staking",
    source="statistics_classified_pools",
    series=(
        Series(
            key="delegator_count",
            url_name="delegators",
            label="Delegators",
            colour="#EE9B54",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/staking_delegator_count/",
)

STAKING_AVG_DELEGATOR_PER_POOL = ChartSpec(
    name="staking_avg_delegator_per_pool_count",
    slug="staking-avg-delegator-per-pool-count",
    title="Delegators per pool",
    description="Average number of delegators in a pool.",
    blurb="Average number of delegators in a pool",
    keywords=("average", "delegators", "pool", "mean", "staking"),
    category="staking",
    source="statistics_classified_pools",
    series=(
        # MEAN, not LAST: the number is already an average, and a week of it
        # reads better as the week's average than as whichever value Sunday
        # happened to hold.
        Series(
            key="delegator_avg_count_per_pool",
            url_name="perpool",
            label="Delegators per pool",
            colour="#6E97F7",
            agg=Agg.MEAN,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/staking_avg_delegator_per_pool_count/",
)

STAKING_AVG_DELEGATOR_STAKE = ChartSpec(
    name="staking_avg_delegator_stake",
    slug="staking-avg-delegator-stake",
    title="Average delegator stake",
    description="Average stake per delegator.",
    blurb="Average stake per delegator",
    keywords=("average", "delegator", "stake", "mean", "staking"),
    category="staking",
    source="statistics_classified_pools",
    series=(
        Series(
            key="delegator_avg_stake",
            url_name="avgstake",
            label="Average stake",
            colour="#AE7CF7",
            agg=Agg.MEAN,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/staking_avg_delegator_stake/",
)

STAKING_PERCENTAGE_STAKED = ChartSpec(
    name="staking_percentage_staked",
    slug="staking-percentage-staked",
    title="Percentage staked",
    description="Share of all CCD that is staked.",
    blurb="Share of all CCD that is staked",
    keywords=("staked", "percentage", "ratio", "share", "supply", "staking"),
    category="staking",
    source="statistics_ccd_classified",
    series=(
        # The chart draws staked / total_supply. Both halves are levels, and
        # the ratio is taken after grouping -- a mean of daily ratios is not
        # the ratio of the week's closing values.
        Series(key="staked", label="Staked", colour="#549FF2", agg=Agg.LAST),
        Series(
            key="total_supply",
            url_name="supply",
            label="Total supply",
            colour="#8A8F98",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    derived="percentage_staked",
    derived_series=(
        Series(
            key="share",
            url_name="share",
            label="Percentage staked",
            colour="#549FF2",
            agg=Agg.LAST,
            kind=Kind.LINE,
        ),
    ),
    docs_path="charts/staking_percentage_staked/",
)


# --- chain, market and exchanges -------------------------------------------

#: The exchanges both exchange charts track. gate.io's mongo field has a dot
#: in it and a $group output key may not, so it is read through source_fields
#: under a key that can be one.
_EXCHANGES = (
    ("bitfinex", "Bitfinex", "#549FF2"),
    ("bitglobal", "BitGlobal", "#EE9B54"),
    ("mexc", "MEXC", "#70B785"),
    ("ascendex", "AscendEX", "#AE7CF7"),
    ("kucoin", "KuCoin", "#F36F85"),
    ("coinex", "CoinEx", "#F7D30A"),
    ("lcx", "LCX", "#6E97F7"),
    ("gate_io", "Gate.io", "#80B589"),
    ("bitmart", "BitMart", "#2485DF"),
    ("kraken", "Kraken", "#8A8F98"),
)


#: The colour cycles px.area and px.line used, which is what a reader
#: already recognises: seven colours over ten exchanges on the balances,
#: eight on the wallet counts.
_AREA_COLOURS = ("#DC5050", "#33C364", "#2485DF", "#7939BA", "#E87E90", "#F6DB9A", "#8BE7AA")
_LINE_COLOURS = (
    "#EE9B54",
    "#F7D30A",
    "#6E97F7",
    "#F36F85",
    "#AE7CF7",
    "#508A86",
    "#005B58",
    "#0E2625",
)


def _exchange_series(colours: tuple[str, ...]) -> tuple[Series, ...]:
    return tuple(
        Series(
            key=key,
            label=label,
            colour=colours[index % len(colours)],
            agg=Agg.LAST,
            source_fields=("gate.io",) if key == "gate_io" else (),
            literal_field=key == "gate_io",
        )
        for index, (key, label, _old) in enumerate(_EXCHANGES)
    )


TRANSACTION_FEES = ChartSpec(
    name="transaction_fees",
    slug="transaction-fees",
    title="Transaction fees",
    description="Fees paid on the chain over time.",
    blurb="Fees paid on the chain over time",
    keywords=("fees", "revenue", "paid", "cost", "transactions"),
    category="chain",
    source="statistics_transaction_fees",
    series=(
        # A flow: fees accumulate through the day, and a week is their total.
        # microCCD in mongo, CCD on the chart.
        Series(
            key="fee_for_day",
            url_name="fees",
            label="Fees (CCD)",
            colour="#549FF2",
            agg=Agg.SUM,
            scale=1_000_000,
        ),
    ),
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/transaction_fees/",
)

NETWORK_ACTIVITY = ChartSpec(
    name="network_activity",
    slug="network-activity",
    title="Network activity",
    description="CCD transferred per period.",
    blurb="CCD transferred per period",
    keywords=("tps", "throughput", "activity", "volume", "speed", "usage"),
    category="chain",
    source="statistics_network_activity",
    series=(
        Series(
            key="network_activity",
            url_name="activity",
            label="Activity",
            colour="#70B785",
            agg=Agg.SUM,
        ),
    ),
    # The published chart also draws a TPS line from the transaction count,
    # which lives in another collection; it is merged in on the date.
    extra_source="statistics_mongo_transactions",
    extra_series=(
        Series(
            key="account_transaction",
            url_name="tps",
            label="TPS",
            colour="#AE7CF7",
            agg=Agg.SUM,
        ),
    ),
    derived="activity_and_tps",
    chain_start=CHAIN_START,
    has_page=True,
    aliases=("network_activity_tps",),
    derived_series=(
        Series(
            key="activity", url_name="activity", label="Activity", colour="#70B785", agg=Agg.SUM
        ),
        # A line on its own axis: 0.6 transactions a second beside fourteen
        # million CCD is an invisible bar.
        Series(
            key="tps",
            url_name="tps",
            label="TPS",
            colour="#AE7CF7",
            agg=Agg.MEAN,
            kind=Kind.LINE,
            secondary_y=True,
        ),
    ),
    docs_path="charts/network_activity/",
    # Generated image route; see routers/charts/generated.py.
    has_image=True,
)

FEE_STABILIZATION = ChartSpec(
    name="fee_stabilization",
    slug="fee-stabilization",
    title="Fee stabilization",
    description="Cost of a regular transfer over time.",
    blurb="Cost of a regular transfer over time",
    keywords=("fees", "cost", "transfer", "stable", "cheap"),
    category="chain",
    source="statistics_microccd",
    # The one source that stores its numbers as strings. $sum over a string
    # contributes nothing and reports zero, so every one is cast. The four are
    # a ratio, not a flow: a week takes its closing value.
    series=tuple(
        Series(key=key, label=key, colour="#549FF2", agg=Agg.LAST, cast_to_double=True)
        for key in ("GTU_numerator", "GTU_denominator", "NRG_numerator", "NRG_denominator")
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    derived="transfer_cost",
    log_y=True,
    derived_series=(
        Series(
            key="cost",
            url_name="cost",
            label="Cost for regular transfer (CCD)",
            colour="#70B785",
            agg=Agg.LAST,
            kind=Kind.LINE,
        ),
    ),
    docs_path="charts/fee_stabilization/",
)

DAILY_LIMITS = ChartSpec(
    name="daily_limits",
    slug="daily-limits",
    title="Daily limits",
    description="CCD needed to reach the top 100 and top 250.",
    blurb="CCD needed to reach the top 100 and top 250",
    keywords=("rich", "top", "whales", "leaderboard", "ranking", "holders"),
    category="accounts",
    source="statistics_daily_limits",
    series=(
        Series(
            key="amount_to_make_top_100",
            url_name="top100",
            label="Top 100",
            colour="#AE7CF7",
            agg=Agg.LAST,
        ),
        Series(
            key="amount_to_make_top_250",
            url_name="top250",
            label="Top 250",
            colour="#70B785",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/fee_stabilization/",
)

REALIZED_PRICES = ChartSpec(
    name="realized_prices",
    slug="realized-prices",
    title="Realized price",
    description="Average price at which coins last moved.",
    blurb="Average price at which coins last moved",
    keywords=("realized", "valuation", "cost", "basis", "market"),
    category="accounts",
    source="statistics_realized_prices",
    series=(
        Series(
            key="realised_price",
            url_name="price",
            label="Realized price",
            colour="#549FF2",
            agg=Agg.LAST,
        ),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/realized_prices/",
)

EXCHANGE_WALLETS = ChartSpec(
    name="exchange_wallets",
    slug="exchange-wallets",
    title="Exchange wallets",
    description="Count of exchange wallets (aliases) over time.",
    blurb="Count of exchange wallets over time",
    keywords=("exchanges", "wallets", "aliases", "custody"),
    category="exchanges",
    source="statistics_exchange_wallets",
    series=_exchange_series(_LINE_COLOURS),
    kind=Kind.LINE,
    # Not the chain's start. The collection has a row for every day back to
    # genesis, but every wallet in it is empty until the first exchange
    # held any CCD -- 233 days of flat zero. chain_start is the slider's
    # left stop and what "all" resolves to, so both began in the dead
    # stretch.
    chain_start=EXCHANGE_WALLETS_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/exchange_wallets/",
)

CCD_ON_EXCHANGES = ChartSpec(
    name="ccd_on_exchanges",
    slug="ccd-on-exchanges",
    title="CCD on exchanges",
    description="Balance held on exchange wallets.",
    blurb="Balance held on exchange wallets",
    keywords=("exchanges", "cex", "listed", "custody", "binance"),
    category="exchanges",
    source="statistics_ccd_classified",
    series=_exchange_series(_AREA_COLOURS),
    kind=Kind.AREA,
    # See CCD_ON_EXCHANGES_START: 235 days of zero before the first
    # balance, and chain_start is the slider's left stop.
    chain_start=CCD_ON_EXCHANGES_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/ccd_on_exchanges/",
)


# --- rewards ---------------------------------------------------------------

STAKING_DISTRIBUTION_OF_REWARDS = ChartSpec(
    name="staking_distribution_of_rewards",
    slug="staking-distribution-of-rewards",
    title="Reward distribution",
    description="Daily breakdown of rewards between validators and delegators.",
    blurb="Daily breakdown of rewards",
    keywords=("rewards", "distribution", "payday", "staking", "earnings"),
    category="staking",
    source="statistics_daily_payday",
    # Rewards are paid out that day: a flow, and a week is their total.
    series=(
        Series(
            key="total_rewards_validators",
            url_name="validators",
            label="Validators",
            colour="#549FF2",
            agg=Agg.SUM,
        ),
        Series(
            key="total_rewards_pool_delegators",
            url_name="pooldelegators",
            label="Pool Delegators",
            colour="#EE9B54",
            agg=Agg.SUM,
        ),
        Series(
            key="total_rewards_passive_delegators",
            url_name="passivedelegators",
            label="Passive Delegators",
            colour="#AE7CF7",
            agg=Agg.SUM,
        ),
    ),
    kind=Kind.STACKED_BAR,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/staking_distribution_of_rewards/",
)

STAKING_RESTAKED_REWARDS = ChartSpec(
    name="staking_restaked_rewards",
    slug="staking-restaked-rewards",
    title="Restaked rewards",
    description="Share of daily rewards that is restaked.",
    blurb="Share of daily rewards restaked",
    keywords=("restake", "compounding", "rewards", "staking"),
    category="staking",
    source="statistics_daily_payday",
    series=(
        # Already a percentage. Summing seven gives up to 700%; taking
        # Sunday's throws the other six days away.
        Series(key="restaked_rewards_perc", label="Restaked", colour="#AE7CF7", agg=Agg.MEAN),
    ),
    kind=Kind.LINE,
    chain_start=CHAIN_START,
    # statistics.py already serves this one's png.
    has_image=True,
    has_page=True,
    docs_path="charts/staking_restaked_rewards/",
)


# --- intraday --------------------------------------------------------------
#
# The exception to everything above. These have no calendar grouping at all:
# their x-axis unit IS the candle interval, and "all charts start weekly"
# cannot apply to a one-minute chart. They carry no mongo source either --
# Kraken's order book and the chain's own CCD/EUR rate, neither in the
# statistics collection -- so they never reach the grouping pipeline, and
# BY_SOURCE skips them because their source is empty.
#
# They are registered all the same: the bot and the gallery want one list of
# every chart, and leaving ten out of it would mean two lists again.

_KRAKEN_INTERVALS = (
    ("1m", "1m", "1m candles and volume from the order book"),
    ("5m", "5m", "5m candles and volume from the order book"),
    ("15m", "15m", "15m candles and volume from the order book"),
    ("30m", "30m", "30m candles and volume from the order book"),
    ("1h", "1h", "Hourly candles and volume from the order book"),
    ("4h", "4h", "Four-hour candles and volume from the order book"),
    ("1d", "daily", "Daily candles and volume from the order book"),
)

_PRICE_WINDOWS = (
    ("24h", "24 hours", "The CCD price over the last 24 hours"),
    ("90d", "90 days", "The CCD price over the last 90 days"),
    ("1y", "a year", "The CCD price over the last year"),
)


def _intraday(
    name: str, title: str, blurb: str, kind: Kind, words, listed: bool = False
) -> ChartSpec:
    return ChartSpec(
        name=name,
        slug=name.replace("_", "-"),
        title=title,
        description=blurb + ".",
        blurb=blurb,
        keywords=words,
        claims=("price", "ccd", "usd", "value"),
        category="exchanges",
        source="",
        series=(),
        groupings=(),
        windows=(),
        kind=kind,
        has_image=True,
        listed=listed,
        # One page covers both intraday families: neither aggregates, and
        # what they have to say they have to say together.
        docs_path="charts/ccd_price/",
        chain_start=CHAIN_START,
    )


KRAKEN_CHARTS = tuple(
    _intraday(
        f"ccd_kraken_{code}",
        f"CCD on Kraken, {label}",
        blurb,
        Kind.CANDLE,
        ("price", "ccd", "usd", "value", "chart", "kraken", "candles", "ohlc", code),
        # Four hours is the one the gallery and the bot open at: short
        # enough to be current, long enough to have a shape.
        listed=code == "4h",
    )
    for code, label, blurb in _KRAKEN_INTERVALS
)

PRICE_CHARTS = tuple(
    _intraday(
        f"ccd_price_{code}",
        f"CCD price, last {label}",
        blurb,
        Kind.LINE,
        ("price", "ccd", "usd", "value", "chart", "rate", code),
    )
    for code, label, blurb in _PRICE_WINDOWS
)

ALL_SPECS: tuple[ChartSpec, ...] = (
    TRANSACTIONS_COUNT,
    ACCOUNTS_GROWTH,
    ACTIVE_ADDRESSES,
    HOLDERS,
    AGENT_REGISTRIES,
    PLT_TVL,
    STAKING_VALIDATOR_COUNT,
    STAKING_OPEN_POOL_COUNT,
    STAKING_DELEGATOR_COUNT,
    STAKING_AVG_DELEGATOR_PER_POOL,
    STAKING_AVG_DELEGATOR_STAKE,
    STAKING_PERCENTAGE_STAKED,
    COOLDOWNS,
    TRANSACTION_FEES,
    NETWORK_ACTIVITY,
    FEE_STABILIZATION,
    DAILY_LIMITS,
    REALIZED_PRICES,
    EXCHANGE_WALLETS,
    CCD_ON_EXCHANGES,
    STAKING_DISTRIBUTION_OF_REWARDS,
    STAKING_RESTAKED_REWARDS,
    *KRAKEN_CHARTS,
    *PRICE_CHARTS,
)

BY_NAME = {spec.name: spec for spec in ALL_SPECS}
BY_SLUG = {spec.slug: spec for spec in ALL_SPECS}


def _merged_by_source() -> dict[str, ChartSpec]:
    """One grouping spec per mongo collection, not per chart.

    Several charts read the same collection -- three of the staking charts
    come out of statistics_classified_pools -- and the API groups by the
    mongo `type` it was asked for, with no idea which chart wants it. So the
    rules for a source are the union of what every chart drawing from it
    needs: one pipeline that returns every field any of them will ask for.

    Safe only while they agree about a shared field, which
    test_two_charts_never_disagree_about_how_a_field_groups enforces.
    """
    merged: dict[str, ChartSpec] = {}
    for spec in ALL_SPECS:
        # Every collection this spec can read, not just its default. A chart
        # whose source is pre-grouped reads a different one per grouping --
        # active_addresses has daily, weekly and monthly variants -- and the
        # API is asked for whichever one the page resolved to. Keyed on the
        # default alone, two of its three groupings were refused outright.
        for source in sorted({spec.source, *spec.source_by_grouping.values()}):
            if not source:
                continue
            existing = merged.get(source)
            if existing is None:
                merged[source] = spec.model_copy(update={"source": source})
                continue
            seen = {s.key for s in existing.series}
            merged[source] = existing.model_copy(
                update={
                    "series": existing.series + tuple(s for s in spec.series if s.key not in seen)
                }
            )
    for spec in ALL_SPECS:
        # A second collection a chart reads alongside its own. Merged from
        # the main series only, its fields never reached the pipeline and
        # the trace that needed them stayed missing.
        if not spec.extra_source or not spec.extra_series:
            continue
        existing = merged.get(spec.extra_source)
        if existing is None:
            merged[spec.extra_source] = spec.model_copy(
                update={"source": spec.extra_source, "series": spec.extra_series}
            )
            continue
        seen = {s.key for s in existing.series}
        merged[spec.extra_source] = existing.model_copy(
            update={
                "series": existing.series + tuple(s for s in spec.extra_series if s.key not in seen)
            }
        )

    return merged


#: Keyed on the mongo collection, carrying every field any chart reads from
#: it. See _merged_by_source.
BY_SOURCE = _merged_by_source()


#: Every name a chart answers to, its own and the routes it superseded.
_BY_PLOT_NAME = {alias: spec for spec in ALL_SPECS for alias in (spec.name, *spec.aliases)}


def spec_for_plot(plot_name: str) -> ChartSpec | None:
    """The chart behind a /plots route name, or None if nothing claims it."""
    return _BY_PLOT_NAME.get(plot_name)
