"""The charts this bot can put into a chat.

Every one of these is already rendered by the site at
``/plots/mainnet/<name>/image.png`` -- a Plotly figure through kaleido, cached
for an hour. The bot renders nothing: Telegram fetches those URLs itself when
it builds an inline result, so this module is a catalogue and a search, and
that is all it should ever be.

The names are duplicated from the site rather than imported, because a base
may not import another base -- and because the wording wants to be different.
The site writes a paragraph under a chart the reader is already looking at; an
inline result gets one line to say whether this is the chart they meant.

``test_catalogue.py`` asserts this list still matches the site's routes, so a
chart added or removed there fails here rather than quietly going missing.
"""

import re
from dataclasses import dataclass

#: Every chart is mainnet-only. The routes exist for other nets and answer 200
#: with an HTML error page rather than an image, which would reach Telegram as
#: a broken result, so the net is not a parameter here at all.
NET = "mainnet"

#: Telegram allows 50 inline results; far fewer is more useful, since the
#: picker is a list someone reads in a chat.
MAX_RESULTS = 12


@dataclass(frozen=True)
class Chart:
    name: str
    title: str
    description: str

    def image_url(self, site_url: str) -> str:
        return f"{site_url.rstrip('/')}/plots/{NET}/{self.name}/image.png"

    def page_url(self, site_url: str) -> str:
        return f"{site_url.rstrip('/')}/plots/{NET}/{self.name}"


CHARTS: tuple[Chart, ...] = (
    Chart("accounts_per_day", "Accounts per day", "New accounts created each day"),
    Chart("ccd_on_exchanges", "CCD on exchanges", "Balance held on exchange wallets"),
    Chart("daily_limits", "Daily limits", "CCD needed to reach the top 100 and top 250"),
    Chart("exchange_wallets", "Exchange wallets", "Count of exchange wallets over time"),
    Chart("fee_stabilization", "Fee stabilization", "Cost of a regular transfer over time"),
    Chart("network_activity_tps", "Network activity", "CCD transferred per day, and TPS"),
    Chart("realized_prices", "Realized price", "Average price at which coins last moved"),
    Chart("transaction_fees", "Transaction fees", "Fees paid on the chain over time"),
    Chart("transaction_types", "Transaction types", "Transactions by high-level type"),
    Chart("staking_validator_count", "Validator count", "Validators over time"),
    Chart("staking_validator_staked_amounts", "Validator stake", "What validators have staked"),
    Chart("staking_delegator_count", "Delegator count", "Delegators over time"),
    Chart("staking_open_pool_count", "Open pools", "Pools open for delegation over time"),
    Chart("staking_percentage_staked", "Percentage staked", "Share of all CCD that is staked"),
    Chart("staking_restaked_rewards", "Restaked rewards", "Share of daily rewards restaked"),
    Chart("staking_distribution_of_rewards", "Reward distribution", "Daily breakdown of rewards"),
    Chart("staking_avg_delegator_stake", "Average delegator stake", "Average stake per delegator"),
    Chart(
        "staking_avg_delegator_per_pool_count",
        "Delegators per pool",
        "Average number of delegators in a pool",
    ),
)

BY_NAME = {chart.name: chart for chart in CHARTS}


def _words(text: str) -> list[str]:
    return [word for word in re.split(r"[^a-z0-9]+", text.lower()) if word]


def search(query: str, limit: int = MAX_RESULTS) -> list[Chart]:
    """Charts matching ``query``, best first.

    An empty query returns the catalogue rather than nothing: someone who types
    the bot's name and stops should be shown what there is, not an empty box.

    Every word has to match somewhere, so "validator stake" narrows rather than
    widens -- the opposite is worse here, because a list of eighteen charts
    that ignores half of what was typed reads as broken.
    """
    terms = _words(query)
    if not terms:
        return list(CHARTS)[:limit]

    scored: list[tuple[int, str, Chart]] = []
    for chart in CHARTS:
        name = chart.name.lower()
        haystack = f"{chart.name} {chart.title} {chart.description}".lower()
        if not all(term in haystack for term in terms):
            continue
        joined = " ".join(terms)
        if name == joined.replace(" ", "_"):
            rank = 0
        elif name.startswith(joined.replace(" ", "_")):
            rank = 1
        elif all(term in name for term in terms):
            rank = 2
        elif all(term in chart.title.lower() for term in terms):
            rank = 3
        else:
            rank = 4
        scored.append((rank, chart.title, chart))

    scored.sort(key=lambda row: (row[0], row[1]))
    return [chart for _, _, chart in scored[:limit]]
