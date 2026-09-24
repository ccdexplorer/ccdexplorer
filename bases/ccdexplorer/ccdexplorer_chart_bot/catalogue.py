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

#: Telegram allows 50. All eighteen fit, and they should all be reachable:
#: capping this lower hid six charts from anyone who typed the bot's name and
#: looked at what came back, which is the only way to browse an inline bot.
MAX_RESULTS = 18


@dataclass(frozen=True)
class Chart:
    """One chart, and the words somebody might reach for to find it.

    ``keywords`` exists because the route names are ours, not the reader's.
    Nobody types "staking_percentage_staked"; they type "ratio", or "share",
    or "how much is staked". These are the words that should land, listed
    explicitly rather than guessed at by a fuzzy matcher -- a wrong fuzzy match
    is worse than no match, because the reader sends the wrong chart.
    """

    name: str
    title: str
    description: str
    keywords: tuple[str, ...] = ()

    def image_url(self, site_url: str) -> str:
        return f"{site_url.rstrip('/')}/plots/{NET}/{self.name}/image.png"

    def page_url(self, site_url: str) -> str:
        return f"{site_url.rstrip('/')}/plots/{NET}/{self.name}"


CHARTS: tuple[Chart, ...] = (
    Chart(
        "accounts_per_day",
        "Accounts per day",
        "New accounts created each day",
        ("accounts", "new", "growth", "signups", "users", "adoption"),
    ),
    Chart(
        "ccd_on_exchanges",
        "CCD on exchanges",
        "Balance held on exchange wallets",
        ("exchanges", "cex", "listed", "custody", "binance"),
    ),
    Chart(
        "daily_limits",
        "Daily limits",
        "CCD needed to reach the top 100 and top 250",
        ("rich", "top", "whales", "leaderboard", "ranking", "holders"),
    ),
    Chart(
        "exchange_wallets",
        "Exchange wallets",
        "Count of exchange wallets over time",
        ("exchanges", "wallets", "aliases", "custody"),
    ),
    Chart(
        "fee_stabilization",
        "Fee stabilization",
        "Cost of a regular transfer over time",
        ("fees", "cost", "transfer", "stable", "cheap", "price"),
    ),
    Chart(
        "network_activity_tps",
        "Network activity",
        "CCD transferred per day, and TPS",
        ("tps", "throughput", "activity", "volume", "speed", "usage"),
    ),
    Chart(
        "realized_prices",
        "Realized price",
        "Average price at which coins last moved",
        ("price", "realized", "valuation", "cost", "basis", "market"),
    ),
    Chart(
        "transaction_fees",
        "Transaction fees",
        "Fees paid on the chain over time",
        ("fees", "revenue", "paid", "cost", "transactions"),
    ),
    Chart(
        "transaction_types",
        "Transaction types",
        "Transactions by high-level type",
        ("types", "breakdown", "mix", "transactions", "kinds"),
    ),
    Chart(
        "staking_validator_count",
        "Validator count",
        "Validators over time",
        ("validators", "bakers", "nodes", "count", "staking"),
    ),
    Chart(
        "staking_validator_staked_amounts",
        "Validator stake",
        "What validators have staked",
        ("validators", "bakers", "stake", "amounts", "staking"),
    ),
    Chart(
        "staking_delegator_count",
        "Delegator count",
        "Delegators over time",
        ("delegators", "delegation", "count", "staking"),
    ),
    Chart(
        "staking_open_pool_count",
        "Open pools",
        "Pools open for delegation over time",
        ("pools", "open", "delegation", "staking"),
    ),
    Chart(
        "staking_percentage_staked",
        "Percentage staked",
        "Share of all CCD that is staked",
        ("staked", "percentage", "ratio", "share", "supply", "staking"),
    ),
    Chart(
        "staking_restaked_rewards",
        "Restaked rewards",
        "Share of daily rewards restaked",
        ("restake", "compounding", "rewards", "staking"),
    ),
    Chart(
        "staking_distribution_of_rewards",
        "Reward distribution",
        "Daily breakdown of rewards",
        ("rewards", "distribution", "payday", "staking", "earnings"),
    ),
    Chart(
        "staking_avg_delegator_stake",
        "Average delegator stake",
        "Average stake per delegator",
        ("average", "delegator", "stake", "mean", "staking"),
    ),
    Chart(
        "staking_avg_delegator_per_pool_count",
        "Delegators per pool",
        "Average number of delegators in a pool",
        ("average", "delegators", "pool", "mean", "staking"),
    ),
)

BY_NAME = {chart.name: chart for chart in CHARTS}


#: Dropped before matching. People type questions -- "how much is staked" --
#: and every one of those words failing to match a chart name meant the whole
#: query found nothing. None of these narrow anything down, so losing them
#: costs no precision. "ccd", "pool" and "per" are deliberately absent: they
#: appear in chart names and do discriminate.
STOPWORDS = frozenset(
    """a an and are as at be by chart charts for from give graph graphs how
    in is it many me much of on or please show shows that the to what whats
    which with""".split()
)


def _words(text: str) -> list[str]:
    return [word for word in re.split(r"[^a-z0-9]+", text.lower()) if word]


def _terms(query: str) -> list[str]:
    """Query words worth matching on."""
    return [word for word in _words(query) if word not in STOPWORDS and len(word) > 1]


def search(query: str, limit: int = MAX_RESULTS) -> list[Chart]:
    """Charts matching ``query``, best first.

    An empty query returns the catalogue rather than nothing: someone who types
    the bot's name and stops should be shown what there is, not an empty box.

    Every word has to match somewhere, so "validator stake" narrows rather than
    widens -- the opposite is worse here, because a list of eighteen charts
    that ignores half of what was typed reads as broken.
    """
    terms = _terms(query)
    if not terms:
        return list(CHARTS)[:limit]

    matched = _rank(terms, require_all=True)
    if not matched:
        # Nothing matched every word. Rather than answer "no such chart" to a
        # query that was mostly right, fall back to whatever matched most of
        # it -- a near miss the reader can see and reject beats an empty panel
        # they cannot argue with.
        matched = _rank(terms, require_all=False)
    return [chart for _, _, chart in matched[:limit]]


def _rank(terms: list[str], require_all: bool) -> list[tuple[tuple[int, int], str, Chart]]:
    scored: list[tuple[int, str, Chart]] = []
    for chart in CHARTS:
        name = chart.name.lower()
        haystack = " ".join((chart.name, chart.title, chart.description, *chart.keywords)).lower()
        hits = sum(term in haystack for term in terms)
        if hits == 0 or (require_all and hits < len(terms)):
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
        # More words matched is a better answer than a tidier rank.
        scored.append(((len(terms) - hits, rank), chart.title, chart))

    scored.sort(key=lambda row: (row[0], row[1]))
    return scored
