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
import time

from pydantic import BaseModel, ConfigDict

from ccdexplorer.charts import ChartSpec, ChartState, Grouping, Window, chart_path
from ccdexplorer.charts.registry import BY_NAME as SPEC_BY_NAME
from ccdexplorer.charts.registry import spec_for_plot

#: Every chart is mainnet-only. The routes exist for other nets and answer 200
#: with an HTML error page rather than an image, which would reach Telegram as
#: a broken result, so the net is not a parameter here at all.
NET = "mainnet"

#: Charts are drawn light for the bot. The site is dark and its own images
#: match it, but these land in somebody else's chat -- most of them light --
#: where a dark chart reads as a black rectangle rather than a graph.
THEME = "light"

#: Telegram's own limit, and the catalogue is kept under it. Pinned to the
#: catalogue size once instead, this silently stopped matching when charts were
#: added -- three new price charts and the last three fell off the end of an
#: empty query, which is the only way to browse an inline bot. A number that
#: has to be revised whenever the list grows is a number that will not be.
MAX_RESULTS = 50

#: Prefix on callback_data, which Telegram caps at 64 bytes. Chart names are
#: well inside that, so the name itself is the payload.
CALLBACK_PREFIX = "c:"


class Chart(BaseModel):
    """One chart, and the words somebody might reach for to find it.

    ``keywords`` exists because the route names are ours, not the reader's.
    Nobody types "staking_percentage_staked"; they type "ratio", or "share",
    or "how much is staked". These are the words that should land, listed
    explicitly rather than guessed at by a fuzzy matcher -- a wrong fuzzy match
    is worse than no match, because the reader sends the wrong chart.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    title: str
    description: str
    keywords: tuple[str, ...] = ()
    #: Charts that are the same series at different intervals. A chart in
    #: a group is sent with buttons for its siblings; one on its own is not.
    group: str = ""
    #: What the button for this chart says.
    period: str = ""
    #: Words this chart answers ahead of everything else, beating even a chart
    #: with the word in its own name. "price" reaches several charts loosely
    #: -- realized price, fee stabilization -- and somebody typing it wants the
    #: market price, so one family has to be allowed to claim the word outright
    #: rather than win it by accident of naming.
    claims: tuple[str, ...] = ()
    #: True for the one member of a family that the picker shows and that a
    #: keyword resolves to. Meaningless outside a family; `other` has none.
    default: bool = False
    #: How often this chart's image url changes. Telegram downloads a photo url
    #: once and serves its own stored copy every later time it is handed the
    #: same url, so a url that never changes is a chart that never updates --
    #: the Kraken charts showed their first render for four days while the site
    #: was current throughout. This has to match the site's png ttl for the same
    #: chart (PLOT_IMAGE_TTL_BY_CHART): sooner and Telegram refetches an image
    #: that has not been redrawn, later and the chart lags for no reason.
    refresh_seconds: int = 3600
    #: The ChartSpec this chart is drawn from, for the charts that have been
    #: migrated. None for the twenty-five that have not: those keep their own
    #: title, keywords and interval family until Plan 2 moves them.
    spec_name: str | None = None
    #: Searchable words that are not keywords proper -- the window names a
    #: reader may still type after the per-window routes went away.
    window_words: tuple[str, ...] = ()

    @property
    def spec(self) -> ChartSpec | None:
        return SPEC_BY_NAME.get(self.spec_name) if self.spec_name else None

    def image_url(self, site_url: str, now: float | None = None) -> str:
        """Kept for the charts that carry no state. See image_url() below."""
        return image_url(self, site_url, None, now)

    def page_url(self, site_url: str) -> str:
        return page_url(self, site_url, None)


def state_for(
    chart: Chart, window: Window | None = None, grouping: Grouping | None = None
) -> ChartState | None:
    """The state a spec-backed chart should be drawn in, or None without a spec."""
    spec = chart.spec
    if spec is None:
        return None
    return ChartState.from_query(
        spec,
        {
            "window": (window or spec.default_window).value,
            "grouping": (grouping or spec.default_grouping).value,
        },
    )


def _bucket(chart: Chart, now: float | None) -> int:
    """What makes Telegram refetch.

    Telegram downloads a photo url once and serves its own stored copy every
    later time it is handed the same url, so a url that never changes is a
    chart that never updates. Deliberately coarse: a url that changed on every
    call would cost a download per message and throw away the file reuse that
    makes sending one cheap.
    """
    return int((time.time() if now is None else now) // chart.refresh_seconds)


def image_url(
    chart: Chart, site_url: str, state: ChartState | None = None, now: float | None = None
) -> str:
    """The site url for this chart's png, in a form Telegram will refetch.

    The state goes in the path, the same shape as the chart's page. What
    stays in the query is what is not an address: the theme, a rendering
    preference with a working default, and the bucket, which exists so the
    url stops matching when the chart has been redrawn.
    """
    from ccdexplorer.charts.paths import format_month

    # No theme: light is what a request carrying neither a parameter nor a
    # cookie gets, and Telegram sends neither. One parameter left, and it is
    # the one whose whole job is to stop the url matching.
    query = f"t={_bucket(chart, now)}"
    base = f"{site_url.rstrip('/')}/plots/{NET}/{chart.name}"
    if state is None or chart.spec is None:
        # Its route takes no state, so there is none to put in the path.
        return f"{base}/image.png?{query}"
    return (
        f"{base}/{state.grouping.value}"
        f"/{format_month(state.start)}/{format_month(state.end)}/image.png?{query}"
    )


def page_for(chart: Chart) -> ChartSpec | None:
    """The configurable page behind this chart, if one exists.

    Resolved through the registry rather than through spec_name, because a
    chart can have a page long before its image route takes parameters. Those
    charts get no buttons -- their /plots route would ignore them -- but the
    caption should still lead somewhere a reader can change the range.
    """
    spec = chart.spec or spec_for_plot(chart.name)
    return spec if spec is not None and spec.has_page else None


def page_url(chart: Chart, site_url: str, state: ChartState | None = None) -> str:
    """The page to open, carrying what the reader is already looking at.

    A path, like everywhere else: this is the one url a reader actually sees
    and might keep.
    """
    spec = page_for(chart)
    if spec is None:
        # No page to send them to -- an intraday chart has nothing to
        # configure -- so the image's own url stands.
        return f"{site_url.rstrip('/')}/plots/{NET}/{chart.name}"
    return f"{site_url.rstrip('/')}{chart_path(spec, state, net=NET)}"


def callback_data(chart: Chart, window: Window, grouping: Grouping) -> str:
    return f"{CALLBACK_PREFIX}{chart.name}:{window.value}:{grouping.value}"


#: What the twelve retired family buttons meant. Their urls redirect; their
#: callback payloads have to resolve too, because a button tapped in a
#: months-old message would otherwise answer "That chart is no longer
#: available" about a chart that very much is. 180d has no exact Window; 90d
#: is the nearest that does not overstate it, matching the url redirect.
_RETIRED_WINDOWS = {"30d": "30d", "90d": "90d", "180d": "90d", "365d": "1y"}


def _retired(name: str):
    """(chart, state) for a payload naming a chart that was folded into a spec."""
    base, _, suffix = name.rpartition("_")
    target = _RETIRED_WINDOWS.get(suffix)
    if target is None:
        return None
    chart = BY_NAME.get(base)
    if chart is None or chart.spec is None:
        return None
    return chart, state_for(chart, Window(target))


def parse_callback(data: str):
    """(chart, state) for a button press, or None if it is stale.

    Two payload shapes, because buttons already sitting in people's chats
    carry the old one: a bare name for a chart with no spec, and
    name:window:grouping for one that has.
    """
    if not data.startswith(CALLBACK_PREFIX):
        return None
    parts = data[len(CALLBACK_PREFIX) :].split(":")
    chart = BY_NAME.get(parts[0])
    if chart is None:
        retired = _retired(parts[0])
        if retired is None:
            return None
        return retired
    if len(parts) == 1:
        return chart, None
    if len(parts) != 3 or chart.spec is None:
        return None
    try:
        return chart, state_for(chart, Window(parts[1]), Grouping(parts[2]))
    except ValueError:
        return None


#: The charts drawn from a ChartSpec, in catalogue order. The rest keep their
#: own entries until Plan 2 migrates them.
SPEC_BACKED = ("transactions_count", "plt_tvl", "agent_registries")


CHARTS: tuple[Chart, ...] = (
    Chart(
        name="ccd_kraken_1m",
        title="CCD on Kraken, 1m",
        description="1m candles and volume from the order book",
        keywords=("price", "ccd", "usd", "value", "chart", "kraken", "minute", "candles", "ohlc"),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="1m",
        refresh_seconds=60,
    ),
    Chart(
        name="ccd_kraken_5m",
        title="CCD on Kraken, 5m",
        description="5m candles and volume from the order book",
        keywords=("price", "ccd", "usd", "value", "chart", "kraken", "candles", "ohlc"),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="5m",
        refresh_seconds=300,
    ),
    Chart(
        name="ccd_kraken_15m",
        title="CCD on Kraken, 15m",
        description="15m candles and volume from the order book",
        keywords=("price", "ccd", "usd", "value", "chart", "kraken", "candles", "ohlc"),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="15m",
        refresh_seconds=900,
    ),
    Chart(
        name="ccd_kraken_30m",
        title="CCD on Kraken, 30m",
        description="30m candles and volume from the order book",
        keywords=("price", "ccd", "usd", "value", "chart", "kraken", "candles", "ohlc"),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="30m",
        refresh_seconds=1800,
    ),
    Chart(
        name="ccd_kraken_1h",
        title="CCD on Kraken, 1h",
        description="Hourly candles and volume from the order book",
        keywords=(
            "price",
            "ccd",
            "usd",
            "value",
            "chart",
            "kraken",
            "candles",
            "ohlc",
            "volume",
            "exchange",
            "traded",
            "hourly",
        ),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="1h",
    ),
    Chart(
        name="ccd_kraken_4h",
        title="CCD on Kraken, 4h",
        description="Four-hour candles and volume from the order book",
        keywords=(
            "price",
            "ccd",
            "usd",
            "value",
            "chart",
            "kraken",
            "candles",
            "ohlc",
            "volume",
            "exchange",
            "traded",
        ),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="4h",
        default=True,
    ),
    Chart(
        name="ccd_kraken_1d",
        title="CCD on Kraken, daily",
        description="Daily candles and volume from the order book",
        keywords=(
            "price",
            "ccd",
            "usd",
            "value",
            "chart",
            "kraken",
            "candles",
            "ohlc",
            "volume",
            "exchange",
            "traded",
            "daily",
        ),
        group="price",
        claims=("price", "ccd", "usd", "value"),
        period="1d",
    ),
    Chart(
        name="staking_percentage_staked",
        title="Percentage staked",
        description="Share of all CCD that is staked",
        keywords=("staked", "percentage", "ratio", "share", "supply", "staking"),
        group="other",
        spec_name="staking_percentage_staked",
    ),
    Chart(
        name="staking_validator_count",
        title="Validator count",
        description="Validators over time",
        keywords=("validators", "bakers", "nodes", "count", "staking"),
        group="other",
        spec_name="staking_validator_count",
    ),
    Chart(
        name="staking_delegator_count",
        title="Delegator count",
        description="Delegators over time",
        keywords=("delegators", "delegation", "count", "staking"),
        group="other",
        spec_name="staking_delegator_count",
    ),
    Chart(
        name="staking_open_pool_count",
        title="Open pools",
        description="Pools open for delegation over time",
        keywords=("pools", "open", "delegation", "staking"),
        group="other",
        spec_name="staking_open_pool_count",
    ),
    Chart(
        name="staking_validator_staked_amounts",
        title="Validator stake",
        description="What validators have staked",
        keywords=("validators", "bakers", "stake", "amounts", "staking"),
        group="other",
    ),
    Chart(
        name="staking_restaked_rewards",
        title="Restaked rewards",
        description="Share of daily rewards restaked",
        keywords=("restake", "compounding", "rewards", "staking"),
        group="other",
        spec_name="staking_restaked_rewards",
    ),
    Chart(
        name="staking_distribution_of_rewards",
        title="Reward distribution",
        description="Daily breakdown of rewards",
        keywords=("rewards", "distribution", "payday", "staking", "earnings"),
        group="other",
        spec_name="staking_distribution_of_rewards",
    ),
    Chart(
        name="staking_avg_delegator_stake",
        title="Average delegator stake",
        description="Average stake per delegator",
        keywords=("average", "delegator", "stake", "mean", "staking"),
        group="other",
        spec_name="staking_avg_delegator_stake",
    ),
    Chart(
        name="staking_avg_delegator_per_pool_count",
        title="Delegators per pool",
        description="Average number of delegators in a pool",
        keywords=("average", "delegators", "pool", "mean", "staking"),
        group="other",
        spec_name="staking_avg_delegator_per_pool_count",
    ),
    Chart(
        name="accounts_growth",
        title="Accounts growth",
        description="New accounts created over time",
        # "per day" stays a keyword although the chart no longer is: the
        # grouping is a button now, but it is still what people type.
        keywords=("accounts", "new", "growth", "signups", "users", "adoption", "per", "day"),
        group="other",
        spec_name="accounts_growth",
    ),
    Chart(
        name="network_activity",
        title="Network activity",
        description="CCD transferred, and transactions per second",
        keywords=("tps", "throughput", "activity", "volume", "speed", "usage"),
        group="other",
        spec_name="network_activity",
    ),
    Chart(
        name="transaction_fees",
        title="Transaction fees",
        description="Fees paid on the chain over time",
        keywords=("fees", "revenue", "paid", "cost", "transactions"),
        group="other",
        spec_name="transaction_fees",
    ),
    Chart(
        name="fee_stabilization",
        title="Fee stabilization",
        description="Cost of a regular transfer over time",
        keywords=("fees", "cost", "transfer", "stable", "cheap"),
        group="other",
        spec_name="fee_stabilization",
    ),
    Chart(
        name="daily_limits",
        title="Daily limits",
        description="CCD needed to reach the top 100 and top 250",
        keywords=("rich", "top", "whales", "leaderboard", "ranking", "holders"),
        group="other",
        spec_name="daily_limits",
    ),
    Chart(
        name="realized_prices",
        title="Realized price",
        description="Average price at which coins last moved",
        keywords=("realized", "valuation", "cost", "basis", "market"),
        group="other",
        spec_name="realized_prices",
    ),
    Chart(
        name="ccd_on_exchanges",
        title="CCD on exchanges",
        description="Balance held on exchange wallets",
        keywords=("exchanges", "cex", "listed", "custody", "binance"),
        group="other",
        spec_name="ccd_on_exchanges",
    ),
    Chart(
        name="exchange_wallets",
        title="Exchange wallets",
        description="Count of exchange wallets over time",
        keywords=("exchanges", "wallets", "aliases", "custody"),
        group="other",
        spec_name="exchange_wallets",
    ),
    # The three families that have specs. One entry each instead of four:
    # the windows are a button row now, not four route names.
    Chart(
        name="transactions_count",
        title="Transactions",
        description="Transactions by category",
        keywords=("txs", "tx", "transactions", "count", "volume", "activity"),
        # "txs", not "chain". It is the only chart in its group, so the
        # group name is the button name, and a button called "chain" said
        # nothing about the transactions behind it -- people looked for
        # them under "other", which holds the seventeen that are not this.
        group="txs",
        claims=("txs",),
        # The window used to be part of the chart name, so "/c 90d" found
        # one. It is a button now; the words stay searchable.
        window_words=("30d", "90d", "180d", "365d", "1y"),
        spec_name="transactions_count",
    ),
    Chart(
        name="plt_tvl",
        title="PLT stablecoin TVL",
        description="Total value locked in PLT stablecoins, in USD",
        keywords=("tvl", "locked", "stablecoin", "stablecoins", "plt", "supply"),
        group="plt",
        claims=("tvl",),
        # The window used to be part of the chart name, so "/c 90d" found
        # one. It is a button now; the words stay searchable.
        window_words=("30d", "90d", "180d", "365d", "1y"),
        spec_name="plt_tvl",
    ),
    Chart(
        name="agent_registries",
        title="Agent registries",
        description="Agents registered on CIS-8004 contracts",
        keywords=("agents", "agent", "registry", "registries", "cis8004", "registered"),
        group="agents",
        claims=("agents", "registries"),
        # The window used to be part of the chart name, so "/c 90d" found
        # one. It is a button now; the words stay searchable.
        window_words=("30d", "90d", "180d", "365d", "1y"),
        spec_name="agent_registries",
    ),
)

BY_NAME = {chart.name: chart for chart in CHARTS}

#: Ties are broken by the order above rather than alphabetically, so the
#: order is an editorial decision. Bare "price" should offer the day before
#: the year, and alphabetically it did the opposite.
_ORDER = {chart.name: index for index, chart in enumerate(CHARTS)}


def siblings(chart: Chart) -> list[Chart]:
    """The same series at its other intervals, in catalogue order.

    Keyed on `period`, not on `group`. A group is a category, and `other` holds
    eighteen unrelated charts; if this returned those, keyboard_for and
    photo_result would each attach a keyboard of seventeen buttons to charts
    that have nothing to do with one another.

    Empty for a chart that stands alone, which is how the caller knows not
    to draw an interval keyboard under it.
    """
    if not chart.group or not chart.period:
        return []
    return [c for c in CHARTS if c.group == chart.group and c.period]


def resolve_families(charts: list[Chart]) -> list[Chart]:
    """Collapse each family in `charts` to the one chart worth sending.

    A query matching several members of one family -- "price" matches all seven
    Kraken intervals -- must answer with that family once. Resolving after a
    slice sent the same default repeatedly; dropping non-defaults instead made
    "1d" and "90d" match nothing at all, because those words only ever match a
    member that is not the default.

    Order is the caller's, which for a search is rank and for the picker is
    catalogue order.
    """
    seen: dict[str, Chart] = {}
    for chart in charts:
        resolved = family_default(chart.group) or chart
        seen.setdefault(resolved.name, resolved)
    return list(seen.values())


def family_default(group: str) -> Chart | None:
    """The chart a group opens at, or None for a group that is not a family."""
    return next((c for c in CHARTS if c.group == group and c.default), None)


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

    # A claimed word answers with its claimant and nothing else. Ranking alone
    # only put the claimant first, so "price" still sent realized prices and fee
    # stabilization after it -- three charts for a word that means one thing.
    # `claims` exists to let a family take a word outright rather than win it by
    # accident of naming, and that is a claim on the answer, not just the order.
    # Filtered after ranking, never instead of it, so the family still leads
    # with the interval it opens at.
    claimed = [
        row for row in matched if row[2].claims and all(term in row[2].claims for term in terms)
    ]
    if claimed:
        matched = claimed

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
        haystack = " ".join(
            (chart.name, chart.title, chart.description, *chart.keywords, *chart.window_words)
        ).lower()
        hits = sum(term in haystack for term in terms)
        if hits == 0 or (require_all and hits < len(terms)):
            continue
        joined = " ".join(terms)
        if chart.claims and all(term in chart.claims for term in terms):
            # Which member a claimed word leads with. Catalogue order runs
            # shortest first, because that is the order the buttons must read
            # in, and the shortest is the worst thing to lead with -- so the
            # family says which one it opens at rather than inheriting it.
            rank = 0 if chart.default else 1
        elif name == joined.replace(" ", "_"):
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
        scored.append(((len(terms) - hits, rank), _ORDER[chart.name], chart))

    scored.sort(key=lambda row: (row[0], row[1]))
    return scored
