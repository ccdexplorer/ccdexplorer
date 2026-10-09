"""Figures for the charts whose data is live rather than a collection.

Two of them, and they are the two that had a hand-written route and no page:
the node's current cooldown state, and one exchange's order book. Neither is
a date-keyed collection, so neither reaches the grouping pipeline, and the
generated page only knew how to offer a grouping and a date range.

A spec names one of these by `live_source`, the way it names a `derived`
transform, and the generated page renders through it. Registered here rather
than declared in the charts component because a figure is plotly and that
component is imported by the bot and the api.

Neither chart is Series-shaped: Kraken is OHLC plus volume on two subplots
and the schedule is bars plus a reference line, so build_figure's series loop
has nothing to say about either. The state they are drawn in -- interval,
lookback -- is the spec's business and arrives the same way it does
everywhere else.
"""

import datetime as dt

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ccdexplorer.charts import ChartSpec, ChartState
from ccdexplorer.charts.state import lookback_range

from ccdexplorer.ccdexplorer_site.app.routers.statistics import (
    get_all_data_for_analysis_limited,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    ccdexplorer_plotly_template,
    empty_chart_figure,
    get_url_from_api,
)

# --- the cooldown schedule -------------------------------------------------
#
# Live, not stored: it reads the node's current cooldown state through the
# api, so it is the schedule as it stands rather than a history. The history
# is the `cooldowns` chart, which answers a different question -- how much
# was locked on a given day, rather than which day it returns.

#: microCCD to CCD, the unit the axis is labelled in.
MICRO_CCD = 1_000_000

#: The band holding every account past the ones the palette can colour.
OTHERS_LABEL = "Other accounts"

#: Grey, for the things on this chart that are not one account: the
#: grouped tail and the reference line.
GROUPED_COLOUR = "#8A8F98"


def average_daily_release(history: list[dict]) -> float | None:
    """What a day's release usually looks like, in microCCD, or None.

    Nothing records what was released. What is recorded is how much stood
    in cooldown at the end of each day, so a fall in that balance is stake
    coming back and by how much.

    An approximation, and it understates: stake entering cooldown lifts the
    balance on the same day, which hides part of a release that happened
    alongside it. A rise is not counted as a negative release, or new
    entries would net the average down twice over.

    Averaged over every day in the span, not only the days something moved.
    Most days nothing is released, and a mean over the ones that did would
    describe a rarer event and sit well above the bars.
    """
    totals = [
        (row["date"], row["total_amount"]) for row in history if row.get("total_amount") is not None
    ]
    if len(totals) < 2:
        return None

    releases = [max(0, before - after) for (_, before), (_, after) in zip(totals, totals[1:])]
    return sum(releases) / len(releases)


def releases_by_day(accounts: list[dict]) -> dict[str, dict[str, int]]:
    """What releases on each day, and whose it is.

    Built from the accounts rather than from cooldown_summary, which
    aggregates by release moment and so throws the account away. A day's
    bar is often several accounts and sometimes one large one, and which
    of those it is cannot be read off a single total.

    Keyed by account index as a string, because that is what the label and
    the csv both want and the index is only ever an identifier here.
    """
    by_day: dict[str, dict[str, int]] = {}
    for row in accounts:
        account = str(row.get("account_index", "unknown"))
        for cooldown in row.get("account_cooldowns") or []:
            when = dt.datetime.fromisoformat(str(cooldown["end_time"]).replace("Z", "+00:00"))
            day = when.date().isoformat()
            per_account = by_day.setdefault(day, {})
            per_account[account] = per_account.get(account, 0) + cooldown["amount"]
    return dict(sorted(by_day.items()))


def schedule_span(by_day: dict[str, dict[str, int]], today: dt.date) -> list[str]:
    """Every date from today to the farthest release, inclusive.

    The data decides how far this reaches, not a fixed window. A fixed one
    was wrong twice over: seven dates counting today as the first clipped a
    release the accounts-cooldown table placed six days out, and any length
    chosen in advance either hides the end of the schedule or draws empty
    weeks past it. The cooldown period is bounded anyway -- the farthest
    out is a payday plus the cooldown -- so the span is short by
    construction.

    Zero-filled between, because a day on which nothing is released is a
    fact about the schedule. Absent -- which is what grouping by day gives
    -- a quiet stretch reads as a schedule that ends at the last busy day.

    Days already past are left out: a cooldown that has expired is stake
    that has come back, and this is a chart of what has not.
    """
    ahead = [day for day in by_day if day >= today.isoformat()]
    if not ahead:
        return []
    last = dt.date.fromisoformat(max(ahead))
    return [
        (today + dt.timedelta(days=offset)).isoformat() for offset in range((last - today).days + 1)
    ]


def stacked_segments(
    by_day: dict[str, dict[str, int]], span: list[str], limit: int
) -> list[tuple[str, list[int]]]:
    """One stack segment per account, largest first, the tail grouped.

    Largest first so the biggest holder is the band nearest the axis and
    the stack reads consistently across days. Grouped past `limit` because
    the segments are told apart by colour: more bands than the template has
    colours is a legend in which two entries look identical.
    """
    totals: dict[str, int] = {}
    for day in span:
        for account, amount in by_day.get(day, {}).items():
            totals[account] = totals.get(account, 0) + amount

    ranked = sorted(totals, key=lambda account: totals[account], reverse=True)
    kept, tail = ranked[:limit], ranked[limit:]

    segments = [
        (account, [by_day.get(day, {}).get(account, 0) for day in span]) for account in kept
    ]
    if tail:
        segments.append(
            (
                OTHERS_LABEL,
                [sum(by_day.get(day, {}).get(account, 0) for account in tail) for day in span],
            )
        )
    return segments


def build_cooldown_schedule_figure(
    by_day: dict[str, dict[str, int]],
    span: list[str],
    theme: str,
    average: float | None = None,
    locked_total: int = 0,
) -> go.Figure:
    """The schedule, as a stack per day showing which accounts make it up."""
    if not span:
        # Not an empty pair of axes, which reads as "nothing happens ever"
        # rather than "nothing is locked right now".
        return empty_chart_figure(theme, "No stake is in cooldown")

    template = ccdexplorer_plotly_template(theme)
    # Read off the template rather than restated here, so the number of
    # bands the chart will colour distinctly cannot drift from the number
    # of colours it has to do it with.
    colours = list(template.layout.colorway or ())
    segments = stacked_segments(by_day, span, len(colours) or 1)

    days = [dt.date.fromisoformat(day) for day in span]
    figure = go.Figure()
    for index, (label, amounts) in enumerate(segments):
        grouped = label == OTHERS_LABEL
        figure.add_trace(
            go.Bar(
                x=days,
                y=[amount / MICRO_CCD for amount in amounts],
                name=label if grouped else f"#{label}",
                # Grey for the grouped tail, because it is not an account
                # and a palette colour would claim it was one.
                marker=dict(color=GROUPED_COLOUR if grouped else colours[index % len(colours)]),
                hovertemplate=(
                    f"%{{x|%d %b %Y}}<br>{'' if grouped else 'Account '}"
                    f"{label}: %{{y:,.0f}} CCD<extra></extra>"
                ),
            )
        )

    if average:
        # Said against the bars because it is the same measurement: CCD
        # released on a day. The bars say when stake comes back; this says
        # whether that is a lot.
        figure.add_hline(
            y=average / MICRO_CCD,
            line=dict(color=GROUPED_COLOUR, width=1, dash="dash"),
            annotation_text=f"average day: {average / MICRO_CCD:,.0f} CCD",
            annotation_position="top left",
            annotation_font=dict(size=11, color=GROUPED_COLOUR),
        )

    figure.update_xaxes(type="date", title=None)
    figure.update_yaxes(title="CCD released")
    figure.update_layout(
        barmode="stack",
        title=(
            "<b>Stake leaving cooldown</b><br>"
            f"<sup>{locked_total / MICRO_CCD:,.0f} CCD locked</sup>"
        ),
        # No legend. It is a list of account indexes, which is not something
        # a reader recognises or can do anything with, and on a day of seven
        # bands it took a third of the chart to say so. Which account a band
        # is belongs in the hover, where it is asked for.
        showlegend=False,
        template=template,
        height=400,
    )
    return figure


async def _cooldown_schedule(app, net: str, state: ChartState, spec: ChartSpec):
    """The live schedule and the history its reference line comes from."""
    api_result = await get_url_from_api(
        f"{app.api_url}/v2/{net}/accounts/cooldown",
        app.httpx_client,
    )
    accounts = api_result.return_value if api_result.ok else []
    by_day = releases_by_day(accounts)

    # The lookback the reader chose, not the whole history: "what a day
    # usually releases" over five years and over the last thirty days are
    # different claims, and which one is on screen is now theirs to pick.
    start, end = lookback_range(spec, state)
    history = await get_all_data_for_analysis_limited(
        "statistics_cooldowns", app, start.isoformat(), end.isoformat()
    )
    return by_day, history


async def cooldown_schedule_figure(
    spec: ChartSpec, app, net: str, state: ChartState, theme: str
) -> go.Figure:
    by_day, history = await _cooldown_schedule(app, net, state, spec)
    return build_cooldown_schedule_figure(
        by_day,
        schedule_span(by_day, dt.date.today()),
        theme,
        average=average_daily_release(history),
        # Everything in cooldown, including anything dated before today
        # that the node has not yet released: it is still locked.
        locked_total=sum(sum(day.values()) for day in by_day.values()),
    )


async def cooldown_schedule_rows(spec: ChartSpec, app, net: str, state: ChartState) -> list[dict]:
    """One row per account per release day, with the reference line beside.

    Per account rather than per day, because that is what the chart draws
    now: a day's total is the sum of its bands and a csv of totals alone
    could not be checked against it.

    The average repeats down the column rather than being a second file:
    it is what the chart compares each day against, and the rows without it
    lose the comparison the chart is about.
    """
    by_day, history = await _cooldown_schedule(app, net, state, spec)
    average = average_daily_release(history)
    return [
        {
            "date": day,
            "account": account,
            "released": amount,
            "average_daily_release": "" if average is None else round(average),
        }
        for day in schedule_span(by_day, dt.date.today())
        for account, amount in sorted(by_day.get(day, {}).items())
    ]


# --- CCD on Kraken ---------------------------------------------------------
#
# One exchange's order book: what people actually paid, with volume, and the
# only thing on this site that depends on a third party being up. The api
# does the fetching and the falling back; this draws.
#
# No date range, because the api serves a fixed number of bars per interval.
# The interval IS the reach -- 120 one-minute candles is two hours, 120 daily
# ones is four months -- which is why the interval is what the page offers.

CANDLE_UP = "#26A69A"
CANDLE_DOWN = "#EF5350"


def build_kraken_figure(payload: dict, interval: str, theme: str) -> go.Figure:
    """Candles and volume for one interval, or the empty state without them."""
    candles = (payload or {}).get("candles") or []
    title = f"CCD/USD on Kraken, {interval}"
    if not candles:
        # Kraken is not ours. The api answers 503 when it has nothing, and a
        # bare figure would read as a flat line rather than an absence.
        return empty_chart_figure(theme, "No candles available from Kraken")

    at = [c["at"] for c in candles]
    closes = [c["close"] for c in candles]
    opens = [c["open"] for c in candles]
    change = payload.get("change_pct") or 0
    up = change >= 0

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, row_heights=[0.78, 0.22], vertical_spacing=0.04
    )
    fig.add_trace(
        go.Candlestick(
            x=at,
            open=opens,
            high=[c["high"] for c in candles],
            low=[c["low"] for c in candles],
            close=closes,
            increasing_line_color=CANDLE_UP,
            decreasing_line_color=CANDLE_DOWN,
            increasing_fillcolor=CANDLE_UP,
            decreasing_fillcolor=CANDLE_DOWN,
            line_width=1,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=at,
            y=[c["volume"] for c in candles],
            marker_color=[CANDLE_UP if c["close"] >= c["open"] else CANDLE_DOWN for c in candles],
            marker_line_width=0,
            opacity=0.45,
        ),
        row=2,
        col=1,
    )
    # The dashed last price. "x domain" and not "paper": with row/col set,
    # Plotly resolves the reference against the subplot's data axis, and x=1
    # then reads as 1970-01-01 and stretches the chart back five decades.
    fig.add_hline(
        y=closes[-1],
        line_dash="dash",
        line_width=1,
        line_color=CANDLE_UP if up else CANDLE_DOWN,
        row=1,
        col=1,
    )
    fig.add_annotation(
        x=1,
        xref="paper",
        y=closes[-1],
        yref="y",
        text=f"{closes[-1]:.8f}",
        showarrow=False,
        # On the scale, in the right margin, rather than on the canvas: the
        # badge's left edge sits at the plot's right edge, so it covers the tick
        # it stands in for and cannot reach the candles whatever the price does.
        # The margin below is widened to hold it -- an explicit margin does
        # override the template's, which an earlier note here doubted.
        xanchor="left",
        xshift=6,
        font=dict(color="white", size=11),
        bgcolor=CANDLE_UP if up else CANDLE_DOWN,
        borderpad=3,
    )

    empty = payload.get("bars_without_trades") or 0
    subtitle = (
        f"O{opens[-1]:.8f}  H{candles[-1]['high']:.8f}  "
        f"L{candles[-1]['low']:.8f}  C{closes[-1]:.8f}   "
        f"<b>{'+' if up else ''}{change:,.2f}%</b> over {payload.get('bars')} bars"
    )
    if empty:
        subtitle += f"   ·   {empty} with no trades"

    fig.update_layout(
        template=ccdexplorer_plotly_template(theme),
        title=f"<b>{title}</b><br><sup>{subtitle}</sup>",
        height=420,
        showlegend=False,
        xaxis_rangeslider_visible=False,
        bargap=0.25,
        # Room for an eight-decimal scale and the badge over it. The template
        # leaves 24, which is right for a chart whose scale is on the left.
        margin_r=96,
    )
    # Right, as every trading chart has it: the newest candles are on the right,
    # so that is where the eye already is when it wants the number.
    fig.update_yaxes(title_text="USD", tickformat=".8f", showgrid=False, side="right", row=1, col=1)
    fig.update_yaxes(showticklabels=False, showgrid=False, side="right", row=2, col=1)
    fig.update_xaxes(title=None, row=1, col=1)
    fig.update_xaxes(title=None, row=2, col=1)
    return fig


async def _kraken_payload(app, interval: str) -> dict:
    api_result = await get_url_from_api(
        f"{app.api_url}/v2/mainnet/misc/ccd-ohlc/{interval}",
        app.httpx_client,
    )
    return (api_result.return_value if api_result.ok else None) or {}


async def kraken_figure(spec: ChartSpec, app, net: str, state: ChartState, theme: str) -> go.Figure:
    interval = state.interval.value
    return build_kraken_figure(await _kraken_payload(app, interval), interval, theme)


async def kraken_rows(spec: ChartSpec, app, net: str, state: ChartState) -> list[dict]:
    """The candles, as rows. `date` because that is the column every csv on
    this site leads with, although these are timestamps."""
    payload = await _kraken_payload(app, state.interval.value)
    return [
        {
            "date": c["at"],
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
            "volume": c["volume"],
            "trades": c["trades"],
        }
        for c in payload.get("candles") or []
    ]


#: Keyed by ChartSpec.live_source. can_be_generated checks a spec's name is
#: in here, so a spec naming a provider that does not exist gets no page
#: rather than a page that answers 500 to every caller.
FIGURES = {
    "cooldown_schedule": cooldown_schedule_figure,
    "kraken_ohlc": kraken_figure,
}

ROWS = {
    "cooldown_schedule": cooldown_schedule_rows,
    "kraken_ohlc": kraken_rows,
}
