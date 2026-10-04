"""One figure for the charts that only ever needed one.

Fifteen handlers in statistics.py each fetched a series, drew it and set a
title. What differed between them -- the field, the colour, the label, the
chart kind -- is what the spec already carries, so they collapse into this.

The grouping is the reader's choice now, so the title has to say which one it
drew. That label and the data have gone out of step before: a year of weekly
bars once drew under a title saying "per Day", which is worse than a dense
chart because nothing about it looks wrong.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import datetime as dt

from ccdexplorer.charts import ChartSpec, ChartState, Grouping, Kind

from ccdexplorer.ccdexplorer_site.app.routers.charts.images import (
    PERIOD_LABEL,
    subtitle_for,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    ccdexplorer_plotly_template,
    empty_chart_figure,
)


#: A bucket covering fewer days than its period is partial: a 90-day window
#: starts and ends mid-week, so the first and last weekly bars are short.
#: Drawn plainly they read as activity collapsing at both ends of the chart,
#: which is a claim about the chain rather than about the calendar.
_FULL_DAYS = {Grouping.DAILY: 1, Grouping.WEEKLY: 7}


def expected_days(bucket: str, grouping: Grouping) -> int:
    """How many days a complete bucket starting on `bucket` would hold."""
    fixed = _FULL_DAYS.get(grouping)
    if fixed is not None:
        return fixed
    # A month is measured against its own length; February is not short.
    start = dt.date.fromisoformat(bucket[:10]).replace(day=1)
    following = (start + dt.timedelta(days=32)).replace(day=1)
    return (following - start).days


def partial_flags(rows: list[dict], grouping: Grouping) -> list[bool]:
    """Which rows cover less than their whole period.

    A row with no _days is not guessed at: an older API response carries
    none, and absent is not the same as short.
    """
    flags = []
    for row in rows:
        days = row.get("_days")
        flags.append(days is not None and days < expected_days(str(row["date"]), grouping))
    return flags


#: Charts that plot a computation of their fields rather than the fields.
#: Each returns the traces to draw -- (label, colour, values) -- or an empty
#: list when the data it needs is not there, which draws the empty state
#: rather than a chart of something else.
#:
#: The constants are the handwritten charts', kept so the two agree: 501 is
#: the energy a regular transfer costs, and the million turns microCCD into
#: CCD.
TRANSFER_ENERGY = 501


def _transfer_cost(frame):
    needed = ("GTU_numerator", "GTU_denominator", "NRG_numerator", "NRG_denominator")
    if any(c not in frame.columns for c in needed):
        return {}
    cost = (
        TRANSFER_ENERGY
        / 1_000_000
        * frame["GTU_numerator"]
        / frame["GTU_denominator"]
        / (frame["NRG_denominator"] / frame["NRG_numerator"])
    )
    return {"cost": cost}


def _percentage_staked(frame):
    if "staked" not in frame.columns or "total_supply" not in frame.columns:
        return {}
    share = frame["staked"] / frame["total_supply"].replace(0, pd.NA) * 100
    return {"share": share}


def _active_validators(frame):
    if "validator_count" not in frame.columns:
        return {}
    # suspended_count was added in March 2025; the first four years of
    # documents have no such field, and subtracting a missing one gives NaN
    # -- so the active line simply stopped before 2025 while the chart
    # claimed to show all of history.
    suspended = frame.get("suspended_count")
    suspended = pd.Series(0, index=frame.index) if suspended is None else suspended.fillna(0)
    return {
        "active": frame["validator_count"] - suspended,
        "suspended": suspended,
    }


def _accounts_level_and_growth(frame):
    out = {}
    if "account_level" in frame.columns:
        out["level"] = frame["account_level"]
    if "account_count" in frame.columns:
        out["growth"] = frame["account_count"]
    return out


#: Seconds in a day: TPS is the day's transaction count spread over it.
SECONDS_PER_DAY = 86_400


def _activity_and_tps(frame):
    out = {}
    if "network_activity" in frame.columns:
        out["activity"] = frame["network_activity"]
    if "account_transaction" in frame.columns:
        # One source failing costs one line, not the whole chart.
        out["tps"] = frame["account_transaction"] / SECONDS_PER_DAY
    return out


DERIVATIONS = {
    "activity_and_tps": _activity_and_tps,
    "transfer_cost": _transfer_cost,
    "percentage_staked": _percentage_staked,
    "active_validators": _active_validators,
    "accounts_level_and_growth": _accounts_level_and_growth,
}


def build_figure(
    spec: ChartSpec,
    rows: list[dict],
    state: ChartState,
    *,
    theme: str,
) -> go.Figure:
    """The figure for `spec` over `rows`, drawn in the state the reader chose."""
    if not spec.series or not spec.source:
        raise ValueError(
            f"{spec.name} is intraday: it has no series to draw generically. "
            "Its own handler builds its candles."
        )

    if not rows:
        # Not a bare figure: empty axes read as a flat line at zero, which is
        # a claim about the data rather than an absence of it.
        return empty_chart_figure(theme, f"No data for {subtitle_for(state)}")

    frame = pd.json_normalize(rows)
    dates = pd.to_datetime(frame["date"]).to_list()

    partial = partial_flags(rows, state.grouping)

    # A trace on its own axis needs a figure that has one: TPS against CCD
    # transferred is two scales that share no useful range.
    secondary = any(s.secondary_y for s in spec.display_series)
    fig = make_subplots(specs=[[{"secondary_y": True}]]) if secondary else go.Figure()

    # Derived charts compute what they draw; the rest read it straight off
    # the frame. Either way the reader sees display_series, selects from it
    # and shares it in a url.
    computed = DERIVATIONS[spec.derived](frame) if spec.derived else {}

    for series in spec.display_series:
        if series.key not in state.traces:
            continue
        if spec.derived:
            if series.key not in computed:
                # One source failing costs one line, not the chart.
                continue
            values = computed[series.key]
        elif series.key in frame.columns:
            values = frame[series.key]
        else:
            # Skipped rather than drawn as zero: a series the data never
            # carried is absent, not a flat line at the bottom.
            continue
        if series.scale != 1:
            # The stored unit is not the one the chart shows: fee_for_day is
            # microCCD, and a week of it read as 227 billion rather than 227
            # thousand.
            values = values / series.scale
        drawn = list(values) if hasattr(values, "__iter__") else [values] * len(dates)
        # Only a number a short period understates is worth marking. A
        # snapshot is right however many days contributed to it.
        marks = partial if series.short_period_understates else [False] * len(dates)
        trace = _trace(series.kind or spec.kind, dates, drawn, series, marks)
        if secondary:
            fig.add_trace(trace, secondary_y=series.secondary_y)
        else:
            fig.add_trace(trace)

    return _finish(fig, spec, state, theme)


def _finish(fig: go.Figure, spec: ChartSpec, state: ChartState, theme: str) -> go.Figure:
    """The layout both paths share."""
    if not fig.data:
        return empty_chart_figure(theme, "No series to draw for this selection")

    fig.update_xaxes(type="date", title=None)
    if spec.log_y:
        # Fee stabilization spans orders of magnitude; linear it is a flat
        # line with one spike.
        fig.update_yaxes(type="log")
    fig.update_layout(
        barmode="stack" if spec.kind is Kind.STACKED_BAR else None,
        showlegend=len(fig.data) > 1,
        title=(
            f"<b>{spec.title} per {PERIOD_LABEL[state.grouping]}</b>"
            f"<br><sup>{subtitle_for(state)}</sup>"
        ),
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )
    return fig


#: How faded a partial bar is. Visible as different without being so faint
#: it reads as missing.
PARTIAL_OPACITY = 0.45
FULL_OPACITY = 1.0


def _trace(kind: Kind, dates, values, series, partial: list[bool]):
    """One trace, in the shape its chart draws."""
    # Said twice on purpose: the fade catches the eye and the hover explains
    # it, because a faded bar alone could be read as any kind of emphasis.
    note = ["<br><i>partial period — fewer days than the rest</i>" if p else "" for p in partial]
    # customdata on its own displays nothing; plotly needs a template that
    # refers to it. Without one the faded bar had no explanation anywhere,
    # and the first thing anyone asked was why two bars were a different
    # colour.
    hover = f"%{{x|%d %b %Y}}<br>{series.label}: %{{y}}%{{customdata}}<extra></extra>"
    if kind is Kind.AREA:
        # Stacked bands making up a total: the band heights are the point,
        # so they stack rather than overlay.
        return go.Scatter(
            x=dates,
            y=values,
            name=series.label,
            mode="lines",
            line=dict(width=0.5, color=series.colour),
            fillcolor=series.colour,
            stackgroup="one",
            customdata=note,
            hovertemplate=hover,
        )
    if kind is Kind.LINE:
        # A line has no per-point weight to vary, so the note carries it.
        return go.Scatter(
            x=dates,
            y=values,
            name=series.label,
            mode="lines",
            marker=dict(color=series.colour),
            customdata=note,
            hovertemplate=hover,
        )
    return go.Bar(
        x=dates,
        y=values,
        name=series.label,
        marker=dict(
            color=series.colour,
            opacity=[PARTIAL_OPACITY if p else FULL_OPACITY for p in partial],
        ),
        customdata=note,
        hovertemplate=hover,
    )
