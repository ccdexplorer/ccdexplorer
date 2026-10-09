"""A chart page's configuration, as something that fits in a URL.

A page is not an API. Every value here falls back to the spec's default rather
than raising, because the alternative is a shared link that shows an error
page instead of the chart somebody was trying to pass on.
"""

import datetime as dt

from pydantic import BaseModel, ConfigDict

from .models import Axis, ChartSpec, Grouping, Interval, Window


def latest_complete_day(today: dt.date | None = None) -> dt.date:
    """The last day a chart can draw: yesterday.

    Every one of these is built from the last block of a day, so today's
    figure does not exist until the day does. Drawn anyway it is a final bar
    a fraction of its neighbours, or a line dipping at the right-hand edge --
    which reads as the chain going quiet rather than as the clock.
    """
    return (today or dt.datetime.now(dt.UTC).date()) - dt.timedelta(days=1)


def resolve_window(
    spec: ChartSpec, window: Window, today: dt.date | None = None
) -> tuple[dt.date, dt.date]:
    """The date range a window covers for this chart."""
    end = latest_complete_day(today)
    days = window.days()
    if days is None:
        return spec.chain_start, end
    return max(end - dt.timedelta(days=days), spec.chain_start), end


#: Where the resolution changes, in days of span. Chosen against what a
#: 720-pixel-wide chart can show and what a reader can count: six months of
#: daily points is about 180 marks, which is dense but readable, and three
#: years of weekly is about 156. Beyond that daily is more points than the
#: image has pixels.
DAILY_UP_TO_DAYS = 182
WEEKLY_UP_TO_DAYS = 1095


def resolution_for(start: dt.date, end: dt.date) -> Grouping:
    """The grouping a chart that does not ask should draw itself at.

    Only for charts whose grouping changes nothing about the number -- see
    ChartSpec.automatic_grouping. For the rest the grouping is the reader's
    question to answer.
    """
    span = (end - start).days
    if span <= DAILY_UP_TO_DAYS:
        return Grouping.DAILY
    if span <= WEEKLY_UP_TO_DAYS:
        return Grouping.WEEKLY
    return Grouping.MONTHLY


class ChartState(BaseModel):
    """What the reader has selected."""

    model_config = ConfigDict(frozen=True)

    grouping: Grouping
    window: Window
    start: dt.date
    end: dt.date
    traces: tuple[str, ...]
    #: True when start/end came from the query rather than from the window,
    #: so to_query() round-trips what the reader actually chose.
    explicit_dates: bool = False
    #: The candle interval, for a chart configured by one. None elsewhere:
    #: the calendar charts have no such thing and a value would be a lie.
    interval: Interval | None = None

    @classmethod
    def from_query(cls, spec: ChartSpec, params, today: dt.date | None = None) -> "ChartState":
        grouping = _one_of(params.get("grouping"), Grouping, spec.groupings, spec.default_grouping)
        window = _one_of(params.get("window"), Window, spec.windows, spec.default_window)
        start, end = resolve_window(spec, window, today)

        explicit = False
        parsed = _dates(params.get("from"), params.get("to"))
        if parsed is not None:
            start, end = parsed
            # Clamped: a path naming this month runs to a last day that has
            # not happened yet.
            end = min(end, latest_complete_day(today))
            explicit = True

        # The traces the reader sees, which for a derived chart are not
        # the fields it reads.
        known = {s.key for s in spec.display_series}
        asked = [t for t in (params.get("traces") or "").split(",") if t in known]
        traces = tuple(asked) if asked else tuple(s.key for s in spec.display_series)

        interval = None
        if spec.axis is Axis.INTERVAL:
            interval = _one_of(
                params.get("interval"), Interval, spec.intervals, spec.default_interval
            )

        # A LOOKBACK chart's x-axis is a fixed span forward from today, and
        # its window is the lookback for a reference figure rather than the
        # range. Read as a range -- which is what every other chart does --
        # the cooldown schedule would draw the last thirty days, a chart of a
        # schedule that has already happened. Last here, so a from/to in a
        # shared link cannot put it back in the past either.
        if spec.axis is Axis.LOOKBACK:
            start = today or dt.datetime.now(dt.UTC).date()
            end = start + dt.timedelta(days=spec.horizon_days - 1)
            explicit = False

        # Last, because it needs the range: a chart that picks its own
        # resolution has nothing to read from the request, and a grouping
        # left over in an old url would otherwise draw two points.
        if spec.automatic_grouping:
            grouping = resolution_for(start, end)

        return cls(
            grouping=grouping,
            window=window,
            start=start,
            end=end,
            traces=traces,
            explicit_dates=explicit,
            interval=interval,
        )

    def to_query(self) -> dict[str, str]:
        query = {"grouping": self.grouping.value, "window": self.window.value}
        if self.explicit_dates:
            query["from"] = self.start.isoformat()
            query["to"] = self.end.isoformat()
        query["traces"] = ",".join(self.traces)
        if self.interval is not None:
            query["interval"] = self.interval.value
        return query


def lookback_range(
    spec: ChartSpec, state: ChartState, today: dt.date | None = None
) -> tuple[dt.date, dt.date]:
    """The history a LOOKBACK chart's reference figure is measured over.

    Not state.start/state.end, which are the forward horizon. This is the
    other direction: the stored days behind today that the dashed average on
    the cooldown schedule is an average of.

    resolve_window already ends at the last complete day and clamps the start
    to the chain start, which is what this needs: the stored history stops at
    yesterday and does not reach back before the collection existed.
    """
    return resolve_window(spec, state.window, today)


def _one_of(raw, enum, allowed, default):
    """The requested value, if the spec offers it; the default otherwise."""
    if raw is None:
        return default
    try:
        value = enum(raw)
    except ValueError:
        return default
    return value if (not allowed or value in allowed) else default


def _dates(raw_from, raw_to):
    if not raw_from or not raw_to:
        return None
    try:
        return dt.date.fromisoformat(raw_from), dt.date.fromisoformat(raw_to)
    except ValueError:
        return None
