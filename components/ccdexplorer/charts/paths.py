"""A chart's state as a path, not a query string.

/charts/daily-limits/weekly/202306/202604

Months rather than days, because a month is the precision the slider offers
-- it steps by one -- and what the chart handlers already round to. A `to`
month means the whole of it, so 202604 ends on the 30th of April.

Where a page is forgiving of a bad query parameter and falls back to its
default, a path is refused: the query string decorates an address, but the
path IS the address, and quietly drawing something other than what it names
makes the url a lie.
"""

import datetime as dt

from .models import ChartSpec, Grouping
from .state import ChartState, latest_complete_day

MONTH_FORMAT = "%Y%m"


def parse_month(value: str) -> dt.date | None:
    """The first day of a YYYYMM month, or None if that is not one."""
    if not value or len(value) != 6 or not value.isdigit():
        return None
    try:
        return dt.datetime.strptime(value, MONTH_FORMAT).date()
    except ValueError:
        return None


def month_start(value: str) -> dt.date | None:
    return parse_month(value)


def month_end(value: str) -> dt.date | None:
    """The last day of a YYYYMM month.

    Found by stepping into the next month and back a day, so February is 28
    or 29 without anyone having to say which.
    """
    start = parse_month(value)
    if start is None:
        return None
    following = (start + dt.timedelta(days=32)).replace(day=1)
    return following - dt.timedelta(days=1)


def format_month(value: dt.date) -> str:
    return value.strftime(MONTH_FORMAT)


#: Separates one trace from the next. No trace name may contain it, which
#: Series enforces by deriving url_name from alphanumerics only.
TRACE_SEPARATOR = "-"


def parse_traces(spec: ChartSpec, segment: str) -> tuple[str, ...] | None:
    """The mongo keys a trace segment names, or None if it names something
    the chart does not have.

    Refused rather than filtered: "top100-nonsense" is not a request for
    top100, it is a url that does not mean anything, and drawing most of it
    would hide that.
    """
    if not segment:
        return None
    by_url_name = {s.url_name: s.key for s in spec.display_series}
    asked = segment.split(TRACE_SEPARATOR)
    if len(asked) != len(set(asked)):
        return None
    if any(name not in by_url_name for name in asked):
        return None
    # The chart's own order, so one selection has exactly one url.
    chosen = {by_url_name[name] for name in asked}
    return tuple(s.key for s in spec.display_series if s.key in chosen)


def format_traces(spec: ChartSpec, keys: tuple[str, ...]) -> str:
    """The segment for these traces, in the chart's own order."""
    wanted = set(keys)
    return TRACE_SEPARATOR.join(series.url_name for series in spec.series if series.key in wanted)


def state_from_path(
    spec: ChartSpec,
    grouping: str,
    start: str,
    end: str,
    traces: str = "",
    today: dt.date | None = None,
) -> ChartState | None:
    """The state this path names, or None if it does not name one."""
    try:
        parsed_grouping = Grouping(grouping)
    except ValueError:
        return None
    if spec.groupings and parsed_grouping not in spec.groupings:
        return None

    first, last = month_start(start), month_end(end)
    if first is None or last is None or first > last:
        return None
    # Nothing to draw: every chart stops at yesterday, so a range beginning
    # after that names no state at all.
    if first > latest_complete_day(today):
        return None

    chosen = ""
    if traces:
        keys = parse_traces(spec, traces)
        if keys is None:
            return None
        chosen = ",".join(keys)

    return ChartState.from_query(
        spec,
        {
            "grouping": parsed_grouping.value,
            "from": first.isoformat(),
            "to": last.isoformat(),
            "traces": chosen,
        },
        today=today,
    )


def chart_path(spec: ChartSpec, state: ChartState | None, net: str = "") -> str:
    """Where this chart in this state lives.

    Without a state, the bare slug: the chart as it opens, and the shortest
    thing to hand anyone.
    """
    prefix = f"/{net}" if net else ""
    if state is None:
        return f"{prefix}/charts/{spec.slug}"
    path = (
        f"{prefix}/charts/{spec.slug}/{state.grouping.value}"
        f"/{format_month(state.start)}/{format_month(state.end)}"
    )
    # Every trace is the default, and a default is not spelled out.
    if len(state.traces) < len(spec.display_series):
        path += f"/{format_traces(spec, state.traces)}"
    return path
