"""Turning a chart's requested state into the arguments a figure builder wants.

The image routes used to be one route per window -- transactions_count_90d --
with the grouping inferred from the window by image_freq(). Window and
grouping are independent now and both come off the query string, so the
inference is gone and the mapping is explicit.
"""

import datetime as dt
from urllib.parse import parse_qsl, urlencode

from fastapi import Request

from ccdexplorer.charts import ChartSpec, ChartState, Grouping, Window, resolve_window

#: The pandas frequency each grouping draws at. "W-MON" because the Mongo
#: pipeline truncates weeks to Monday, and a chart whose bars disagreed with
#: its own data would be wrong in a way nothing looks like.
PANDAS_FREQ = {
    Grouping.DAILY: "D",
    Grouping.WEEKLY: "W-MON",
    Grouping.MONTHLY: "MS",
}

#: What one bar covers, for the title. Kept beside PANDAS_FREQ deliberately:
#: these two have to agree, and the last time they drifted a year of weekly
#: bars claimed to be daily ones.
PERIOD_LABEL = {
    Grouping.DAILY: "Day",
    Grouping.WEEKLY: "Week",
    Grouping.MONTHLY: "Month",
}

#: What each retired route name meant. These urls are in Telegram's file cache
#: and in links people have already shared, so they redirect rather than 404.
#: 180d has no exact Window; 90d is the nearest that does not overstate it.
LEGACY_WINDOWS = {"30d": "30d", "90d": "90d", "180d": "90d", "365d": "1y"}


def range_for_post(
    spec_name: str, start_date: str, end_date: str, window: str | None
) -> tuple[dt.date, dt.date]:
    """The range a chart POST should draw, given the slider and the Range row.

    The window wins when it is set, because it is the control the reader just
    touched; without one the slider's own dates stand. An unrecognised value
    falls back to the slider rather than raising -- a page is not an API, and
    a stale link should still draw a chart.
    """
    from ccdexplorer.charts.registry import BY_NAME

    spec = BY_NAME.get(spec_name)
    if spec is not None and window:
        try:
            return resolve_window(spec, Window(window))
        except ValueError:
            pass
    return dt.date.fromisoformat(start_date), dt.date.fromisoformat(end_date)


def state_for(spec: ChartSpec, request: Request) -> ChartState:
    """The chart state this image request is asking for."""
    return ChartState.from_query(spec, request.query_params)


def freq_and_period(state: ChartState) -> tuple[str, str]:
    return PANDAS_FREQ[state.grouping], PERIOD_LABEL[state.grouping]


def subtitle_for(state: ChartState, today: dt.date | None = None) -> str:
    """The date range under the title, in the words the window was asked in."""
    if state.explicit_dates:
        return f"{state.start.isoformat()} - {state.end.isoformat()}"
    days = state.window.days()
    return f"last {days} days" if days else "all time"


def legacy_redirect_target(net: str, name: str, window_suffix: str, query: str = "") -> str | None:
    """Where a retired `<name>_<window>` url should send the caller.

    None for a suffix that was never a route, so the caller can 404 rather
    than redirect to a window that does not exist.
    """
    target = LEGACY_WINDOWS.get(window_suffix)
    if target is None:
        return None
    # Everything the caller sent except what this redirect decides. theme is
    # the one that matters: every Telegram-cached legacy url carries
    # theme=light, and dropping it returns a black rectangle to a light chat.
    kept = [(k, v) for k, v in parse_qsl(query) if k not in ("window", "grouping")]
    extra = f"&{urlencode(kept)}" if kept else ""
    return f"/plots/{net}/{name}/image.png?window={target}&grouping=weekly{extra}"


#: Query parameters that are not addresses, and so survive a redirect to the
#: path form. `theme` is a rendering preference with a working default, like
#: asking for a different stylesheet. `t` is a cache-busting bucket -- it
#: exists precisely so the url stops matching when the chart changes, which
#: is the opposite of identifying one.
NON_ADDRESS_PARAMS = ("theme", "t")


def image_path(name: str, net: str, state, query: str = "") -> str:
    """Where this chart's png lives, in the same shape as its page."""
    from urllib.parse import parse_qsl, urlencode

    from ccdexplorer.charts.paths import format_month

    path = (
        f"/plots/{net}/{name}/{state.grouping.value}"
        f"/{format_month(state.start)}/{format_month(state.end)}/image.png"
    )
    kept = [(k, v) for k, v in parse_qsl(query) if k in NON_ADDRESS_PARAMS]
    return f"{path}?{urlencode(kept)}" if kept else path


def parse_slider_range(start: str, end: str) -> tuple[dt.date, dt.date] | None:
    """The range the date slider is showing, or None if that is not one.

    Its tooltips read "Jun 2023": month and year, because the slider steps by
    a month. date.fromisoformat cannot read that, and dropping the range
    silently is what made dragging the slider appear to do nothing.

    Snapped to whole months, which is both what the slider can express and
    what the urls carry, so a dragged range and a typed one agree.
    """
    import dateutil.parser

    try:
        first = dateutil.parser.parse(start).date().replace(day=1)
        last = dateutil.parser.parse(end).date()
    except (ValueError, OverflowError, TypeError):
        return None
    if first > last:
        return None
    following = (last.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
    return first, following - dt.timedelta(days=1)
