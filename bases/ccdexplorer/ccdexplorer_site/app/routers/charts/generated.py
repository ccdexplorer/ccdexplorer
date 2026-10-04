"""Chart pages built from their specs.

A page needs to know which fields to fetch, what to call them, how they
group and what the chart is called. The spec says all of it, so there is
nothing left for a handwritten handler to add -- and fifteen of them in
statistics.py were the same handler fifteen times, each with no way to change
the range or the grouping.

Generated at import, one GET and one POST per spec, so a chart added to the
registry gets a page without anyone remembering to write one.
"""

import datetime as dt

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from pydantic import BaseModel

from ccdexplorer.charts import (
    Agg,
    ChartSpec,
    ChartState,
    build_grouping_pipeline,
    chart_path,
)
from ccdexplorer.charts.paths import format_month, state_from_path
from ccdexplorer.charts.state import DAILY_UP_TO_DAYS, WEEKLY_UP_TO_DAYS
from ccdexplorer.env import environment
from ccdexplorer.charts.registry import ALL_SPECS

from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure
from ccdexplorer.ccdexplorer_site.app.routers.charts.images import (
    PERIOD_LABEL,
    image_path,
    parse_slider_range,
    subtitle_for,
)
from ccdexplorer.ccdexplorer_site.app.state import get_user_detailsv2
from ccdexplorer.ccdexplorer_site.app.utils import (
    empty_chart_figure,
    return_plot_response,
    theme_from_query,
    get_theme_from_request,
    get_url_from_api,
)

router = APIRouter()


#: How each rule reads in the settings panel. Plain words rather than SUM or
#: LAST: the panel is read by someone deciding whether a weekly number means
#: what they think it means, not by someone reading the registry.
AGG_WORDING = {
    Agg.SUM: "summed",
    Agg.LAST: "value on the last day",
    Agg.MEAN: "average of the days",
    Agg.DELTA_OF_LAST: "change since the previous period",
}

#: Charts whose source is already grouped by period. Nothing is combined at
#: all: the grouping selects a differently-aggregated collection, and a
#: distinct count cannot be derived from narrower ones anyway.
PRE_GROUPED_WORDING = "counted over the period itself"


def aggregation_notes(spec: ChartSpec) -> list[tuple[str, str]]:
    """What happens to the days inside a period, as (label, wording) pairs.

    Group By offers day, week and month without saying how the days are
    combined. For a sum that is obvious; for a closing value or a mean it is
    not, and that difference is the whole reason some of these charts are
    right.

    Named per trace only where the traces disagree: accounts growth draws a
    closing total beside a difference, and network activity a sum beside a
    mean. Where they all do the same thing the label is dropped and the
    wording said once -- transactions by category is five sums, and
    "Account: summed / Transfer: summed" five times over is a paragraph
    saying what one word says, against labels the legend already carries.
    """
    if spec.source_by_grouping:
        wording = {s.label: PRE_GROUPED_WORDING for s in spec.display_series}
    else:
        wording = {s.label: AGG_WORDING[s.agg] for s in spec.display_series}

    distinct = set(wording.values())
    if len(distinct) == 1:
        return [("", distinct.pop())]
    return list(wording.items())


class ChartPostData(BaseModel):
    """What the settings panel sends back. Shared by every generated page."""

    theme: str
    start_date: str
    end_date: str
    group_by_selection: str
    trace_selection: str | None = None
    filename: str | None = None


def rows_are_grouped(rows: list[dict]) -> bool:
    """Whether the API actually grouped what it returned.

    ?grouping= is new, and an API that predates it ignores the parameter --
    FastAPI drops an unknown query param silently -- answering with the raw
    daily documents. Drawn by a page that asked for weekly, those are seven
    bars where one was meant, under a title saying "per Week"; nothing about
    it looks wrong, which is exactly the problem.

    The grouping pipeline emits a _days count per bucket and the ungrouped
    branch cannot, so the rows say for themselves which they are. An empty
    result is not a wrong one: the empty state already covers that.
    """
    return not rows or "_days" in rows[0]


async def _fetch(spec: ChartSpec, app, state: ChartState) -> list[dict]:
    """The grouped rows for this chart, from the API that knows the rules."""

    async def fetch(source: str) -> list[dict]:
        result = await get_url_from_api(
            f"{app.api_url}/v2/mainnet/misc/statistics/{source}"
            f"/{state.start.isoformat()}/{state.end.isoformat()}"
            f"?grouping={state.grouping.value}",
            app.httpx_client,
        )
        return result.return_value if result.ok else []

    rows = await fetch(spec.source_for(state.grouping))
    if not spec.extra_source:
        return rows

    # Merged on the date: network activity draws CCD transferred from one
    # collection and the transaction count behind its TPS line from another.
    # A row missing from the second simply has no TPS, rather than no chart.
    wanted = {s.key for s in spec.extra_series}
    extra = {
        row["date"]: {k: v for k, v in row.items() if k in wanted}
        for row in await fetch(spec.extra_source)
    }
    return [{**row, **extra.get(row["date"], {})} for row in rows]


def share_context(spec: ChartSpec, net: str, state: ChartState) -> dict:
    """What the page needs to be worth pasting into a chat.

    Both urls carry the state. The card has to agree with the page it
    opens: a link shared at monthly over three years whose preview is the
    last year weekly describes a different chart.

    Absolute, because an unfurler has no page to resolve a relative url
    against -- but only when SITE_URL is set. Jinja renders None as the
    literal "None", and that is how a link to a host called None gets
    sent; a relative url is wrong for Open Graph but harmless on a laptop,
    which is the only place the setting is missing.
    """
    base = environment.get("SITE_URL") or ""
    page = chart_path(spec, state, net)
    # The same words the figure puts above itself, because the card and the
    # chart it opens have to agree: "per Week" over a chart of monthly bars
    # is the mislabelling this migration was about, moved one layer out.
    headline = f"{spec.title} per {PERIOD_LABEL[state.grouping]}"
    return {
        "og_title": f"{headline} — Concordium {net}",
        "og_description": (spec.description or spec.blurb or "").rstrip("."),
        "og_page_url": f"{base}{page}",
        "og_image_url": f"{base}{image_path(spec.name, net, state)}",
        # Said on the card as well as in the figure: the preview is often
        # the whole of what someone sees before deciding to open it.
        "og_subtitle": subtitle_for(state),
    }


def register_chart_page(spec: ChartSpec) -> None:
    """A page and its data endpoint for one chart."""

    def _render(request: Request, net: str, state: ChartState, user):
        return request.app.templates.TemplateResponse(
            request,
            "charts/generated_chart.html",
            {
                "request": request,
                "env": environment,
                "user": user,
                "net": net,
                "spec": spec,
                "state": state,
                "chain_start": spec.chain_start.isoformat(),
                "yesterday": (dt.datetime.now().astimezone(dt.UTC) - dt.timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ),
                "hx_post_url": f"/{net}/charts/{spec.slug}/data",
                # Only worth offering when there is more than one thing to
                # turn off; a single checkbox that cannot be unticked is a
                # control that does nothing.
                "include_trace_selection": len(spec.display_series) > 1,
                "traces": {s.key: s.label for s in spec.display_series},
                "aggregation_notes": aggregation_notes(spec),
                # The script rewrites the address bar as a path, so it
                # needs each trace's url name as well as its label.
                "trace_url_names": {s.key: s.url_name for s in spec.display_series},
                "docs": spec.docs_path,
                # The rule the address-bar rewriter needs when there are no
                # radios to read. Rendered from the server's constants, so
                # the two cannot drift into disagreeing about what a span
                # means.
                "auto_grouping": (
                    {"daily_up_to": DAILY_UP_TO_DAYS, "weekly_up_to": WEEKLY_UP_TO_DAYS}
                    if spec.automatic_grouping
                    else None
                ),
                # A path like everything else: it appears in the page
                # source, so it is a url someone can copy.
                "csv_url": (
                    f"/{net}/charts/{spec.slug}/{state.grouping.value}"
                    f"/{format_month(state.start)}/{format_month(state.end)}/data.csv"
                ),
                **share_context(spec, net, state),
            },
        )

    async def _page(request: Request, net: str, state: ChartState):
        if net != "mainnet" and spec.mainnet_only:
            return request.app.templates.TemplateResponse(
                request,
                "testnet/not-available.html",
                {"env": environment, "net": net, "request": request},
            )
        return _render(request, net, state, await get_user_detailsv2(request))

    @router.get(f"/{{net}}/charts/{spec.slug}", response_class=HTMLResponse)
    # `spec` is closed over, not a default argument: FastAPI reads a default
    # as a request field and rejects the name outright.
    async def page(request: Request, net: str):
        """The chart as it opens.

        A url carrying the old ?grouping=&from=&to= is sent to the path that
        says the same thing, so what has already been shared still lands
        somewhere -- and lands somewhere clean.
        """
        query = request.query_params
        if any(k in query for k in ("grouping", "from", "to", "window")):
            state = ChartState.from_query(spec, query)
            return RedirectResponse(chart_path(spec, state, net), status_code=308)
        return await _page(request, net, ChartState.from_query(spec, {}))

    # Registered before the trace route below, and it has to stay that way.
    # FastAPI matches in registration order, and `{traces}` is happy to
    # capture the literal "data.csv" -- which is exactly what it did, so
    # every chart's Download Data link answered 404.
    @router.get(
        f"/{{net}}/charts/{spec.slug}/{{grouping}}/{{start}}/{{end}}/data.csv",
        response_class=Response,
    )
    async def data_csv(request: Request, net: str, grouping: str, start: str, end: str):
        """The rows behind the chart, in the state the path names.

        Same shape as the page, so the file matches what is on screen and the
        link is one someone can keep.
        """
        state = state_from_path(spec, grouping, start, end)
        if state is None:
            raise HTTPException(status_code=404, detail="No such chart view.")
        rows = await _fetch(spec, request.app, state)
        keys = [s.key for s in spec.series if s.key in state.traces or spec.derived]
        header = ["date", *keys]
        lines = [",".join(header)]
        for row in rows:
            lines.append(",".join(str(row.get(k, "")) for k in header))
        return Response(
            "\n".join(lines),
            media_type="text/csv",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{spec.name}-{state.grouping.value}'
                    f'-{state.start}-{state.end}.csv"'
                )
            },
        )

    @router.get(
        f"/{{net}}/charts/{spec.slug}/{{grouping}}/{{start}}/{{end}}/{{traces}}",
        response_class=HTMLResponse,
    )
    async def page_at_traces(
        request: Request, net: str, grouping: str, start: str, end: str, traces: str
    ):
        """The chart with only some of its traces drawn.

        Absent when every trace is selected: that is the default, and a
        default does not need spelling out in the address.
        """
        state = state_from_path(spec, grouping, start, end, traces)
        if state is None:
            raise HTTPException(status_code=404, detail="No such chart view.")
        return await _page(request, net, state)

    @router.get(
        f"/{{net}}/charts/{spec.slug}/{{grouping}}/{{start}}/{{end}}",
        response_class=HTMLResponse,
    )
    async def page_at(request: Request, net: str, grouping: str, start: str, end: str):
        """The chart at a named grouping and range.

        Refused rather than defaulted when the path does not name a real
        state: a query parameter decorates an address, but the path is the
        address, and drawing something else would make the url a lie.
        """
        state = state_from_path(spec, grouping, start, end)
        if state is None:
            raise HTTPException(status_code=404, detail="No such chart view.")
        return await _page(request, net, state)

    @router.post(f"/{{net}}/charts/{spec.slug}/data", response_class=Response)
    async def data(request: Request, net: str, post_data: ChartPostData):
        # The slider is the only thing on the page that sets a range, so its
        # dates stand. The windows live in the bot, which cannot show one.
        #
        # Parsed rather than passed through: the slider's tooltips read
        # "Jun 2023", which date.fromisoformat cannot, and dropping the range
        # silently is what made dragging it appear to do nothing.
        window = parse_slider_range(post_data.start_date, post_data.end_date)
        asked = {
            "grouping": post_data.group_by_selection,
            "traces": post_data.trace_selection or "",
        }
        if window is not None:
            asked["from"], asked["to"] = (d.isoformat() for d in window)
        state = ChartState.from_query(spec, asked)
        rows = await _fetch(spec, request.app, state)
        theme = post_data.theme or await get_theme_from_request(request)
        if not rows_are_grouped(rows):
            # A missing chart is better than a mislabelled one.
            fig = empty_chart_figure(
                theme, "Chart data unavailable — the API has not been updated yet"
            )
        else:
            fig = build_figure(spec, rows, state, theme=theme)
        return fig.to_html(
            config={"responsive": True, "displayModeBar": False},
            full_html=False,
            include_plotlyjs=False,
        )

    async def _image(request: Request, net: str, state: ChartState):
        """The chart as a png, which is what the bot and the share link use."""
        if net != "mainnet" and spec.mainnet_only:
            raise HTTPException(status_code=404, detail=f"{spec.title} is mainnet only.")
        rows = await _fetch(spec, request.app, state)
        theme = theme_from_query(request)
        if not rows_are_grouped(rows):
            figure = empty_chart_figure(
                theme, "Chart data unavailable — the API has not been updated yet"
            )
        else:
            figure = build_figure(spec, rows, state, theme=theme)
        return await return_plot_response(figure, request, spec.title)

    @router.get(f"/plots/{{net}}/{spec.name}", response_class=Response)
    @router.get(f"/plots/{{net}}/{spec.name}/image.png", response_class=Response)
    async def image(request: Request, net: str):
        """The chart as it opens.

        Kept at its own name because this is what the share button copies
        and what Telegram has already cached. A url still carrying the old
        query form is sent to the path that means the same thing.
        """
        query = request.query_params
        if any(k in query for k in ("grouping", "window", "from", "to")):
            state = ChartState.from_query(spec, query)
            return RedirectResponse(
                image_path(spec.name, net, state, str(request.url.query)),
                status_code=308,
            )
        return await _image(request, net, ChartState.from_query(spec, {}))

    @router.get(
        f"/plots/{{net}}/{spec.name}/{{grouping}}/{{start}}/{{end}}/image.png",
        response_class=Response,
    )
    async def image_at(request: Request, net: str, grouping: str, start: str, end: str):
        state = state_from_path(spec, grouping, start, end)
        if state is None:
            raise HTTPException(status_code=404, detail="No such chart view.")
        return await _image(request, net, state)

    @router.get(
        f"/plots/{{net}}/{spec.name}/{{grouping}}/{{start}}/{{end}}/{{traces}}/image.png",
        response_class=Response,
    )
    async def image_at_traces(
        request: Request, net: str, grouping: str, start: str, end: str, traces: str
    ):
        state = state_from_path(spec, grouping, start, end, traces)
        if state is None:
            raise HTTPException(status_code=404, detail="No such chart view.")
        return await _image(request, net, state)

    # Named so a traceback says which chart, rather than "page" fifteen times.
    page.__name__ = f"page_{spec.name}"
    image.__name__ = f"image_{spec.name}"
    image_at.__name__ = f"image_at_{spec.name}"
    image_at_traces.__name__ = f"image_at_traces_{spec.name}"
    data_csv.__name__ = f"data_csv_{spec.name}"
    page_at.__name__ = f"page_at_{spec.name}"
    page_at_traces.__name__ = f"page_at_traces_{spec.name}"
    data.__name__ = f"data_{spec.name}"


def can_be_generated(spec: ChartSpec) -> bool:
    """Whether a generated page could actually serve this chart.

    Intraday specs have no series to draw, and statistics_plt's pipeline
    refuses to build -- a page for either would fail on every request. Asked
    of the pipeline rather than listed here, so a source that gains support
    gets its page without anyone remembering to come back.
    """
    if not spec.source or not spec.series:
        return False
    today = dt.date.today().isoformat()
    try:
        build_grouping_pipeline(spec, today, today, spec.default_grouping)
    except NotImplementedError:
        return False
    return True


for _registered in ALL_SPECS:
    if can_be_generated(_registered):
        register_chart_page(_registered)
