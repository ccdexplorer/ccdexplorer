import datetime as dt

import dateutil
import pandas as pd
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
import uuid
from typing import Any, Optional

from ccdexplorer.ccdexplorer_site.app.routers.statistics import (
    ccdexplorer_plotly_template,
    get_all_data_for_analysis_limited,
    image_freq,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    get_theme_from_request,
    get_url_from_api,
    return_plot_response,
)
from fastapi import HTTPException

router = APIRouter()


@router.get("/{net}/charts/agent-registries", response_class=HTMLResponse)
async def get_agent_registries(
    request: Request,
    net: str,
):
    if net == "mainnet":
        chain_start = dt.date(2026, 5, 27).strftime("%Y-%m-%d")
        yesterday = (dt.datetime.now().astimezone(dt.UTC) - dt.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        filename = (
            f"/tmp/agent-registries - {dt.datetime.now():%Y-%m-%d %H-%M-%S} - {uuid.uuid4()}.csv"
        )
        return request.app.templates.TemplateResponse(
            request,
            "charts/sc_agent_registries.html",
            {
                "env": request.app.env,
                "net": net,
                "request": request,
                "chain_start": chain_start,
                "yesterday": yesterday,
                "filename": filename,
                "include_dropdown_fancy": False,
                "include_kpis": False,
            },
        )
    else:
        return request.app.templates.TemplateResponse(
            request,
            "testnet/not-available.html",
            {
                "env": request.app.env,
                "net": net,
                "request": request,
            },
        )


class PostData(BaseModel):
    theme: str
    start_date: str
    end_date: str
    group_by_selection: str
    trace_selection: Optional[str] = None
    dropdown_values_fancy: Optional[str] = None
    kpi: Optional[str] = None
    filename: str


def build_agent_registries_figure(
    all_data: list[dict],
    *,
    theme: str,
    freq: str,
    subtitle: str = "",
) -> go.Figure:
    """The figure both the page and the image route draw.

    Extracted from the POST handler so an image route can reuse it with a
    window it computed rather than one a date picker supplied. The CSV the
    page writes for its download button stays in the handler: an image route
    has nowhere to put one.
    """
    if not all_data:
        # A quiet window is not an error. Returning an empty figure keeps the
        # route drawing an empty chart rather than raising on an empty frame.
        return go.Figure(layout={"template": ccdexplorer_plotly_template(theme)})

    registries = sorted({c for row in all_data for c in (row.get("registries") or []) if c})

    df_per_day = pd.json_normalize(all_data).fillna(0)
    df_per_day = df_per_day.drop(columns=["registries"], errors="ignore")

    df_per_day["date"] = pd.to_datetime(df_per_day["date"])
    agg_map = {col: "agents_registered" for col in df_per_day.columns if col != "date"}

    df_per_day = (
        df_per_day.groupby([pd.Grouper(key="date", freq=freq, label="left", closed="left")])  # type: ignore
        .sum()
        .reset_index()
    )
    fig = go.Figure()

    if len(registries) == 1:
        title = f"Agent Registries (Contract {registries[0].strip('<>').split(',')[0]})"
    elif registries:
        title = f"Agent Registries ({len(registries)} CIS-8004 contracts)"
    else:
        title = "Agent Registries"
    fig.add_trace(
        go.Bar(
            x=df_per_day["date"].to_list(),
            y=df_per_day["agents_registered"].to_list(),
            name="Agents Registered",
            # marker=dict(color="#549FF2"),
        )
    )

    fig.update_xaxes(type="date")
    fig.update_layout(dragmode=False)

    fig.update_layout(
        barmode="stack",
        showlegend=False,
        legend_orientation="h",
        legend_y=-0.2,
        title=f"<b>{title}</b><br><sup>{subtitle}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )

    # Convert non-date columns to integers
    non_date_columns = df_per_day.columns.difference(["date"])
    # Fill NA values with 0
    df_per_day = df_per_day.fillna(0)
    df_per_day[non_date_columns] = df_per_day[non_date_columns].astype(int)

    return fig


@router.post(
    "/{net}/ajax_statistics_standalone/agent_registries",
    response_class=Response,
)
async def statistics_agent_registries(
    request: Request,
    net: str,
    post_data: PostData,
):
    # theme = await get_theme_from_request(request)
    tracks = post_data.dropdown_values_fancy
    theme = post_data.theme
    start_date_str = post_data.start_date
    end_date_str = post_data.end_date
    parsed_date: dt.datetime = dateutil.parser.parse(post_data.start_date)
    post_data.start_date = dt.datetime(parsed_date.year, parsed_date.month, 1).strftime("%Y-%m-%d")

    end_parsed: dt.datetime = dateutil.parser.parse(post_data.end_date)
    next_month = dt.datetime(end_parsed.year, end_parsed.month, 1) + relativedelta(months=1)
    last_day = next_month - relativedelta(days=1)
    post_data.end_date = last_day.strftime("%Y-%m-%d")
    analysis = "statistics_agent_registry"
    if net != "mainnet":
        return request.app.templates.TemplateResponse(
            request,
            "testnet/not-available.html",
            {
                "env": request.app.env,
                "net": net,
                "request": request,
            },
        )

    if post_data.group_by_selection == "daily":
        letter = "D"
        tooltip = "Day"
    if post_data.group_by_selection == "weekly":
        letter = "W-MON"
        tooltip = "Week"
    if post_data.group_by_selection == "monthly":
        letter = "MS"
        tooltip = "Month"

    all_data = await get_all_data_for_analysis_limited(
        analysis, request.app, post_data.start_date, post_data.end_date
    )

    # Which contracts were counted is recorded per day by the nightrunner, from
    # what the contracts themselves report supporting -- so the title can name
    # the registries actually behind the bars rather than a number typed in
    # here. Read before the frame is built: it is a list column, and summing
    # it during the groupby would just concatenate one copy per day.
    fig = build_agent_registries_figure(
        all_data, theme=theme, freq=letter, subtitle=f"{start_date_str} - {end_date_str}"
    )
    df_per_day = pd.json_normalize(all_data).fillna(0)
    df_per_day = df_per_day.drop(columns=["registries"], errors="ignore")
    df_per_day["date"] = pd.to_datetime(df_per_day["date"])
    df_per_day = (
        df_per_day.groupby([pd.Grouper(key="date", freq=letter, label="left", closed="left")])  # type: ignore
        .sum()
        .reset_index()
    )
    non_date_columns = df_per_day.columns.difference(["date"])
    df_per_day = df_per_day.fillna(0)
    df_per_day[non_date_columns] = df_per_day[non_date_columns].astype(int)

    df_per_day.to_csv(post_data.filename, index=False)
    return fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )


#: The windows the chart bot's buttons offer. One route each, never a
#: parameterised path: plot_image_paths scans route paths to build the warmer's
#: list, and _PLOT_IMAGE_ROUTE only matches a literal chart name.
IMAGE_WINDOWS = (30, 90, 180, 365)


async def agent_registries_image(request: Request, net: str, days: int):
    """One window of the agent registries chart, as a PNG the bot can fetch."""
    if net != "mainnet":
        # Not the page's "not available" template: Telegram fetches this url
        # itself and would render an HTML body as a broken image.
        raise HTTPException(status_code=404, detail="Agent registries are mainnet only.")

    end = dt.date.today()
    start = end - dt.timedelta(days=days)
    all_data = await get_all_data_for_analysis_limited(
        "statistics_agent_registry", request.app, start.isoformat(), end.isoformat()
    )
    theme = await get_theme_from_request(request)
    fig = build_agent_registries_figure(
        all_data, theme=theme, freq=image_freq(days), subtitle=f"last {days} days"
    )
    return await return_plot_response(fig, request, f"Agent Registries, {days}d")


@router.get("/plots/{net}/agent_registries_30d", response_class=Response)
@router.get("/plots/{net}/agent_registries_30d/image.png", response_class=Response)
async def agent_registries_30d(request: Request, net: str):
    return await agent_registries_image(request, net, 30)


@router.get("/plots/{net}/agent_registries_90d", response_class=Response)
@router.get("/plots/{net}/agent_registries_90d/image.png", response_class=Response)
async def agent_registries_90d(request: Request, net: str):
    return await agent_registries_image(request, net, 90)


@router.get("/plots/{net}/agent_registries_180d", response_class=Response)
@router.get("/plots/{net}/agent_registries_180d/image.png", response_class=Response)
async def agent_registries_180d(request: Request, net: str):
    return await agent_registries_image(request, net, 180)


@router.get("/plots/{net}/agent_registries_365d", response_class=Response)
@router.get("/plots/{net}/agent_registries_365d/image.png", response_class=Response)
async def agent_registries_365d(request: Request, net: str):
    return await agent_registries_image(request, net, 365)
