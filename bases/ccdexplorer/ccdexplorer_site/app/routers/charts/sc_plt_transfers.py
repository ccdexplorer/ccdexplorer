import datetime as dt

import pandas as pd
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from pydantic import BaseModel
import uuid
from typing import Any, Optional

from ccdexplorer.charts import ChartState
from ccdexplorer.charts.paths import state_from_path
from ccdexplorer.ccdexplorer_site.app.routers.charts.images import (
    freq_and_period,
    image_path,
    legacy_redirect_target,
    state_for,
    subtitle_for,
)
from ccdexplorer.charts.registry import BY_NAME

from ccdexplorer.ccdexplorer_site.app.routers.statistics import (
    ccdexplorer_plotly_template,
    get_all_data_for_analysis_limited,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    parse_slider_date,
    theme_from_query,
    get_url_from_api,
    return_plot_response,
)
from fastapi import HTTPException

router = APIRouter()


@router.get("/{net}/charts/plt-transfers", response_class=HTMLResponse)
async def get_plt_transfers(
    request: Request,
    net: str,
):
    if net == "mainnet":
        request.state.api_calls = {}
        request.state.api_calls["Protocol-Level Tokens"] = (
            f"{request.app.api_url}/docs#/Protocol-Level%20Tokens/get_all_plt_tokens"
        )
        chain_start = dt.date(2025, 9, 22).strftime("%Y-%m-%d")
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/{net}/plts/overview",
            request.app.httpx_client,
        )
        plts: dict = api_result.return_value if api_result.ok else {}  # type: ignore
        stablecoin_tracks = sorted(list(set([x["stablecoin_tracks"] for x in plts.values()])))
        stablecoin_tracks = {x: x for x in stablecoin_tracks if x is not None}
        if "XAU" in stablecoin_tracks:
            stablecoin_tracks["XAU"] = "Gold"
        yesterday = (dt.datetime.now().astimezone(dt.UTC) - dt.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        filename = (
            f"/tmp/plt-transfers - {dt.datetime.now():%Y-%m-%d %H-%M-%S} - {uuid.uuid4()}.csv"
        )
        return request.app.templates.TemplateResponse(
            request,
            "charts/sc_plt_transfers.html",
            {
                "env": request.app.env,
                "net": net,
                "request": request,
                "chain_start": chain_start,
                "state": ChartState.from_query(BY_NAME["plt_tvl"], request.query_params),
                "yesterday": yesterday,
                "filename": filename,
                "include_dropdown_fancy": True,
                "dropdown_label": "PLTs that track",
                "dropdown_elements": stablecoin_tracks,
                "include_kpis": True,
                "kpi_elements": {
                    "tvl_in_usd": "TVL (in USD)",
                    "mint": "Mint",
                    "transfer": "Transfer",
                    "burn": "Burn",
                    "tx_count": "Transaction Count",
                },
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


def extract_usd_transfers(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for entry in data:
        date = entry["date"]
        tokens = entry["tokens"]
        simplified_tokens = {}
        for token_name, token_data in tokens.items():
            simplified_tokens[token_name] = {
                "USD.transfer": token_data["USD"]["transfer"],
                "USD.burn": token_data["USD"]["burn"],
                "USD.mint": token_data["USD"]["mint"],
                "USD.total_supply": token_data["USD"]["total_supply"],
                "count_txs": token_data["count_txs"],
            }
        result.append({"date": date, "tokens": simplified_tokens})
    return result


class PostData(BaseModel):
    theme: str
    start_date: str
    end_date: str
    group_by_selection: str
    trace_selection: Optional[str] = None
    dropdown_values_fancy: str
    kpi: Optional[str] = None
    filename: str


@router.post(
    "/{net}/ajax_statistics_standalone/plt_transfers",
    response_class=Response,
)
async def statistics_plt_transfers(
    request: Request,
    net: str,
    post_data: PostData,
):
    tracks = post_data.dropdown_values_fancy
    theme = post_data.theme
    start_date_str = post_data.start_date
    end_date_str = post_data.end_date
    parsed_date: dt.datetime = parse_slider_date(post_data.start_date, "start_date")
    post_data.start_date = dt.datetime(parsed_date.year, parsed_date.month, 1).strftime("%Y-%m-%d")

    end_parsed: dt.datetime = parse_slider_date(post_data.end_date, "end_date")
    next_month = dt.datetime(end_parsed.year, end_parsed.month, 1) + relativedelta(months=1)
    last_day = next_month - relativedelta(days=1)
    post_data.end_date = last_day.strftime("%Y-%m-%d")
    analysis = "statistics_plt"
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

    df_per_day = pd.json_normalize(extract_usd_transfers(all_data)).fillna(0)  # type: ignore

    df_per_day["date"] = pd.to_datetime(df_per_day["date"])
    agg_map = {
        col: ("last" if "USD.total_supply" in col else "sum")
        for col in df_per_day.columns
        if col != "date"
    }

    df_per_day = (
        df_per_day.groupby([pd.Grouper(key="date", freq=letter, label="left", closed="left")])  # type: ignore
        .agg(agg_map)
        .reset_index()
    )
    fig = go.Figure()

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/plts/overview",
        request.app.httpx_client,
    )
    plts: dict = api_result.return_value if api_result.ok else {}  # type: ignore

    for plt in plts.values():
        if plt["stablecoin_tracks"] == tracks:
            if post_data.kpi in ["mint", "transfer", "burn"]:
                title = f"{post_data.kpi.capitalize()} for stablecoins tracking {tracks} (in USD)"
                fig.add_trace(
                    go.Bar(
                        x=df_per_day["date"].to_list(),
                        y=df_per_day[f"tokens.{plt['_id']}.USD.{post_data.kpi}"].to_list(),
                        name=f"{plt['_id']}",
                        # marker=dict(color="#549FF2"),
                    )
                )
            if post_data.kpi == "tx_count":
                title = f"Tx count for stablecoins tracking {tracks}"
                fig.add_trace(
                    go.Bar(
                        x=df_per_day["date"].to_list(),
                        y=df_per_day[f"tokens.{plt['_id']}.count_txs"].to_list(),  # type: ignore
                        name=f"{plt['_id']}",
                        # marker=dict(color="#549FF2"),
                    )
                )
            if post_data.kpi == "tvl_in_usd":
                title = f"TVL (end of period) for stablecoins tracking {tracks} (in USD)"
                fig.add_trace(
                    go.Bar(
                        x=df_per_day["date"].to_list(),
                        y=df_per_day[f"tokens.{plt['_id']}.USD.total_supply"].to_list(),  # type: ignore
                        name=f"{plt['_id']}",
                        # marker=dict(color="#549FF2"),
                    )
                )
    fig.update_xaxes(type="date")

    fig.update_layout(
        barmode="stack",
        showlegend=True,
        legend_orientation="h",
        legend_y=-0.2,
        title=f"<b>{title}</b><br><sup>{start_date_str} - {end_date_str}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )

    # Convert non-date columns to integers
    non_date_columns = df_per_day.columns.difference(["date"])
    # Fill NA values with 0
    df_per_day = df_per_day.fillna(0)
    df_per_day[non_date_columns] = df_per_day[non_date_columns].astype(int)

    df_per_day.to_csv(post_data.filename, index=False)
    return fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )


def build_plt_tvl_figure(
    all_data: list[dict],
    *,
    theme: str,
    freq: str,
    stablecoins: set[str] | None = None,
    subtitle: str = "",
) -> go.Figure:
    """Total PLT stablecoin TVL in USD, as one line.

    Not the page's figure. The page stacks a bar per token and filters to the
    stablecoins tracking one currency, which needs an API call to know which
    those are; at card size in a chat that is unreadable, and the question a
    reader has there is how much is locked in total. Same data, different
    chart, so there is nothing to share.

    `last` and not `sum`: TVL is a level, and adding two days of it would
    invent money. The page's own agg_map already treats these columns that way.
    """
    if not all_data:
        return go.Figure(layout={"template": ccdexplorer_plotly_template(theme)})

    df = pd.json_normalize(extract_usd_transfers(all_data)).fillna(0)
    df["date"] = pd.to_datetime(df["date"])
    supply = [c for c in df.columns if c.endswith("USD.total_supply")]
    if stablecoins is not None:
        # The title says stablecoin and the route is named plt_tvl, so a PLT
        # that tracks nothing must not be in the total. `stablecoins` is empty
        # when we could not find out which those are -- then there is no honest
        # number to draw, and a blank chart beats one that is confidently wrong.
        supply = [c for c in supply if c.split(".")[1] in stablecoins]
    if not supply:
        return go.Figure(layout={"template": ccdexplorer_plotly_template(theme)})

    # Per token first, so a token that stops reporting keeps its last known
    # level rather than dropping the total to zero.
    df = (
        df.groupby([pd.Grouper(key="date", freq=freq, label="left", closed="left")])[supply]
        .last()
        .reset_index()
    )
    # And carried across empty bins for the same reason. A day the nightly run
    # skipped has no rows, so .last() gives NaN and the row sum skips it to 0 --
    # which drew a cliff to zero and back, as though every stablecoin had been
    # redeemed overnight and reissued the next morning.
    df[supply] = df[supply].ffill()
    df["tvl"] = df[supply].sum(axis=1)

    fig = go.Figure(
        go.Scatter(
            x=df["date"].to_list(),
            y=df["tvl"].to_list(),
            name="TVL",
            mode="lines",
            fill="tozeroy",
        )
    )
    fig.update_xaxes(type="date")
    fig.update_layout(
        showlegend=False,
        dragmode=False,
        title=f"<b>PLT stablecoin TVL (USD)</b><br><sup>{subtitle}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )
    return fig


async def plt_tvl_image(request: Request, net: str, state: ChartState):
    """One window of total PLT stablecoin TVL, as a PNG the bot can fetch."""
    if net != "mainnet":
        raise HTTPException(status_code=404, detail="PLT TVL is mainnet only.")

    all_data = await get_all_data_for_analysis_limited(
        "statistics_plt", request.app, state.start.isoformat(), state.end.isoformat()
    )

    # Which PLTs are stablecoins is not in the statistics data, so it comes
    # from the same overview endpoint the page uses. A failure here yields an
    # empty set, which draws nothing rather than every PLT under a title that
    # says stablecoin.
    overview = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/plts/overview", request.app.httpx_client
    )
    plts: dict = overview.return_value if overview.ok else {}
    stablecoins = {
        str(plt.get("_id")) for plt in plts.values() if plt.get("stablecoin_tracks") is not None
    }

    # theme_from_query, not get_theme_from_request: this is a GET with no
    # body, and the latter falls back to dark. Telegram sends neither a
    # parameter nor a cookie, so every PLT chart in a chat was a black
    # rectangle. Light is what a request that says nothing should get.
    theme = theme_from_query(request)
    fig = build_plt_tvl_figure(
        all_data,
        theme=theme,
        freq=freq_and_period(state)[0],
        stablecoins=stablecoins,
        subtitle=f"{subtitle_for(state)}, by {freq_and_period(state)[1].lower()}",
    )
    return await return_plot_response(fig, request, f"PLT stablecoin TVL, {state.window.value}")


@router.get("/plots/{net}/plt_tvl", response_class=Response)
@router.get("/plots/{net}/plt_tvl/image.png", response_class=Response)
async def plt_tvl_plot(request: Request, net: str):
    """The chart as it opens, and what the share button copies.

    A url still carrying the old ?grouping=&window= is sent to the path that
    says the same thing, so anything already shared lands somewhere clean.
    """
    spec = BY_NAME["plt_tvl"]
    query = request.query_params
    if any(k in query for k in ("grouping", "window", "from", "to")):
        state = state_for(spec, request)
        return RedirectResponse(
            image_path("plt_tvl", net, state, str(request.url.query)), status_code=308
        )
    return await plt_tvl_image(request, net, state_for(spec, request))


@router.get(
    "/plots/{net}/plt_tvl/{grouping}/{start}/{end}/image.png",
    response_class=Response,
)
async def plt_tvl_plot_at(request: Request, net: str, grouping: str, start: str, end: str):
    spec = BY_NAME["plt_tvl"]
    state = state_from_path(spec, grouping, start, end)
    if state is None:
        raise HTTPException(status_code=404, detail="No such chart view.")
    return await plt_tvl_image(request, net, state)


@router.get(
    "/plots/{net}/plt_tvl/{grouping}/{start}/{end}/{traces}/image.png",
    response_class=Response,
)
async def plt_tvl_plot_at_traces(
    request: Request, net: str, grouping: str, start: str, end: str, traces: str
):
    spec = BY_NAME["plt_tvl"]
    state = state_from_path(spec, grouping, start, end, traces)
    if state is None:
        raise HTTPException(status_code=404, detail="No such chart view.")
    return await plt_tvl_image(request, net, state)


@router.get("/plots/{net}/plt_tvl_{window}", response_class=Response)
@router.get("/plots/{net}/plt_tvl_{window}/image.png", response_class=Response)
async def plt_tvl_legacy(request: Request, net: str, window: str):
    """One route per window became one route with parameters.

    These urls are in Telegram's own file cache and in links people have
    already shared, so they redirect rather than 404.
    """
    target = legacy_redirect_target(net, "plt_tvl", window, request.url.query)
    if target is None:
        raise HTTPException(status_code=404, detail="No such chart.")
    return RedirectResponse(target, status_code=308)
