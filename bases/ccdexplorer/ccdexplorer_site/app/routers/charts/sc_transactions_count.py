import datetime as dt

import pandas as pd
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel


from ccdexplorer.charts import ChartState
from ccdexplorer.ccdexplorer_site.app.routers.charts.images import (
    freq_and_period,
    legacy_redirect_target,
    subtitle_for,
)

from ccdexplorer.ccdexplorer_site.app.routers.statistics import (
    ccdexplorer_plotly_template,
    get_all_data_for_analysis_limited,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    parse_slider_date,
    get_theme_from_request,
    get_url_from_api,
    return_plot_response,
)
from fastapi import HTTPException

router = APIRouter()


class TXCountReportingRequest(BaseModel):
    # net: str
    theme: str
    start_date: str
    end_date: str
    group_by_selection: str
    trace_selection: str
    filename: str


@router.post(
    "/{net}/ajax_transaction_types_reporting",
    response_class=Response,
)
async def ajax_transaction_types_reporting(
    request: Request,
    net: str,
    post_data: TXCountReportingRequest,
):
    post_data.trace_selection = post_data.trace_selection.split(",")
    theme = post_data.theme
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

    start_date_str = post_data.start_date
    end_date_str = post_data.end_date
    parsed_date: dt.datetime = parse_slider_date(post_data.start_date, "start_date")
    post_data.start_date = dt.datetime(parsed_date.year, parsed_date.month, 1).strftime("%Y-%m-%d")

    end_parsed: dt.datetime = parse_slider_date(post_data.end_date, "end_date")
    next_month = dt.datetime(end_parsed.year, end_parsed.month, 1) + relativedelta(months=1)
    last_day = next_month - relativedelta(days=1)
    post_data.end_date = last_day.strftime("%Y-%m-%d")
    analysis = "statistics_mongo_transactions"

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
    fig = build_transactions_count_figure(
        all_data,
        theme=theme,
        freq=letter,
        traces=post_data.trace_selection,
        subtitle=f"{start_date_str} - {end_date_str}",
        per=tooltip,
    )

    # The CSV behind the page's download button. It stays here rather than in
    # the builder: an image route has no filename to write to.
    df = pd.json_normalize(all_data).fillna(0)
    if len(df) > 0:
        df["date"] = pd.to_datetime(df["date"])
        df = (
            df.groupby([pd.Grouper(key="date", freq=letter, label="left", closed="left")])
            .sum()
            .reset_index()
        )
        for key, _label, _colour, columns in TX_CATEGORIES:
            df = add_if_present(key, columns, df)
        present = [t for t in post_data.trace_selection if t in df.columns]
        df = df[["date"] + present]
        df["total_selected"] = df[present].sum(axis=1) if present else 0
        df.to_csv(post_data.filename, index=False)

    return fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )


def add_if_present(grouper_name: str, column_names: list[str], df: pd.DataFrame):
    for column_name in column_names:
        if column_name in df.columns:
            if grouper_name not in df.columns:
                df[grouper_name] = 0
            df[grouper_name] += df[column_name]
    return df


class PostData(BaseModel):
    theme: str
    start_date: str
    end_date: str
    group_by_selection: str
    trace_selection: str


@router.post(
    "/{net}/ajax_statistics_standalone/statistics_network_summary_accounts_per_day",
    response_class=Response,
)
async def statistics_network_summary_accounts_per_day_standalone(
    request: Request,
    net: str,
    post_data: PostData,
):
    # theme = await get_theme_from_request(request)
    theme = post_data.theme
    start_date_str = post_data.start_date
    end_date_str = post_data.end_date
    parsed_date: dt.datetime = parse_slider_date(post_data.start_date, "start_date")
    post_data.start_date = dt.datetime(parsed_date.year, parsed_date.month, 1).strftime("%Y-%m-%d")

    end_parsed: dt.datetime = parse_slider_date(post_data.end_date, "end_date")
    next_month = dt.datetime(end_parsed.year, end_parsed.month, 1) + relativedelta(months=1)
    last_day = next_month - relativedelta(days=1)
    post_data.end_date = last_day.strftime("%Y-%m-%d")
    analysis = "statistics_network_summary"
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
        letter = "W"
        tooltip = "Week"
    if post_data.group_by_selection == "monthly":
        letter = "ME"
        tooltip = "Month"

    if post_data.trace_selection != "cis5":
        all_data = await get_all_data_for_analysis_limited(
            analysis, request.app, post_data.start_date, post_data.end_date
        )

        df_per_day = pd.DataFrame(all_data)
        df_per_day["d_count_accounts"] = df_per_day["account_count"] - df_per_day[
            "account_count"
        ].shift(+1)

        df_per_day = df_per_day.dropna(subset=["d_count_accounts"])

        df_per_day.rename(
            columns={"d_count_accounts": "growth_native", "account_count": "level"},
            inplace=True,
        )
        df_per_day = df_per_day[["date", "growth_native"]]

    if post_data.trace_selection != "native":
        ###CIS-5 public keys
        api_result = await get_url_from_api(
            f"{request.app.api_url}/v2/mainnet/smart-wallets/public-key-creation/{post_data.start_date}/{post_data.end_date}",
            request.app.httpx_client,
        )
        result = api_result.return_value if api_result.ok else []
        if len(result) == 0:
            df_cis5 = pd.DataFrame(columns=["date", "growth_cis5"])
        else:
            df_cis5 = pd.DataFrame(result)
            df_cis5.rename(
                columns={"_id": "date", "count": "growth_cis5"},
                inplace=True,
            )
    if post_data.trace_selection == "all":
        if len(df_cis5) > 0:
            df_merged = pd.merge(df_per_day, df_cis5, on="date", how="outer").fillna(0)
        else:
            df_merged = df_per_day
    elif post_data.trace_selection == "native":
        df_merged = df_per_day
    elif post_data.trace_selection == "cis5":
        df_merged = df_cis5

    df_merged["date"] = pd.to_datetime(df_merged["date"])
    df_merged = df_merged.groupby([pd.Grouper(key="date", freq=letter)]).sum().reset_index()
    fig = go.Figure()
    if post_data.trace_selection != "cis5":
        fig.add_trace(
            go.Bar(
                x=df_merged["date"].to_list(),
                y=df_merged["growth_native"].to_list(),
                name="Native",
                marker=dict(color="#549FF2"),
            )
        )
    if post_data.trace_selection != "native":
        if "growth_cis5" in df_merged.columns:
            fig.add_trace(
                go.Bar(
                    x=df_merged["date"].to_list(),
                    y=df_merged["growth_cis5"].to_list(),
                    name="CIS-5",
                    marker=dict(color="#AE7CF7"),
                )
            )

    fig.update_xaxes(type="date")
    if post_data.trace_selection == "all":
        title = f"Accounts (Native and CIS-5) Growth per {tooltip}"
    if post_data.trace_selection == "native":
        title = f"Accounts (Native) Growth per {tooltip}"
    if post_data.trace_selection == "cis5":
        title = f"Accounts (CIS-5) Growth per {tooltip}"

    fig.update_layout(
        barmode="stack",
        showlegend=False,
        title=f"<b>{title}</b><br><sup>{start_date_str} - {end_date_str}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )

    return fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )


#: The five categories: the source columns each rolls up, the label it draws
#: under, and its colour. One table instead of five if-blocks and a parallel
#: dict that had to agree with them.
TX_CATEGORIES = (
    (
        "account",
        "Account",
        "#EE9B54",
        ["account_creation", "credential_keys_updated", "credentials_updated"],
    ),
    (
        "transfer",
        "Transfer",
        "#F7D30A",
        [
            "account_transfer",
            "transferred_to_encrypted",
            "transferred_to_public",
            "encrypted_amount_transferred",
            "transferred_with_schedule",
        ],
    ),
    (
        "smart ctr",
        "Smart Contracts",
        "#6E97F7",
        ["contract_initialized", "contract_update_issued", "module_deployed"],
    ),
    (
        "staking",
        "Staking",
        "#F36F85",
        [
            "baker_configured",
            "baker_added",
            "baker_removed",
            "baker_keys_updated",
            "baker_restake_earnings_updated",
            "baker_stake_updated",
            "delegation_configured",
        ],
    ),
    ("register data", "Data", "#AE7CF7", ["data_registered"]),
)


def build_transactions_count_figure(
    all_data: list[dict],
    *,
    theme: str,
    freq: str,
    traces: list[str],
    subtitle: str = "",
    per: str = "",
) -> go.Figure:
    """The figure both the page and the image routes draw.

    `traces` is the page's trace_selection; the image routes pass every
    category. The rollups are the fragile part -- a renamed source column just
    stops contributing and the chart still draws -- so they live in
    TX_CATEGORIES where they can be read at a glance.
    """
    if not all_data:
        return go.Figure(layout={"template": ccdexplorer_plotly_template(theme)})

    df = pd.json_normalize(all_data).fillna(0)
    df["date"] = pd.to_datetime(df["date"])
    df = (
        df.groupby([pd.Grouper(key="date", freq=freq, label="left", closed="left")])
        .sum()
        .reset_index()
    )
    if len(df) == 0:
        return go.Figure(layout={"template": ccdexplorer_plotly_template(theme)})
    df = df.fillna(0)

    for key, _label, _colour, columns in TX_CATEGORIES:
        df = add_if_present(key, columns, df)

    fig = go.Figure()
    for key, label, colour, _columns in TX_CATEGORIES:
        if key in traces and key in df.columns:
            fig.add_trace(
                go.Bar(
                    x=df["date"].to_list(),
                    y=df[key].to_list(),
                    name=label,
                    marker=dict(color=colour),
                )
            )

    chosen = [label for key, label, _c, _cols in TX_CATEGORIES if key in traces]
    # "per Day"/"per Week"/"per Month": what one bar covers. The page has always
    # said so, and a stacked bar chart without it is ambiguous.
    heading = f"Account Transaction Types ({', '.join(chosen)})"
    if per:
        heading += f" per {per}"
    fig.update_layout(
        barmode="stack",
        showlegend=False,
        title=f"<b>{heading}</b><br><sup>{subtitle}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )
    return fig


async def transactions_count_image(request: Request, net: str, state: ChartState):
    """The transaction counts chart at the requested window and grouping."""
    if net != "mainnet":
        raise HTTPException(status_code=404, detail="Transaction counts are mainnet only.")

    all_data = await get_all_data_for_analysis_limited(
        "statistics_mongo_transactions",
        request.app,
        state.start.isoformat(),
        state.end.isoformat(),
    )
    theme = await get_theme_from_request(request)
    freq, period = freq_and_period(state)
    fig = build_transactions_count_figure(
        all_data,
        theme=theme,
        freq=freq,
        traces=[key for key, _l, _c, _cols in TX_CATEGORIES],
        subtitle=subtitle_for(state),
        per=period,
    )
    return await return_plot_response(fig, request, f"Transactions, {state.window.value}")


# The /plots image routes for this chart used to live here. They moved to
# charts/generated.py, which registers the same paths for every spec -- and
# because this router is included first, these shadowed those and won every
# request. The difference showed up as a theme: the generated handler reads
# theme_from_query, which falls back to light for a request carrying neither
# parameter nor cookie, and that is exactly what Telegram sends. These read
# get_theme_from_request, which falls back to dark, so this chart arrived in
# chats as a black rectangle while its neighbours arrived light.
#
# The legacy `<name>_<window>` redirects below stay: generated.py does not
# register those, and the urls are in Telegram's file cache.


@router.get("/plots/{net}/transactions_count_{window}", response_class=Response)
@router.get("/plots/{net}/transactions_count_{window}/image.png", response_class=Response)
async def transactions_count_legacy(request: Request, net: str, window: str):
    """One route per window became one route with parameters.

    These urls are in Telegram's own file cache and in links people have
    already shared, so they redirect rather than 404.
    """
    target = legacy_redirect_target(net, "transactions_count", window, request.url.query)
    if target is None:
        raise HTTPException(status_code=404, detail="No such chart.")
    return RedirectResponse(target, status_code=308)
