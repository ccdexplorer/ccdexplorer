import datetime as dt
import uuid

import pandas as pd
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel


from ccdexplorer.charts import ChartState
from ccdexplorer.charts.registry import BY_NAME

from ccdexplorer.ccdexplorer_site.app.routers.statistics import (
    ccdexplorer_plotly_template,
    get_all_data_for_analysis_limited,
)
from ccdexplorer.ccdexplorer_site.app.utils import parse_slider_date

router = APIRouter()


class PostData(BaseModel):
    theme: str
    start_date: str
    end_date: str
    group_by_selection: str
    trace_selection: str
    filename: str


@router.post(
    "/{net}/ajax_statistics_standalone/statistics_active_addresses",
    response_class=Response,
)
async def statistics_active_addresses(
    request: Request,
    net: str,
    post_data: PostData,
):
    post_data.trace_selection = post_data.trace_selection.split(",")  # type: ignore

    theme = post_data.theme
    start_date_str = post_data.start_date
    end_date_str = post_data.end_date
    parsed_date: dt.datetime = parse_slider_date(post_data.start_date, "start_date")
    post_data.start_date = dt.datetime(parsed_date.year, parsed_date.month, 1).strftime("%Y-%m-%d")

    end_parsed: dt.datetime = parse_slider_date(post_data.end_date, "end_date")
    next_month = dt.datetime(end_parsed.year, end_parsed.month, 1) + relativedelta(months=1)
    last_day = next_month - relativedelta(days=1)
    post_data.end_date = last_day.strftime("%Y-%m-%d")

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
        analysis = "statistics_unique_addresses_v2_daily"
        letter = "D"
        tooltip = "Day"
    if post_data.group_by_selection == "weekly":
        analysis = "statistics_unique_addresses_v2_weekly"
        letter = "W"
        tooltip = "Week"
    if post_data.group_by_selection == "monthly":
        analysis = "statistics_unique_addresses_v2_monthly"
        letter = "ME"
        tooltip = "Month"

    all_data = await get_all_data_for_analysis_limited(
        analysis,  # type: ignore
        request.app,
        post_data.start_date,
        post_data.end_date,  # type: ignore
    )

    df_merged = pd.json_normalize(all_data)  # type: ignore
    df_merged = df_merged.rename(
        columns={
            "unique_impacted_address_count.address": "address",
            "unique_impacted_address_count.contract": "contract",
            "unique_impacted_address_count.public_key": "public_key",
        }
    )

    fig = go.Figure()
    if "address" in post_data.trace_selection:
        fig.add_trace(
            go.Bar(
                x=df_merged["date"].to_list(),
                y=df_merged["address"].to_list(),
                name="Native",
                marker=dict(color="#549FF2"),
            )
        )
    if "contract" in post_data.trace_selection:
        if "contract" in df_merged.columns:
            fig.add_trace(
                go.Bar(
                    x=df_merged["date"].to_list(),
                    y=df_merged["contract"].to_list(),
                    name="Contracts",
                    marker=dict(color="#AE7CF7"),
                )
            )
    if "public_key" in post_data.trace_selection:
        if "public_key" in df_merged.columns:
            fig.add_trace(
                go.Bar(
                    x=df_merged["date"].to_list(),
                    y=df_merged["public_key"].to_list(),
                    name="CIS-5",
                    marker=dict(color="#70B785"),
                )
            )

    fig.update_xaxes(type="date")
    trace_titles = {
        "address": "Native",
        "public_key": "CIS-5",
        "contract": "Contract",
    }

    # Generate title based on selected traces
    selected_traces = post_data.trace_selection
    if len(selected_traces) == 3:
        title = f"Unique Active Addresses (Native, Contract and CIS-5) per {tooltip}"  # type: ignore
    else:
        selected_titles = [
            trace_titles[trace] for trace in selected_traces if trace in trace_titles
        ]
        if len(selected_titles) == 1:
            title = f"Unique Active {selected_titles[0]} Addresses per {tooltip}"  # type: ignore
        else:
            title = f"Unique Active Addresses ({', '.join(selected_titles)}) per {tooltip}"  # type: ignore

    fig.update_layout(
        barmode="stack",
        showlegend=False,
        title=f"<b>{title}</b><br><sup>{start_date_str} - {end_date_str}</sup>",
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )

    # test
    # Remove the "complete" column
    df_merged = df_merged.drop(columns=["complete"])

    # Convert non-date columns to integers
    non_date_columns = df_merged.columns.difference(["date"])
    # Fill NA values with 0
    df_merged = df_merged.fillna(0)
    df_merged[non_date_columns] = df_merged[non_date_columns].astype(int)
    df_merged.to_csv(post_data.filename, index=False)
    return fig.to_html(
        config={"responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
    )
