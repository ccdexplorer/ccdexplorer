"""When locked stake comes back, as a chart rather than a column of numbers.

The accounts-cooldown page listed each release moment in a table, so a
reader wanting to know whether anything large is about to unlock had to
read down a column and compare.

Live, not stored: it reads the node's current cooldown state through the
api, so it is the schedule as it stands rather than a history. The
history is the `cooldowns` chart, which answers a different question --
how much was locked on a given day, rather than which day it returns.

It has no spec for that reason: there is no date-keyed collection behind
it and nothing for a grouping or a date range to select.
"""

import datetime as dt

import plotly.graph_objects as go
from fastapi import APIRouter, Request
from fastapi.responses import Response

from ccdexplorer.charts.registry import COOLDOWNS_START
from ccdexplorer.ccdexplorer_site.app.routers.statistics import (
    get_all_data_for_analysis_limited,
)
from ccdexplorer.ccdexplorer_site.app.routers.tools import (
    cooldown_schedule_by_day,
    cooldown_summary,
)
from ccdexplorer.ccdexplorer_site.app.utils import (
    ccdexplorer_plotly_template,
    empty_chart_figure,
    get_url_from_api,
    return_plot_response,
    theme_from_query,
)

router = APIRouter()

#: The blue the other staking charts use for the amount they are about.
BAR_COLOUR = "#549FF2"

#: microCCD to CCD, the unit the axis is labelled in.
MICRO_CCD = 1_000_000


def average_daily_release(history: list[dict]) -> float | None:
    """What a day's release usually looks like, in microCCD, or None.

    Nothing records what was released. What is recorded is how much stood
    in cooldown at the end of each day, so a fall in that balance is stake
    coming back and by how much.

    An approximation, and it understates: stake entering cooldown lifts the
    balance on the same day, which hides part of a release that happened
    alongside it. A rise is not counted as a negative release, or new
    entries would net the average down twice over.

    Averaged over every day, not only the days something moved. Most days
    nothing is released, and a mean over the 312 days that did would
    describe a rarer event and sit well above the bars.
    """
    totals = [
        (row["date"], row["total_amount"]) for row in history if row.get("total_amount") is not None
    ]
    if len(totals) < 2:
        return None

    releases = [max(0, before - after) for (_, before), (_, after) in zip(totals, totals[1:])]
    return sum(releases) / len(releases)


def build_cooldown_schedule_figure(
    per_day: dict[str, int], theme: str, average: float | None = None
) -> go.Figure:
    """One bar per day on which stake leaves cooldown."""
    if not per_day:
        # Not an empty pair of axes, which reads as "nothing happens ever"
        # rather than "nothing is locked right now".
        return empty_chart_figure(theme, "No stake is in cooldown")

    days = [dt.date.fromisoformat(d) for d in per_day]
    amounts = [v / MICRO_CCD for v in per_day.values()]

    figure = go.Figure(
        data=[
            go.Bar(
                x=days,
                y=amounts,
                name="Released",
                marker=dict(color=BAR_COLOUR),
                hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f} CCD<extra></extra>",
            )
        ]
    )
    if average:
        # Said against the bars because it is the same measurement: CCD
        # released on a day. The bars say when stake comes back; this says
        # whether that is a lot.
        figure.add_hline(
            y=average / MICRO_CCD,
            line=dict(color="#8A8F98", width=1, dash="dash"),
            annotation_text=f"average day: {average / MICRO_CCD:,.0f} CCD",
            annotation_position="top left",
            annotation_font=dict(size=11, color="#8A8F98"),
        )

    figure.update_xaxes(type="date", title=None)
    figure.update_yaxes(title="CCD released")
    figure.update_layout(
        title=(
            "<b>Stake leaving cooldown</b><br>"
            f"<sup>{sum(amounts):,.0f} CCD locked, as at "
            f"{dt.date.today().isoformat()}</sup>"
        ),
        showlegend=False,
        template=ccdexplorer_plotly_template(theme),
        height=400,
    )
    return figure


async def cooldown_schedule_image(request: Request, net: str):
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/accounts/cooldown",
        request.app.httpx_client,
    )
    accounts = api_result.return_value if api_result.ok else []
    per_day = cooldown_schedule_by_day(cooldown_summary(accounts))

    # The whole stored history, so the reference line is what a day has
    # usually looked like rather than what the last few weeks did.
    history = await get_all_data_for_analysis_limited(
        "statistics_cooldowns",
        request.app,
        COOLDOWNS_START.isoformat(),
        dt.date.today().isoformat(),
    )
    figure = build_cooldown_schedule_figure(
        per_day, theme_from_query(request), average_daily_release(history)
    )
    return await return_plot_response(figure, request, "Stake leaving cooldown")


@router.get("/plots/{net}/cooldown_schedule", response_class=Response)
@router.get("/plots/{net}/cooldown_schedule/image.png", response_class=Response)
async def cooldown_schedule_plot(request: Request, net: str):
    """The schedule as it stands.

    No window or grouping in the path: there is no history here to select
    from, only what is locked now and when it is released.
    """
    return await cooldown_schedule_image(request, net)
