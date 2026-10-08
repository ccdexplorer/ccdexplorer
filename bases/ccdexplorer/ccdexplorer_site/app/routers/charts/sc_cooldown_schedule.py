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


def build_cooldown_schedule_figure(per_day: dict[str, int], theme: str) -> go.Figure:
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
    figure = build_cooldown_schedule_figure(per_day, theme_from_query(request))
    return await return_plot_response(figure, request, "Stake leaving cooldown")


@router.get("/plots/{net}/cooldown_schedule", response_class=Response)
@router.get("/plots/{net}/cooldown_schedule/image.png", response_class=Response)
async def cooldown_schedule_plot(request: Request, net: str):
    """The schedule as it stands.

    No window or grouping in the path: there is no history here to select
    from, only what is locked now and when it is released.
    """
    return await cooldown_schedule_image(request, net)
