import httpx
from ccdexplorer.site_user import SiteUser
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from ccdexplorer.env import environment

from ccdexplorer.ccdexplorer_site.app.state import (
    get_httpx_client,
    get_labeled_accounts,
    get_user_detailsv2,
)

router = APIRouter()


@router.get("/{net}/charts", response_class=HTMLResponse)
async def get_charts_home(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    request.state.api_calls = {}

    user: SiteUser | None = await get_user_detailsv2(request)
    return request.app.templates.TemplateResponse(
        "charts/charts_home.html",
        {
            "request": request,
            "net": net,
            "tags": tags,
            "user": user,
            "env": environment,
        },
    )


@router.get("/{net}/charts-plt", response_class=HTMLResponse)
async def get_plt_charts_home(
    request: Request,
    net: str,
    tags: dict = Depends(get_labeled_accounts),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    request.state.api_calls = {}

    user: SiteUser | None = await get_user_detailsv2(request)
    return request.app.templates.TemplateResponse(
        "charts/plt_home.html",
        {
            "request": request,
            "net": net,
            "tags": tags,
            "user": user,
            "env": environment,
        },
    )
