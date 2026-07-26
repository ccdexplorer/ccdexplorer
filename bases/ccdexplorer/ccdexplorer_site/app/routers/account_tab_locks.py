import math

import httpx
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ccdexplorer.env import environment

from ccdexplorer.ccdexplorer_site.app.state import get_httpx_client
from ccdexplorer.ccdexplorer_site.app.utils import (
    account_address_is_alias,
    create_dict_for_tabulator_display_for_account_plt_locks,
    get_url_from_api,
)

router = APIRouter()


@router.get("/account/locks-tab-content/{net}/{account_id}", response_class=HTMLResponse)
async def locks_tab_content(
    request: Request,
    net: str,
    account_id: str,
):
    return request.app.templates.get_template("account/account_locks.html").render(
        {
            "net": net,
            "account_id": account_id,
            "env": request.app.env,
        }
    )


@router.get(
    "/ajax_account_plt_locks/{net}/{account_id}",
    response_class=HTMLResponse,
)
async def get_ajax_account_plt_locks(
    request: Request,
    net: str,
    account_id: str,
    page: int = Query(),
    size: int = Query(),
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    """
    PLT locks this account participates in (as creator, controller, funder, or recipient).
    """
    skip = (page - 1) * size
    is_alias = account_address_is_alias(account_id, net, request.app)
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/{net}/account/{account_id}/plt-locks/{skip}/{size}{'/alias' if is_alias else ''}",
        httpx_client,
    )
    api_return_result = api_result.return_value if api_result.ok else None
    if not api_return_result:
        error = f"Request error getting PLT locks for account at {account_id} on {net}."
        return request.app.templates.TemplateResponse(
            request,
            "base/error-request.html",
            {
                "request": request,
                "error": error,
                "env": environment,
                "net": net,
            },
        )

    tb_made_up_rows = [
        create_dict_for_tabulator_display_for_account_plt_locks(net, row)
        for row in api_return_result["data"]
    ]
    total_rows = api_return_result["total_row_count"]
    last_page = math.ceil(total_rows / size)
    return JSONResponse(
        {
            "data": tb_made_up_rows,
            "last_page": max(1, last_page),
            "last_row": total_rows,
        }
    )
