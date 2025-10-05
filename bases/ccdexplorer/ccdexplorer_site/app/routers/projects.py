# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
# pyright: reportOptionalOperand=false
# pyright: reportOptionalIterable=false
# pyright: reportCallIssue=false
# pyright: reportReturnType=false

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse


from ccdexplorer.ccdexplorer_site.app.state import get_httpx_client
from ccdexplorer.ccdexplorer_site.app.utils import get_url_from_api
import httpx

router = APIRouter()


@router.get("/project/{project_id}", response_class=HTMLResponse)
async def get_project_route(
    request: Request,
    project_id: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
):
    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/mainnet/misc/projects/{project_id}",
        httpx_client,
    )
    project = api_result.return_value if api_result.ok else []

    api_result = await get_url_from_api(
        f"{request.app.api_url}/v2/mainnet/misc/projects/{project_id}/addresses",
        httpx_client,
    )
    mainnet_project_addresses = api_result.return_value if api_result.ok else []

    mainnet_accounts = [x for x in mainnet_project_addresses if x["type"] == "account_address"]
    mainnet_contracts = [x for x in mainnet_project_addresses if x["type"] == "contract_address"]
    if project:
        return request.app.templates.TemplateResponse(
            "projects/project_overview.html",
            {
                "request": request,
                "env": request.app.env,
                "project": project,
                "project_id": project_id,
                "display_name": project["display_name"],
                "mainnet_accounts": mainnet_accounts,
                "mainnet_contracts": mainnet_contracts,
                "net": "mainnet",
            },
        )


# @router.get("/projects", response_class=HTMLResponse)
# async def get_projects_route(
#     request: Request,
#     mongodb: MongoDB = Depends(get_mongo_db),
# ):
#     projects = get_all_project_ids(mongodb)
#     return request.app.templates.TemplateResponse(
#         "projects/projects.html",
#         {
#             "request": request,
#             "env": request.app.env,
#             "projects": projects,
#             "net": "mainnet",
#         },
#     )
