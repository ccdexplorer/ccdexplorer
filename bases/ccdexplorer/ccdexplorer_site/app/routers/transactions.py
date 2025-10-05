# ruff: noqa: F403, F405, E402, E501, E722, F401

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
# pyright: reportIndexIssue=false
# pyright: reportGeneralTypeIssues=false
# pyright: reportInvalidTypeArguments=false
from ccdexplorer.site_user import SiteUser
from ccdexplorer.grpc_client.CCD_Types import *
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
)
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse


from ccdexplorer.env import *

from ccdexplorer.ccdexplorer_site.app.state import get_user_detailsv2

router = APIRouter()


def get_all_data_for_analysis_and_project(
    analysis: str, project_id: str, mongodb: MongoDB, dates_to_include: list[str]
) -> list[str]:
    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": analysis}},
        {"$match": {"project": project_id}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = mongodb.mainnet[Collections.statistics].aggregate(pipeline)
    return [x for x in result]


def get_all_usecases(mongodb: MongoDB) -> list[str]:
    return {
        x["_id"]: x["display_name"] for x in mongodb.utilities[CollectionsUtilities.usecases].find()
    }


def get_all_projects(mongodb: MongoDB) -> list[str]:
    return {
        x["_id"]: x["display_name"] for x in mongodb.utilities[CollectionsUtilities.projects].find()
    }


class TXCountReportingRequest(BaseModel):
    net: str
    start_date: str
    end_date: str
    usecase_id: str
    group_by: str
    tx_types: list


@router.get("/{net}/transactions-search", response_class=HTMLResponse)
async def transactions_search(
    request: Request,
    net: str,
):
    user: SiteUser | None = await get_user_detailsv2(request)
    return request.app.templates.TemplateResponse(
        "transactions_search/start.html",
        {"request": request, "env": request.app.env, "user": user, "net": net},
    )
