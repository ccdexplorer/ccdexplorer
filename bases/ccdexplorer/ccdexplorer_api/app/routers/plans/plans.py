from urllib.parse import parse_qs

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from pymongo import ReplaceOne, DeleteOne
from ccdexplorer.mongodb import (
    CollectionsUtilities,
)
from ccdexplorer.ccdexplorer_api.app.models import SELECTABLE_PLANS, User
from ccdexplorer.ccdexplorer_api.app.state_getters import get_user_details
import datetime as dt

router = APIRouter(include_in_schema=False)


def chosen_plan(body: bytes) -> str:
    """The plan a form body asks for, if it is one an account may choose.

    No body is the free plan, as it always was. Anything else has to carry a
    `plan` field naming one of the plans on offer; the name is what the rate
    limiter groups a key by, so it is never stored as sent.
    """
    if not body:
        return "free"
    try:
        fields = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    except UnicodeDecodeError:
        fields = {}
    plan = fields.get("plan", [""])[0]
    if plan not in SELECTABLE_PLANS:
        raise HTTPException(status_code=400, detail="Unknown plan.")
    return plan


@router.post(
    "/plans/set",
)
async def plans_set_plan(
    request: Request,
):
    user: User | None = get_user_details(request)
    if not user:
        response = RedirectResponse(url="/auth/login", status_code=200)
        response.headers["HX-Redirect"] = "/auth/login"
        return response

    plan = chosen_plan(await request.body())

    user: User | None = get_user_details(request)
    if user:
        user.plan = plan
        if plan == "free":
            user.plan_end_date = dt.datetime.now().astimezone(dt.UTC) + dt.timedelta(days=365)
        else:
            user.plan_end_date = dt.datetime.now().astimezone(dt.UTC)

        rr = await request.app.motormongo.utilities[CollectionsUtilities.api_users].bulk_write(
            [
                ReplaceOne(
                    {"_id": user.api_account_id},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
        response = RedirectResponse(url="/account", status_code=200)
        response.headers["HX-Redirect"] = "/account"
        return response


@router.post(
    "/plans/reset",
)
async def plans_reset_plan(
    request: Request,
):
    user: User | None = get_user_details(request)
    if not user:
        response = RedirectResponse(url="/auth/login", status_code=200)
        response.headers["HX-Redirect"] = "/auth/login"
        return response

    user.plan = None
    user.plan_end_date = dt.datetime.now().astimezone(dt.UTC)

    rr = await request.app.motormongo.utilities[CollectionsUtilities.api_users].bulk_write(
        [
            ReplaceOne(
                {"_id": user.api_account_id},
                user.model_dump(exclude_none=True),
                upsert=True,
            )
        ]
    )

    # reset redis key
    await request.app.redis.delete(f"v2:*:{user.api_account_id}:day")
    # delete keys for all plans
    result = (
        await request.app.motormongo.utilities_db["api_api_keys"]
        .find({"api_account_id": user.api_account_id})
        .to_list(length=None)
    )

    if len(result) > 0:
        for x in result:
            await request.app.motormongo.utilities_db["api_api_keys"].bulk_write(
                [DeleteOne({"_id": x["_id"]})]
            )

    response = RedirectResponse(url="/", status_code=200)
    response.headers["HX-Redirect"] = "/"
    return response


@router.get("/plans")
async def key_plans_home(request: Request):
    response = RedirectResponse(url="/", status_code=303)
    return response
