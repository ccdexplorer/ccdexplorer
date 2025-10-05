# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER, API_URL
from fastapi.security.api_key import APIKeyHeader

from fastapi.responses import JSONResponse
from ccdexplorer.mongodb import (
    MongoMotor,
    CollectionsUtilities,
)
from pymongo import ReplaceOne
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor
from fastapi.encoders import jsonable_encoder
import httpx
import datetime as dt
from ccdexplorer.site_user import SiteUser

router = APIRouter(tags=["Site User"], prefix="/v2", include_in_schema=False)
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER)


@router.get("/site_user/explanations", response_class=JSONResponse)
async def get_site_user_explanations(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get explanations for options for site users.

    """
    db_to_use = mongomotor.utilities
    try:
        result = (
            await db_to_use[CollectionsUtilities.preferences_explanations]
            .find({})
            .to_list(length=None)
        )
    except Exception as _:
        result = []

    return {x["_id"]: x for x in result}


@router.get("/site_user/{token}", response_class=JSONResponse)
async def get_site_user_from_token(
    request: Request,
    token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> SiteUser | None:
    """
    Endpoint to get site user from token.

    """
    db_to_use = mongomotor.utilities
    try:
        result = await db_to_use[CollectionsUtilities.users_v2_prod].find_one({"token": token})
        if result:
            return SiteUser(**result)
    except Exception as _:
        result = None

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No user found for {token}.",
        )


async def get_user(request: Request, token: str):
    response = await request.app.httpx_client.get(f"{request.app.api_url}/v2/site_user/{token}")
    if response.status_code == 200:
        return SiteUser(**response.json())
    else:
        return None


@router.put("/site_user/{user_token}/save/email-address", response_class=JSONResponse)
async def post_user_email_address(
    request: Request,
    user_token: str,
    response_form: dict,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """
    Endpoint to update and save user email address.

    """
    user: SiteUser | None = await get_user(request, user_token)
    response_as_dict = jsonable_encoder(response_form)
    user.email_address = response_as_dict["email_address"]
    user.last_modified = dt.datetime.now().astimezone(tz=dt.timezone.utc)
    await mongomotor.utilities[CollectionsUtilities.users_v2_prod].bulk_write(
        [
            ReplaceOne(
                {"token": str(user.token)},
                user.model_dump(exclude_none=True),
                upsert=True,
            )
        ]
    )

    return True


@router.put("/site_user/{user_token}/save/user", response_class=JSONResponse)
async def post_user(
    request: Request,
    user_token: str,
    response_form: dict,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> bool:
    """
    Endpoint to update and save user.

    """
    user: SiteUser | None = await get_user(request, user_token)
    if user:
        response_as_dict = jsonable_encoder(response_form)
        user = SiteUser(**response_as_dict["user"])
        user.last_modified = dt.datetime.now().astimezone(tz=dt.timezone.utc)
        await mongomotor.utilities[CollectionsUtilities.users_v2_prod].bulk_write(
            [
                ReplaceOne(
                    {"token": str(user.token)},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )

        return True
    else:
        return False
