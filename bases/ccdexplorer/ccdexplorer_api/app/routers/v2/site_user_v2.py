"""Internal routes for managing site-user preferences and metadata."""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import APIRouter, Request, Depends, HTTPException, Security
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME, API_URL
from fastapi.security.api_key import APIKeyHeader

from fastapi.responses import JSONResponse
from ccdexplorer.mongodb import (
    MongoMotor,
    CollectionsUtilities,
)
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor
from ccdexplorer.ccdexplorer_api.app.utils import apply_docstring_router_wrappers
from fastapi.encoders import jsonable_encoder
import httpx
import datetime as dt
from ccdexplorer.site_user import SiteUser

router = APIRouter(tags=["Site User"], prefix="/v2", include_in_schema=False)
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)

# Secret fields that must never be serialized out of the API. They live only in
# the stored document and are managed exclusively by the auth endpoints.
SITE_USER_SECRET_FIELDS = ("password", "reset_password_token", "verification_token")
# The only fields a preferences save (``save/user``) is allowed to change.
# Everything else — identity (``token``, ``telegram_chat_id``), credentials
# (``password``, ``*_token``), trust flags (``email_verified``) and the
# separately-verified ``email_address`` — is server-owned and ignored if present
# in the request body, so the endpoint can't be used for mass assignment.
SITE_USER_MUTABLE_FIELDS = (
    "username",
    "first_name",
    "language_code",
    "accounts",
    "contracts",
    "other_notification_preferences",
    "last_seen_on_site",
)


@router.get("/site_user/explanations", response_class=JSONResponse)
async def get_site_user_explanations(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return the explanatory metadata displayed next to preference toggles.

    Args:
        request: FastAPI request context (unused but required).
        mongomotor: Mongo client dependency pointing at the utilities database.
        api_key: API key extracted from the request headers.

    Returns:
        Dictionary keyed by explanation id.
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
    """Fetch the stored site-user profile using the opaque token.

    Args:
        request: FastAPI request context (unused but required).
        token: Site-user token string.
        mongomotor: Mongo client dependency used to query the utilities database.
        api_key: API key extracted from the request headers.

    Returns:
        ``SiteUser`` instance if found.

    Raises:
        HTTPException: If the token cannot be resolved.
    """
    db_to_use = mongomotor.utilities
    try:
        result = await db_to_use[CollectionsUtilities.users_v2_prod].find_one(
            {"token": token},
            # Never expose the password hash or reset/verification tokens. These
            # are projected out so they can't leak even if SiteUser grows fields.
            projection={field: 0 for field in SITE_USER_SECRET_FIELDS},
        )
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
    """Internal helper that fetches the site user via the public endpoint."""
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
    """Persist an updated email address for the site user identified by the token.

    Args:
        request: FastAPI request context used to call helper endpoints.
        user_token: Token that identifies the user.
        response_form: JSON payload sent by the UI containing ``email_address``.
        mongomotor: Mongo client dependency used to persist the user document.
        api_key: API key extracted from the request headers.

    Returns:
        ``True`` once the document has been upserted.

    Raises:
        HTTPException: If the user token cannot be resolved.
    """
    user: SiteUser | None = await get_user(request, user_token)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    response_as_dict = jsonable_encoder(response_form)
    # Update only the email address. A ReplaceOne here would drop the server-only
    # secrets (password hash, reset/verification tokens) that the read endpoint
    # no longer returns and therefore can't be echoed back by the caller.
    await mongomotor.utilities[CollectionsUtilities.users_v2_prod].update_one(
        {"token": str(user.token)},
        {
            "$set": {
                "email_address": response_as_dict["email_address"],
                "last_modified": dt.datetime.now().astimezone(tz=dt.timezone.utc),
            }
        },
        upsert=True,
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
    """Persist user-editable preferences for the site user in the path token.

    The request body is untrusted: identity comes from ``user_token`` (the
    caller's session token in the URL), and only the allow-listed
    ``SITE_USER_MUTABLE_FIELDS`` are applied via ``$set``. Identity, credential
    and trust fields supplied in the body are ignored, so this route cannot be
    used to overwrite ``token``/``password``/``email_verified`` etc. (mass
    assignment).

    Args:
        request: FastAPI request context (unused but required).
        user_token: Token that identifies the user (authoritative identity).
        response_form: JSON payload containing a ``user`` object.
        mongomotor: Mongo client dependency used to persist the document.
        api_key: API key extracted from the request headers.

    Returns:
        ``True`` when the user's preferences are updated, ``False`` if no user
        exists for the token.
    """
    stored = await mongomotor.utilities[CollectionsUtilities.users_v2_prod].find_one(
        {"token": user_token}
    )
    if not stored:
        return False

    payload = jsonable_encoder(response_form).get("user") or {}
    # Validate/normalize through the model (unknown keys are ignored); the path
    # token is the authoritative identity, never the body.
    incoming = SiteUser(**{**payload, "token": user_token})
    incoming_dump = incoming.model_dump(exclude_none=True)

    # Apply only allow-listed fields the caller actually sent. Checking the raw
    # payload (not the model dump) avoids resetting ``accounts``/``contracts`` to
    # their ``{}`` model defaults when the caller omits them.
    update = {
        field: incoming_dump[field]
        for field in SITE_USER_MUTABLE_FIELDS
        if field in payload and field in incoming_dump
    }
    update["last_modified"] = dt.datetime.now().astimezone(tz=dt.timezone.utc)

    await mongomotor.utilities[CollectionsUtilities.users_v2_prod].update_one(
        {"token": user_token},
        {"$set": update},
    )
    return True
