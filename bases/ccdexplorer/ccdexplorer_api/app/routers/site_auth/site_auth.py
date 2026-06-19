"""Email/password authentication endpoints for the public site (SiteUser).

These mirror the API-account auth flow in ``routers/auth/auth.py`` but operate on
the unified :class:`SiteUser` stored in ``users_v2_prod``. The site
(`ccdexplorer_site`) has no database or email access of its own, so all hashing,
persistence and email sending happens here; the site only calls these endpoints
and sets the ``access-token`` cookie with the returned ``token``.
"""

# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
import datetime as dt
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from pymongo import ReplaceOne

from ccdexplorer.ccdexplorer_api.app.security import hash_password, verify_password
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from ccdexplorer.env import environment
from ccdexplorer.mongodb import CollectionsUtilities, MongoMotor
from ccdexplorer.site_user import SiteUser

router = APIRouter(prefix="/site-auth", tags=["Site Auth"], include_in_schema=False)
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)

SITE_URL = environment["SITE_URL"]


class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    reset_password_token: str
    password: str


class SetPasswordRequest(BaseModel):
    password: str
    email: Optional[str] = None


class SetEmailRequest(BaseModel):
    email: str


async def get_site_user_by_field(field: str, value, mongomotor: MongoMotor) -> Optional[SiteUser]:
    """Return the first SiteUser whose ``field`` equals ``value``, if any."""
    result = await mongomotor.utilities[CollectionsUtilities.users_v2_prod].find_one({field: value})
    if result:
        return SiteUser(**result)
    return None


async def save_user(user: SiteUser, mongomotor: MongoMotor) -> None:
    """Upsert the SiteUser keyed on its token (preserves the existing ``_id``)."""
    user.last_modified = dt.datetime.now().astimezone(dt.timezone.utc)
    await mongomotor.utilities[CollectionsUtilities.users_v2_prod].bulk_write(
        [
            ReplaceOne(
                {"token": str(user.token)},
                user.model_dump(exclude_none=True),
                upsert=True,
            )
        ]
    )


def send_verification_email(request: Request, user: SiteUser) -> None:
    """Email the user a link to confirm ownership of their email address."""
    request.app.tooter.email_api(
        title="CCDExplorer.io - Verify your email",
        body=(
            "Welcome to CCDExplorer! Please confirm your email address by clicking "
            f"<a href='{SITE_URL}/auth/verify-email/{user.verification_token}'>Verify email</a>. "
            "If this wasn't you, please ignore this email."
        ),
        email_address=user.email_address,
    )


@router.post("/register", response_class=JSONResponse)
async def register(
    request: Request,
    body: RegisterRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Create a new email/password SiteUser and send a verification email."""
    existing = await get_site_user_by_field("email_address", body.email, mongomotor)
    if existing:
        raise HTTPException(status_code=409, detail="Email address already registered.")

    user = SiteUser(
        token=str(uuid4()),
        email_address=body.email,
        password=hash_password(body.password),
        verification_token=str(uuid4()),
        email_verified=False,
        last_modified=dt.datetime.now().astimezone(dt.timezone.utc),
    )
    await save_user(user, mongomotor)
    send_verification_email(request, user)
    return {"ok": True, "needs_verification": True}


@router.post("/login", response_class=JSONResponse)
async def login(
    body: LoginRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Verify email/password and return the SiteUser token on success."""
    user = await get_site_user_by_field("email_address", body.email, mongomotor)
    if (user is None) or (not user.password) or (not verify_password(body.password, user.password)):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if not user.email_verified:
        raise HTTPException(status_code=403, detail="Please verify your email address first.")
    return {"token": user.token}


@router.get("/verify-email/{verification_token}", response_class=JSONResponse)
async def verify_email(
    verification_token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Confirm an email address and return the token so the site can log the user in."""
    user = await get_site_user_by_field("verification_token", verification_token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="Invalid or expired verification link.")
    user.email_verified = True
    user.verification_token = None
    await save_user(user, mongomotor)
    return {"token": user.token}


@router.post("/forgot-password", response_class=JSONResponse)
async def forgot_password(
    request: Request,
    body: ForgotPasswordRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Email a reset link if the address is known. Always returns ok (no enumeration)."""
    user = await get_site_user_by_field("email_address", body.email, mongomotor)
    if user is not None:
        user.reset_password_token = str(uuid4())
        await save_user(user, mongomotor)
        request.app.tooter.email_api(
            title="CCDExplorer.io - Reset password",
            body=(
                f"Someone requested a password reset for your account on {SITE_URL}. "
                f"If this was you, click "
                f"<a href='{SITE_URL}/auth/reset-password/{user.reset_password_token}'>Reset password</a>. "
                "If this wasn't you, please ignore this email."
            ),
            email_address=user.email_address,
        )
    return {"ok": True}


@router.post("/reset-password", response_class=JSONResponse)
async def reset_password(
    body: ResetPasswordRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Set a new password from a reset token and return the token to log the user in."""
    user = await get_site_user_by_field("reset_password_token", body.reset_password_token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="Invalid or expired reset link.")
    user.password = hash_password(body.password)
    user.reset_password_token = None
    await save_user(user, mongomotor)
    return {"token": user.token}


@router.post("/{token}/set-email", response_class=JSONResponse)
async def set_email(
    request: Request,
    token: str,
    body: SetEmailRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Set/change a SiteUser's email address and require re-verification.

    Used by the settings page so that every email address ends up verified.
    """
    user = await get_site_user_by_field("token", token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    # Re-saving the same, already-verified address is a no-op.
    if body.email == user.email_address and user.email_verified:
        return {"ok": True, "needs_verification": False}

    other = await get_site_user_by_field("email_address", body.email, mongomotor)
    if other is not None and other.token != user.token:
        raise HTTPException(status_code=409, detail="Email address already in use.")

    user.email_address = body.email
    user.email_verified = False
    user.verification_token = str(uuid4())
    await save_user(user, mongomotor)
    send_verification_email(request, user)
    return {"ok": True, "needs_verification": True}


@router.post("/{token}/resend-verification", response_class=JSONResponse)
async def resend_verification(
    request: Request,
    token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Re-send the verification email for a user's current (unverified) address."""
    user = await get_site_user_by_field("token", token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    if not user.email_address:
        raise HTTPException(status_code=400, detail="No email address to verify.")
    if user.email_verified:
        return {"ok": True, "needs_verification": False}

    if not user.verification_token:
        user.verification_token = str(uuid4())
        await save_user(user, mongomotor)
    send_verification_email(request, user)
    return {"ok": True, "needs_verification": True}


@router.post("/{token}/set-password", response_class=JSONResponse)
async def set_password(
    request: Request,
    token: str,
    body: SetPasswordRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Add/replace a password for an existing (e.g. Telegram-first) SiteUser.

    If a new email is supplied, it is stored and a verification email is sent.
    """
    user = await get_site_user_by_field("token", token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")

    if body.email and (body.email != user.email_address):
        other = await get_site_user_by_field("email_address", body.email, mongomotor)
        if other is not None:
            raise HTTPException(status_code=409, detail="Email address already in use.")
        user.email_address = body.email
        user.email_verified = False

    if not user.email_address:
        raise HTTPException(status_code=400, detail="An email address is required to set a password.")

    user.password = hash_password(body.password)

    # Email/password login requires a verified email. A Telegram-first user may
    # already have an (unverified) email, so confirm ownership before allowing it.
    needs_verification = not user.email_verified
    if needs_verification:
        user.verification_token = str(uuid4())

    await save_user(user, mongomotor)

    if needs_verification:
        send_verification_email(request, user)

    return {"ok": True, "needs_verification": needs_verification}
