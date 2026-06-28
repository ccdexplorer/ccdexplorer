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

from ccdexplorer.ccdexplorer_api.app.ratelimiting import require_api_auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password, verify_password
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor
from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from ccdexplorer.env import environment
from ccdexplorer.mongodb import CollectionsUtilities, MongoMotor
from ccdexplorer.site_user import SiteUser

# ``require_api_auth`` validates the key against the registered API keys and 401s
# otherwise. The per-endpoint ``Security(API_KEY_HEADER)`` below only checks that
# the header is *present*, so this router-level dependency is what actually
# authenticates these server-to-server auth endpoints.
router = APIRouter(
    prefix="/site-auth",
    tags=["Site Auth"],
    include_in_schema=False,
    dependencies=[Depends(require_api_auth)],
)
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)

SITE_URL = environment["SITE_URL"]

# --- Brute-force / abuse throttling (fixed window, Redis-backed) ------------- #
# The site proxies every call under one shared API key, so the real client IP is
# not visible here; the target email is the meaningful throttle key. All helpers
# fail open if Redis is unavailable so authentication never hard-breaks.
LOGIN_MAX_ATTEMPTS = 10  # failed logins per email...
LOGIN_WINDOW_SECONDS = 15 * 60  # ...within 15 minutes
EMAIL_MAX_PER_WINDOW = 5  # reset/registration emails per email...
EMAIL_WINDOW_SECONDS = 60 * 60  # ...within 1 hour


def _throttle_key(bucket: str, key: str) -> str:
    return f"site-auth-throttle:{bucket}:{(key or '').strip().lower()}"


async def _too_many_attempts(request: Request, bucket: str, key: str, limit: int) -> bool:
    """Return True once `key` has reached `limit` within the current window."""
    if not key:
        return False
    try:
        value = await request.app.r.get(_throttle_key(bucket, key))
        current = int(value) if value is not None else 0
    except Exception:
        return False  # Redis down → don't lock users out
    return current >= limit


async def _record_attempt(request: Request, bucket: str, key: str, window_seconds: int) -> None:
    """Increment the counter for `key`, setting the window TTL on the first hit."""
    if not key:
        return
    try:
        count = await request.app.r.incr(_throttle_key(bucket, key))
        if count == 1:
            await request.app.r.expire(_throttle_key(bucket, key), window_seconds)
    except Exception:
        pass


async def _reset_attempts(request: Request, bucket: str, key: str) -> None:
    """Clear the counter (e.g. after a successful login)."""
    if not key:
        return
    try:
        await request.app.r.delete(_throttle_key(bucket, key))
    except Exception:
        pass


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


# --- Short-lived token lifecycle (reset / email verification) --------------- #
# These are single-use links emailed to the user; they must expire so a leaked
# link (mailbox access, logs) can't be replayed indefinitely.
RESET_TOKEN_TTL = dt.timedelta(hours=1)
VERIFICATION_TOKEN_TTL = dt.timedelta(hours=24)


def _now() -> dt.datetime:
    return dt.datetime.now().astimezone(dt.timezone.utc)


def _issue_reset_token(user: SiteUser) -> None:
    user.reset_password_token = str(uuid4())
    user.reset_password_token_expires = _now() + RESET_TOKEN_TTL


def _issue_verification_token(user: SiteUser) -> None:
    user.verification_token = str(uuid4())
    user.verification_token_expires = _now() + VERIFICATION_TOKEN_TTL


def _expiry_ok(expires: Optional[dt.datetime]) -> bool:
    """True only if `expires` is a future timestamp.

    A missing expiry (token issued before expiry existed) is treated as expired
    so that no non-expiring link stays usable. Mongo may return naive datetimes;
    those are assumed to be UTC.
    """
    if expires is None:
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=dt.timezone.utc)
    return _now() <= expires


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
    # Throttle to prevent verification-email bombing / registration spam.
    if await _too_many_attempts(request, "register", body.email, EMAIL_MAX_PER_WINDOW):
        raise HTTPException(
            status_code=429, detail="Too many attempts. Please try again later."
        )
    await _record_attempt(request, "register", body.email, EMAIL_WINDOW_SECONDS)
    existing = await get_site_user_by_field("email_address", body.email, mongomotor)
    if existing:
        raise HTTPException(status_code=409, detail="Email address already registered.")

    user = SiteUser(
        token=str(uuid4()),
        email_address=body.email,
        password=hash_password(body.password),
        email_verified=False,
        last_modified=_now(),
    )
    _issue_verification_token(user)
    await save_user(user, mongomotor)
    send_verification_email(request, user)
    return {"ok": True, "needs_verification": True}


@router.post("/login", response_class=JSONResponse)
async def login(
    request: Request,
    body: LoginRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Verify email/password and return the SiteUser token on success."""
    if await _too_many_attempts(request, "login", body.email, LOGIN_MAX_ATTEMPTS):
        raise HTTPException(
            status_code=429,
            detail="Too many login attempts. Please wait a few minutes and try again.",
        )
    user = await get_site_user_by_field("email_address", body.email, mongomotor)
    if (user is None) or (not user.password) or (not verify_password(body.password, user.password)):
        await _record_attempt(request, "login", body.email, LOGIN_WINDOW_SECONDS)
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if not user.email_verified:
        raise HTTPException(status_code=403, detail="Please verify your email address first.")
    await _reset_attempts(request, "login", body.email)
    return {"token": user.token}


@router.get("/verify-email/{verification_token}", response_class=JSONResponse)
async def verify_email(
    verification_token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Confirm an email address and return the token so the site can log the user in."""
    user = await get_site_user_by_field("verification_token", verification_token, mongomotor)
    if user is None or not _expiry_ok(user.verification_token_expires):
        raise HTTPException(status_code=404, detail="Invalid or expired verification link.")
    user.email_verified = True
    user.verification_token = None
    user.verification_token_expires = None
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
    # Throttle to prevent reset-email bombing. Stay silent (still return ok) so
    # this can't be turned into an enumeration or error oracle either.
    if await _too_many_attempts(request, "forgot", body.email, EMAIL_MAX_PER_WINDOW):
        return {"ok": True}
    await _record_attempt(request, "forgot", body.email, EMAIL_WINDOW_SECONDS)
    user = await get_site_user_by_field("email_address", body.email, mongomotor)
    if user is not None:
        _issue_reset_token(user)
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
    if user is None or not _expiry_ok(user.reset_password_token_expires):
        raise HTTPException(status_code=404, detail="Invalid or expired reset link.")
    user.password = hash_password(body.password)
    user.reset_password_token = None
    user.reset_password_token_expires = None
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
    _issue_verification_token(user)
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

    # Always issue a fresh token so the resent link is valid even if a previous
    # verification token had already expired.
    _issue_verification_token(user)
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
        _issue_verification_token(user)

    await save_user(user, mongomotor)

    if needs_verification:
        send_verification_email(request, user)

    return {"ok": True, "needs_verification": needs_verification}
