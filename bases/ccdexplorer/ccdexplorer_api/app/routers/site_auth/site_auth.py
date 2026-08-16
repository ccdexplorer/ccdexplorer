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
import json
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from pymongo import ReplaceOne

from ccdexplorer.ccdexplorer_api.app.ratelimiting import require_api_auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password, verify_password
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor, site_users_collection_name
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
    session_id: Optional[str] = None


class SetEmailRequest(BaseModel):
    email: str


class ResolveSessionRequest(BaseModel):
    session_token: str


class RevokeOtherSessionsRequest(BaseModel):
    keep_session_id: Optional[str] = None


class CreateSessionRequest(BaseModel):
    # Sent by the site (not read from this request's own headers): the caller
    # here is the site's backend, not the browser, so its own User-Agent would
    # be the site's httpx client, not the visitor's.
    user_agent: Optional[str] = None


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
    result = await mongomotor.utilities[site_users_collection_name()].find_one({field: value})
    if result:
        return SiteUser(**result)
    return None


async def save_user(user: SiteUser, mongomotor: MongoMotor) -> None:
    """Upsert the SiteUser keyed on its token (preserves the existing ``_id``)."""
    user.last_modified = dt.datetime.now().astimezone(dt.timezone.utc)
    await mongomotor.utilities[site_users_collection_name()].bulk_write(
        [
            ReplaceOne(
                {"token": str(user.token)},
                user.model_dump(exclude_none=True),
                upsert=True,
            )
        ]
    )


# --- Sessions ----------------------------------------------------------- #
# ``SiteUser.token`` stays exactly what it always was: the account's stable
# identity (Telegram linking, the Mongo lookup/replace key, every existing
# `/site-auth/{token}/...` call). The browser cookie no longer holds that
# value directly -- it holds a *session* token from this collection, one per
# logged-in device, so individual devices can be listed/revoked without
# touching the account's identity.
#
# The session token rotates on every use (at most once per ROTATE_THROTTLE),
# the same idea OAuth refresh-token rotation uses. The value it replaces stays
# valid for SESSION_GRACE_PERIOD purely to absorb concurrent in-flight
# requests that started just before a rotation (e.g. a page firing a couple of
# near-simultaneous HTMX calls on the same cookie) -- using it during the
# grace window doesn't rotate again, it just converges back onto the current
# token. Presenting a token *older* than that is treated as a stolen-cookie
# signal: the session is revoked and an audit entry is written.
ROTATE_THROTTLE = dt.timedelta(seconds=30)
SESSION_GRACE_PERIOD = dt.timedelta(seconds=10)


def _device_label(user_agent: Optional[str]) -> Optional[str]:
    """Coarse "Browser on OS" label from a User-Agent string, e.g. "Chrome on
    macOS" or "Safari on iPhone". Deliberately approximate (order-sensitive
    substring checks, not a full UA-parsing library) -- this is only meant to
    help someone recognize a device in their own session list, not to
    fingerprint or precisely identify one.
    """
    if not user_agent:
        return None
    ua = user_agent

    if "iPhone" in ua:
        os_label = "iPhone"
    elif "iPad" in ua:
        os_label = "iPad"
    elif "Android" in ua:
        os_label = "Android"
    elif "Mac OS X" in ua:
        os_label = "macOS"
    elif "Windows" in ua:
        os_label = "Windows"
    elif "Linux" in ua:
        os_label = "Linux"
    else:
        os_label = None

    # Order matters: Edge/Chrome UAs also contain "Safari", and Chrome's UA
    # also contains "OPR" only for Opera -- check the more specific tokens first.
    if "Edg/" in ua:
        browser = "Edge"
    elif "OPR/" in ua or "Opera" in ua:
        browser = "Opera"
    elif "Chrome" in ua:
        browser = "Chrome"
    elif "Firefox" in ua:
        browser = "Firefox"
    elif "Safari" in ua:
        browser = "Safari"
    else:
        browser = None

    if browser and os_label:
        return f"{browser} on {os_label}"
    return browser or os_label


def _as_utc(value: Optional[dt.datetime]) -> Optional[dt.datetime]:
    """Mongo may hand back naive datetimes; treat those as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value


async def create_session(
    user_token: str, mongomotor: MongoMotor, user_agent: Optional[str] = None
) -> dict:
    """Mint a new session (one per logged-in device) for ``user_token``.

    No IP address or other network identifiers are captured or stored -- just
    a rotating bearer token, its lifecycle timestamps, which account it
    belongs to, and (if provided) a coarse "Browser on OS" device label
    derived from the User-Agent header, so an account owner can tell sessions
    apart on the "Active sessions" list.
    """
    now = _now()
    doc = {
        "_id": str(uuid4()),
        "token": str(uuid4()),
        "previous_token": None,
        "previous_token_expires": None,
        "user_token": user_token,
        "created_at": now,
        "last_seen_at": now,
        "last_rotated_at": now,
        "revoked": False,
        "revoked_reason": None,
        "device": _device_label(user_agent),
    }
    await mongomotor.utilities[CollectionsUtilities.user_sessions].insert_one(doc)
    return doc


async def _resolved(
    user_token: str, session_id: str, session_token: str, mongomotor: MongoMotor
) -> dict:
    user = await get_site_user_by_field("token", user_token, mongomotor)
    if user is None:
        return {"ok": False, "reason": "invalid"}
    return {
        "ok": True,
        "session_id": session_id,
        "session_token": session_token,
        "user": json.loads(user.model_dump_json()),
    }


async def resolve_session(session_token: str, mongomotor: MongoMotor) -> dict:
    """Authenticate a session cookie value, rotating it if it's due."""
    sessions = mongomotor.utilities[CollectionsUtilities.user_sessions]
    now = _now()

    doc = await sessions.find_one({"token": session_token, "revoked": False})
    if doc is not None:
        last_rotated = _as_utc(doc.get("last_rotated_at")) or _as_utc(doc["created_at"])
        if now - last_rotated >= ROTATE_THROTTLE:
            new_token = str(uuid4())
            await sessions.update_one(
                {"_id": doc["_id"], "token": session_token},
                {
                    "$set": {
                        "token": new_token,
                        "previous_token": session_token,
                        "previous_token_expires": now + SESSION_GRACE_PERIOD,
                        "last_rotated_at": now,
                        "last_seen_at": now,
                    }
                },
            )
            returned_token = new_token
        else:
            await sessions.update_one(
                {"_id": doc["_id"]},
                {"$set": {"last_seen_at": now}},
            )
            returned_token = session_token
        return await _resolved(doc["user_token"], doc["_id"], returned_token, mongomotor)

    doc = await sessions.find_one({"previous_token": session_token, "revoked": False})
    if doc is not None:
        if _expiry_ok(_as_utc(doc.get("previous_token_expires"))):
            # In-flight request racing a rotation that already happened --
            # converge on the current token instead of treating this as reuse.
            return await _resolved(doc["user_token"], doc["_id"], doc["token"], mongomotor)
        await sessions.update_one(
            {"_id": doc["_id"]},
            {"$set": {"revoked": True, "revoked_reason": "reuse_detected"}},
        )
        await _append_audit_entry(doc["user_token"], "session_reuse_detected", mongomotor)
        return {"ok": False, "reason": "revoked"}

    # Migration fallback: an old-style cookie holding a raw SiteUser.token
    # directly (pre-dates the session layer). Treat it as an implicit login
    # instead of forcing a mass logout on deploy.
    user = await get_site_user_by_field("token", session_token, mongomotor)
    if user is not None:
        session = await create_session(user.token, mongomotor)
        return await _resolved(user.token, session["_id"], session["token"], mongomotor)

    return {"ok": False, "reason": "invalid"}


async def list_user_sessions(user_token: str, mongomotor: MongoMotor) -> list[dict]:
    cursor = (
        mongomotor.utilities[CollectionsUtilities.user_sessions]
        .find({"user_token": user_token, "revoked": False})
        .sort("last_seen_at", -1)
    )
    return [doc async for doc in cursor]


async def revoke_session(
    user_token: str, session_id: str, reason: str, mongomotor: MongoMotor
) -> None:
    await mongomotor.utilities[CollectionsUtilities.user_sessions].update_one(
        {"_id": session_id, "user_token": user_token},
        {"$set": {"revoked": True, "revoked_reason": reason}},
    )


async def revoke_other_sessions(
    user_token: str,
    keep_session_id: Optional[str],
    reason: str,
    mongomotor: MongoMotor,
) -> None:
    query: dict = {"user_token": user_token, "revoked": False}
    if keep_session_id:
        query["_id"] = {"$ne": keep_session_id}
    await mongomotor.utilities[CollectionsUtilities.user_sessions].update_many(
        query, {"$set": {"revoked": True, "revoked_reason": reason}}
    )


def _iso_utc(value: Optional[dt.datetime]) -> Optional[str]:
    """Serialize as an unambiguous UTC ISO string (explicit +00:00).

    Real MongoDB drivers hand back naive datetimes for UTC-stored values; a
    bare ``.isoformat()`` on those loses the "this is UTC" marker, and
    downstream code that calls ``.astimezone()`` on a naive datetime silently
    reinterprets it as the *server's local time* instead -- a real bug, not
    just a display nit. Normalizing to aware-UTC before serializing avoids it
    regardless of what timezone the server process happens to run in.
    """
    value = _as_utc(value)
    return value.isoformat() if value else None


def _session_public(doc: dict) -> dict:
    return {
        "session_id": doc["_id"],
        "created_at": _iso_utc(doc.get("created_at")),
        "last_seen_at": _iso_utc(doc.get("last_seen_at")),
        "device": doc.get("device"),
    }


# --- Login / session audit trail ----------------------------------------- #
# Deliberately no IP address, user agent, or other device/network identifiers
# -- this log exists only so an account owner can see *that* something
# happened (a login, a suspected stolen cookie) and *when*, not track devices.
async def _append_audit_entry(
    user_token: Optional[str],
    event: str,
    mongomotor: MongoMotor,
) -> None:
    """Best-effort audit log write -- never let this break an auth flow."""
    if not user_token:
        return
    try:
        await mongomotor.utilities[CollectionsUtilities.login_audit_log].insert_one(
            {
                "user_token": user_token,
                "event": event,
                "at": _now(),
            }
        )
    except Exception:
        pass


async def get_audit_log(user_token: str, mongomotor: MongoMotor, limit: int = 10) -> list[dict]:
    cursor = (
        mongomotor.utilities[CollectionsUtilities.login_audit_log]
        .find({"user_token": user_token})
        .sort("at", -1)
        .limit(limit)
    )
    return [doc async for doc in cursor]


def _audit_public(doc: dict) -> dict:
    return {
        "event": doc.get("event"),
        "at": _iso_utc(doc.get("at")),
    }


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
        if user is not None:
            await _append_audit_entry(user.token, "login_failed", mongomotor)
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if not user.email_verified:
        raise HTTPException(status_code=403, detail="Please verify your email address first.")
    await _reset_attempts(request, "login", body.email)
    await _append_audit_entry(user.token, "login_success", mongomotor)
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
    # A password reset proves mailbox ownership, not device ownership -- kick
    # every existing session (including any that still has the old password's
    # session cookie) and let this flow issue a fresh one.
    await revoke_other_sessions(user.token, None, "password_changed", mongomotor)
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

    # Setting/changing a password from the settings page is done from a
    # logged-in session -- keep that one device signed in, kick every other.
    await revoke_other_sessions(user.token, body.session_id, "password_changed", mongomotor)

    if needs_verification:
        send_verification_email(request, user)

    return {"ok": True, "needs_verification": needs_verification}


# --------------------------------------------------------------------------- #
# Sessions ("see sessions" / "log out everywhere")
# --------------------------------------------------------------------------- #
@router.post("/{token}/sessions", response_class=JSONResponse)
async def create_session_endpoint(
    token: str,
    body: CreateSessionRequest = CreateSessionRequest(),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Mint a session for account ``token`` -- called right after a successful
    login/register/verify-email/reset-password, so the browser cookie can hold
    a rotating session token instead of the account's identity token."""
    user = await get_site_user_by_field("token", token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    session = await create_session(user.token, mongomotor, user_agent=body.user_agent)
    return {"session_id": session["_id"], "session_token": session["token"]}


@router.post("/sessions/resolve", response_class=JSONResponse)
async def resolve_session_endpoint(
    body: ResolveSessionRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Authenticate + (if due) rotate a session token. Called once per site
    request by the site's session middleware."""
    return await resolve_session(body.session_token, mongomotor)


@router.get("/{token}/sessions", response_class=JSONResponse)
async def list_sessions_endpoint(
    token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """List active sessions/devices for the settings page."""
    sessions = await list_user_sessions(token, mongomotor)
    return {"sessions": [_session_public(s) for s in sessions]}


@router.delete("/{token}/sessions/{session_id}", response_class=JSONResponse)
async def revoke_session_endpoint(
    token: str,
    session_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Revoke a single session (e.g. from the "Active sessions" list)."""
    await revoke_session(token, session_id, "user_revoked", mongomotor)
    return {"ok": True}


@router.post("/{token}/sessions/revoke-others", response_class=JSONResponse)
async def revoke_other_sessions_endpoint(
    token: str,
    body: RevokeOtherSessionsRequest,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """"Log out of all other devices"."""
    await revoke_other_sessions(token, body.keep_session_id, "logout_others", mongomotor)
    await _append_audit_entry(token, "logout_others", mongomotor)
    return {"ok": True}


@router.get("/{token}/audit-log", response_class=JSONResponse)
async def audit_log_endpoint(
    token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Recent login/session activity for the settings page."""
    entries = await get_audit_log(token, mongomotor)
    return {"entries": [_audit_public(e) for e in entries]}


# --------------------------------------------------------------------------- #
# Account lifecycle: deletion + data export
# --------------------------------------------------------------------------- #
@router.delete("/{token}", response_class=JSONResponse)
async def delete_account(
    token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Permanently delete a SiteUser and every session belonging to it."""
    user = await get_site_user_by_field("token", token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    await mongomotor.utilities[site_users_collection_name()].delete_one({"token": token})
    await mongomotor.utilities[CollectionsUtilities.user_sessions].delete_many(
        {"user_token": token}
    )
    return {"ok": True}


def _prune_disabled_notifications(prefs: Optional[dict]) -> dict:
    """From a {field_name: NotificationPreferences|null} mapping, keep only
    entries that have at least one channel actually turned on, and within
    those, only the enabled channel(s). Used to keep the data export focused
    on features the user has actually enabled instead of the full skeleton of
    every possible per-event/per-channel toggle (almost all left at their
    default "off")."""
    if not prefs:
        return {}
    pruned: dict = {}
    for key, value in prefs.items():
        if not value:
            continue
        enabled_channels = {
            channel: service
            for channel in ("telegram", "email")
            if (service := value.get(channel)) and service.get("enabled")
        }
        if enabled_channels:
            pruned[key] = enabled_channels
    return pruned


@router.get("/{token}/export", response_class=JSONResponse)
async def export_account(
    token: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """The account's data as a downloadable JSON document, secrets stripped
    and notification preferences pruned down to what's actually enabled."""
    user = await get_site_user_by_field("token", token, mongomotor)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    data = json.loads(
        user.model_dump_json(
            exclude={"password", "reset_password_token", "verification_token"}
        )
    )

    data["other_notification_preferences"] = _prune_disabled_notifications(
        data.get("other_notification_preferences")
    )
    for account in (data.get("accounts") or {}).values():
        account["account_notification_preferences"] = _prune_disabled_notifications(
            account.get("account_notification_preferences")
        )
        account["validator_notification_preferences"] = _prune_disabled_notifications(
            account.get("validator_notification_preferences")
        )
    for contract in (data.get("contracts") or {}).values():
        contract_update_issued = (contract.get("contract_notification_preferences") or {}).get(
            "contract_update_issued"
        )
        contract["contract_notification_preferences"] = {
            "contract_update_issued": _prune_disabled_notifications(contract_update_issued)
        }

    return JSONResponse(content=data)
