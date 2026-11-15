import datetime as dt
from typing import Optional
from uuid import uuid4


from ccdexplorer.ccdexplorer_api.app.models import User
from ccdexplorer.ccdexplorer_api.app.routers.site_auth.site_auth import (
    EMAIL_MAX_PER_IP,
    EMAIL_MAX_PER_WINDOW,
    EMAIL_WINDOW_SECONDS,
    LOGIN_MAX_ATTEMPTS,
    LOGIN_MAX_ATTEMPTS_PER_IP,
    LOGIN_WINDOW_SECONDS,
    _record_attempt,
    _reset_attempts,
    _too_many_attempts,
)
from ccdexplorer.ccdexplorer_api.app.security import (
    hash_password,
    manager,
    set_session_cookie,
    verify_password,
)
from ccdexplorer.ccdexplorer_api.app.state_getters import get_mongo_motor, get_user_details
from ccdexplorer.env import API_NET, API_URL, environment
from ccdexplorer.mongodb import MongoMotor
from ccdexplorer.tooter import Tooter, is_acceptable_email_address
from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from pymongo import ReplaceOne
import ipaddress

tooter = Tooter()
motormongo = MongoMotor(tooter, caller_name="api_auth")


router = APIRouter(prefix="/auth", include_in_schema=False)


def get_session() -> MongoMotor:
    return motormongo


async def get_user_by_email(email: str, session) -> Optional[User]:
    """ """
    if isinstance(session, MongoMotor):
        motormongo = session
    else:
        motormongo = session()
    db = motormongo.utilities_db
    result = await db["api_users"].find({"email": email}).to_list(length=1)
    if result:
        return User(**result[0])
    else:
        return None


async def get_user_by_reset_password_token(reset_password_token: str, session) -> Optional[User]:
    """ """
    if isinstance(session, MongoMotor):
        motormongo = session
    else:
        motormongo = session()
    db = motormongo.utilities_db
    result = (
        await db["api_users"].find({"reset_password_token": reset_password_token}).to_list(length=1)
    )
    if not result:
        return None
    user = User(**result[0])
    # No expiry on record means it was issued before there were any.
    expires = user.reset_password_token_expires
    if expires is None or expires.astimezone(dt.UTC) <= dt.datetime.now(dt.UTC):
        return None
    return user


@manager.user_loader(session=get_session)
async def get_user(name: str, session):
    return await get_user_by_email(name, session)


#: How long a reset link works. Long enough to find the email, short enough
#: that an old mailbox is not a standing way into the account.
RESET_TOKEN_LIFETIME = dt.timedelta(hours=1)


#: Addresses a hop between us and the visitor has. Spelled out rather than
#: `is_private`, which also covers the documentation ranges and says nothing
#: about which of them are ours.
_OWN_HOPS = [
    ipaddress.ip_network(n)
    for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "169.254.0.0/16")
    + ("::1/128", "fc00::/7", "fe80::/10")
]


def _is_our_own_hop(address) -> bool:
    return any(address.version == n.version and address in n for n in _OWN_HOPS)


def caller_ip(request: Request) -> str:
    """Who is asking: the address the proxy in front of us wrote.

    The api is reached by browsers directly, through one proxy, so read
    X-Forwarded-For from the right: entries further left are the client's to
    choose. Hops of our own (private addresses) are skipped; anything that is
    not an address falls back to the peer, since this becomes a throttle key.
    """
    peer = request.client.host if request.client else ""
    forwarded = request.headers.get("x-forwarded-for", "")
    if not forwarded:
        return peer
    entries = []
    for part in forwarded.split(","):
        try:
            entries.append(ipaddress.ip_address(part.strip()))
        except ValueError:
            return peer
    for address in reversed(entries):
        if not _is_our_own_hop(address):
            return str(address)
    return str(entries[-1])


TOO_MANY = "Too many attempts. Please try again later."


def _refused(request: Request, template: str, extra: dict | None = None):
    context = {"request": request, "env": environment, "errors": [TOO_MANY], **(extra or {})}
    return request.app.state.templates.TemplateResponse(request, template, context, status_code=429)


@router.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    user = get_user_details(request)
    context = {"request": request, "env": environment, "user": user}
    return request.app.state.templates.TemplateResponse(request, "auth/login.html", context)


@router.get("/register", response_class=HTMLResponse)
def register_get(request: Request):
    context = {
        "request": request,
        "env": environment,
    }
    return request.app.state.templates.TemplateResponse(request, "auth/register.html", context)


@router.post("/login")
async def login(
    request: Request,
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: MongoMotor = Depends(get_mongo_motor),
) -> RedirectResponse:
    """
    Logs in the user provided by form_data.username and form_data.password
    """
    ip = caller_ip(request)
    email = form_data.username
    if await _too_many_attempts(
        request, "api-login-email", email, LOGIN_MAX_ATTEMPTS
    ) or await _too_many_attempts(request, "api-login-ip", ip, LOGIN_MAX_ATTEMPTS_PER_IP):
        return _refused(request, "auth/login.html")

    user = await get_user_by_email(email, db)
    if user is None or not verify_password(form_data.password, user.password):
        # The same answer either way, and both count: an unknown address is
        # exactly what someone working through a list is trying.
        await _record_attempt(request, "api-login-email", email, LOGIN_WINDOW_SECONDS)
        await _record_attempt(request, "api-login-ip", ip, LOGIN_WINDOW_SECONDS)
        error = "Can't find user and/or password is wrong. "
        context = {"request": request, "env": environment, "errors": [error]}
        return request.app.state.templates.TemplateResponse(request, "auth/login.html", context)  # type: ignore

    await _reset_attempts(request, "api-login-email", email)
    response = RedirectResponse(url="/account", status_code=303)
    set_session_cookie(response, user.token)
    return response


@router.get("/forgot-password")
async def forgot_password(
    request: Request,
    response: Response,
    db: MongoMotor = Depends(get_mongo_motor),
) -> HTMLResponse:
    """
    The user has forgotten its password. Ask for email and send a reset email.
    """

    context = {
        "request": request,
        "env": environment,
    }
    return request.app.state.templates.TemplateResponse(
        request, "auth/forgot_password.html", context
    )


@router.get("/reset-password-action/{reset_password_token}")
async def reset_password_action(
    request: Request,
    reset_password_token: str,
    response: Response,
    db: MongoMotor = Depends(get_mongo_motor),
) -> HTMLResponse:
    """
    The user has forgotten its password and clicked on the link in the email.
    """
    user = await get_user_by_reset_password_token(reset_password_token, db)
    if not user:
        response = RedirectResponse(url="/auth/login", status_code=303)
        return response  # type: ignore

    context = {
        "request": request,
        "env": environment,
        "reset_password_token": reset_password_token,
    }
    return request.app.state.templates.TemplateResponse(
        request, "auth/reset_password.html", context
    )


@router.post("/reset-password")
async def forgot_password_action(
    request: Request,
    response: Response,
    db: MongoMotor = Depends(get_mongo_motor),
    username: str = Form(""),
) -> HTMLResponse:
    """
    The user has forgotten its password. Ask for email and send a reset email.
    """
    # Parsed as a form, so "+" and "%2B" in an address are what was typed.
    user = None
    username = username.strip().lower()

    # Counted whether or not the address has an account, and answered the same
    # either way, so the limit cannot be used to tell which ones do.
    ip = caller_ip(request)
    limited = await _too_many_attempts(
        request, "api-reset-email", username or "", EMAIL_MAX_PER_WINDOW
    ) or await _too_many_attempts(request, "api-reset-ip", ip, EMAIL_MAX_PER_IP)
    await _record_attempt(request, "api-reset-email", username or "", EMAIL_WINDOW_SECONDS)
    await _record_attempt(request, "api-reset-ip", ip, EMAIL_WINDOW_SECONDS)
    if username and not limited:
        user = await get_user_by_email(username, db)

    if user:
        user.reset_password_token = str(uuid4())
        user.reset_password_token_expires = dt.datetime.now(dt.UTC) + RESET_TOKEN_LIFETIME
        await db.utilities_db["api_users"].bulk_write(
            [
                ReplaceOne(
                    {"_id": user.api_account_id},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
        request.app.tooter.email_api(
            title="CCDExplorer.io API - Forgot password",
            body=f"""Someone has clicked the 'Reset Password' link on {API_URL} for your account. If this was you and you need to reset your password, please click <a href='{API_URL}/auth/reset-password-action/{user.reset_password_token}'>Reset Password</a>.
            If this wasn't you, please ignore this email.""",
            email_address=user.email,
        )

    context = {
        "request": request,
        "env": environment,
    }
    return request.app.state.templates.TemplateResponse(
        request, "auth/forgot_password_action.html", context
    )


@router.post("/set-password")
async def set_new_password_after_forgot(
    request: Request,
    response: Response,
    db: MongoMotor = Depends(get_mongo_motor),
    # The form's password input is named "username" (it shares the login
    # form's styling); the alias keeps the page as it is.
    password: str = Form("", alias="username"),
    reset_password_token: str = Form(""),
) -> HTMLResponse:
    """
    The user has forgotten its password. Asks for email and clicked on the link. Now sets new password.
    """
    user = None
    if password and reset_password_token:
        user = await get_user_by_reset_password_token(reset_password_token, db)

    if user:
        assert password is not None
        user.password = hash_password(password)
        # A new password retires every cookie made with the old token.
        user.token = str(uuid4())
        user.reset_password_token = None
        user.reset_password_token_expires = None
        await db.utilities_db["api_users"].bulk_write(
            [
                ReplaceOne(
                    {"_id": user.api_account_id},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
        request.app.tooter.email_api(
            title="CCDExplorer.io API - Reset password",
            body=f"""The password on your account on {API_URL} was just reset. If this wasn't you, please contact me on Telegram (explorer.ccd) at once.""",
            email_address=user.email,
        )

    response = RedirectResponse(url="/auth/login", status_code=303)
    return response  # type: ignore


#: How many aliases past the highest in use a claim will try before giving up.
ALIAS_CLAIM_ATTEMPTS = 25


async def claim_alias(db: MongoMotor, api_account_id: str) -> tuple[int, str]:
    """Take the next free alias for an account, atomically.

    The alias was the highest in use plus one, read and then used: two
    registrations at once read the same number and shared one alias -- and
    with it each other's payments, which are found by alias. A claim is a
    single update that only succeeds on an alias nobody holds, so the loser
    of a race simply moves on to the next.

    Running out is an answer, not an IndexError.
    """
    pipeline = [{"$sort": {"alias_id": -1}}, {"$limit": 1}]
    top = await (await db.utilities_db["api_users"].aggregate(pipeline)).to_list(length=1)
    start = int(top[0]["alias_id"]) + 1 if top else 0
    aliases = db.utilities_db["api_aliases"]
    for candidate in range(start, start + ALIAS_CLAIM_ATTEMPTS):
        claimed = await aliases.find_one_and_update(
            {"net": API_NET, "alias_id": candidate, "claimed_by": {"$exists": False}},
            {"$set": {"claimed_by": api_account_id}},
        )
        if claimed:
            return candidate, claimed["alias"]
        if not await aliases.find_one({"net": API_NET, "alias_id": candidate}):
            break  # past the last alias that exists
    raise HTTPException(
        status_code=503, detail="No account addresses are available right now. Please try later."
    )


@router.post("/register", status_code=201)
async def register(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: MongoMotor = Depends(get_mongo_motor),
):
    """
    Registers a new user
    """
    if not is_acceptable_email_address(form_data.username):
        # Before anything is looked up or stored: the forgot-password mail
        # goes to whatever address the account was created with.
        context = {
            "request": request,
            "env": environment,
            "errors": ["please enter a valid email address."],
        }
        return request.app.state.templates.TemplateResponse(request, "auth/register.html", context)
    ip = caller_ip(request)
    if await _too_many_attempts(request, "api-register-ip", ip, EMAIL_MAX_PER_IP):
        return _refused(request, "auth/register.html")
    await _record_attempt(request, "api-register-ip", ip, EMAIL_WINDOW_SECONDS)
    user = await get_user_by_email(form_data.username, db)
    if user is None:
        api_account_id = str(uuid4())
        next_alias_id, alias_account_id = await claim_alias(db, api_account_id)
        user = User(
            scope=API_URL,  # type: ignore
            api_account_id=api_account_id,
            alias_id=next_alias_id,
            alias_account_id=alias_account_id,
            token=str(uuid4()),
            email=form_data.username,
            password=hash_password(form_data.password),
            plan_end_date=dt.datetime.now().astimezone(dt.UTC),
        )
        await db.utilities_db["api_users"].bulk_write(
            [
                ReplaceOne(
                    {"_id": user.api_account_id},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
        response = RedirectResponse(url="/account", status_code=303)
        set_session_cookie(response, user.token)
        return response

    else:
        context = {
            "request": request,
            "env": environment,
            "errors": ["email address already registered."],
        }
    return request.app.state.templates.TemplateResponse(request, "auth/register.html", context)


@router.get("/logout")
async def logout_get():
    """Nothing happens on a GET.

    A link, an image tag or a redirect on another site is a GET, and signing
    someone out is not something they should be able to trigger.
    """
    return RedirectResponse(url="/", status_code=303)


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    db: MongoMotor = Depends(get_mongo_motor),
):
    """End the session -- on the server too.

    The cookie holds the account's token, so deleting it only forgets it on
    this browser; anyone who copied it could still use it. Retiring the token
    makes a copy worth nothing, which does sign out the account's other
    browsers as well.
    """
    user = get_user_details(request)
    if user:
        user.token = str(uuid4())
        await db.utilities_db["api_users"].bulk_write(
            [
                ReplaceOne(
                    {"_id": user.api_account_id},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("api.ccdexplorer.io", path="/")
    request.app.user = None
    return response
