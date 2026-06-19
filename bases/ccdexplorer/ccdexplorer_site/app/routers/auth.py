# ruff: noqa: F403, F405, E402, E501
# pyright: reportOptionalMemberAccess=false
"""Email/password auth pages for the site.

These are thin wrappers: every action is delegated to the API ``/site-auth/*``
endpoints (the site has no DB/email of its own). On success we set the existing
``access-token`` cookie, exactly like the Telegram ``/token/{token}`` login.
"""

import datetime as dt

from fastapi import APIRouter, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from ccdexplorer.ccdexplorer_site.app.utils import post_url_from_api, get_url_from_api

router = APIRouter()


def set_login_cookie(response: Response, token: str) -> None:
    """Attach the 30-day ``access-token`` cookie used for site login."""
    expires = dt.datetime.now().astimezone(dt.UTC) + dt.timedelta(days=30)
    response.set_cookie(
        key="access-token",
        value=token,
        expires=expires.strftime("%a, %d %b %Y %H:%M:%S GMT"),
    )


def _ctx(request: Request, **extra) -> dict:
    context = {"env": request.app.env, "request": request, "net": "mainnet"}
    context.update(extra)
    return context


def _error_of(api_response) -> str:
    """Pull a human-readable message out of a FastAPI error response."""
    detail = None
    if isinstance(api_response.return_value, dict):
        detail = api_response.return_value.get("detail")
    return detail or "Something went wrong. Please try again."


# --------------------------------------------------------------------------- #
# Login
# --------------------------------------------------------------------------- #
@router.get("/auth/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return request.app.templates.TemplateResponse(request, "auth/login.html", _ctx(request))


@router.post("/auth/login")
async def login_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    api_response = await post_url_from_api(
        f"{request.app.api_url}/site-auth/login",
        request.app.httpx_client,
        {"email": email, "password": password},
    )
    if api_response.ok:
        response = RedirectResponse(url="/settings/user/overview", status_code=303)
        set_login_cookie(response, api_response.return_value["token"])
        return response
    return request.app.templates.TemplateResponse(
        request, "auth/login.html", _ctx(request, email=email, error=_error_of(api_response))
    )


# --------------------------------------------------------------------------- #
# Register
# --------------------------------------------------------------------------- #
@router.get("/auth/register", response_class=HTMLResponse)
async def register_get(request: Request):
    return request.app.templates.TemplateResponse(request, "auth/register.html", _ctx(request))


@router.post("/auth/register")
async def register_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    api_response = await post_url_from_api(
        f"{request.app.api_url}/site-auth/register",
        request.app.httpx_client,
        {"email": email, "password": password},
    )
    if api_response.ok:
        return request.app.templates.TemplateResponse(
            request, "auth/register_sent.html", _ctx(request, email=email)
        )
    return request.app.templates.TemplateResponse(
        request, "auth/register.html", _ctx(request, email=email, error=_error_of(api_response))
    )


@router.get("/auth/verify-email/{verification_token}")
async def verify_email(request: Request, verification_token: str):
    api_response = await get_url_from_api(
        f"{request.app.api_url}/site-auth/verify-email/{verification_token}",
        request.app.httpx_client,
    )
    if api_response.ok:
        response = RedirectResponse(url="/settings/user/overview", status_code=303)
        set_login_cookie(response, api_response.return_value["token"])
        return response
    return request.app.templates.TemplateResponse(
        request, "auth/login.html", _ctx(request, error=_error_of(api_response))
    )


# --------------------------------------------------------------------------- #
# Forgot / reset password
# --------------------------------------------------------------------------- #
@router.get("/auth/forgot-password", response_class=HTMLResponse)
async def forgot_password_get(request: Request):
    return request.app.templates.TemplateResponse(
        request, "auth/forgot_password.html", _ctx(request)
    )


@router.post("/auth/forgot-password")
async def forgot_password_post(request: Request, email: str = Form(...)):
    await post_url_from_api(
        f"{request.app.api_url}/site-auth/forgot-password",
        request.app.httpx_client,
        {"email": email},
    )
    # Always show the same confirmation (no account enumeration).
    return request.app.templates.TemplateResponse(
        request, "auth/forgot_password.html", _ctx(request, sent=True)
    )


@router.get("/auth/reset-password/{reset_password_token}", response_class=HTMLResponse)
async def reset_password_get(request: Request, reset_password_token: str):
    return request.app.templates.TemplateResponse(
        request,
        "auth/reset_password.html",
        _ctx(request, reset_password_token=reset_password_token),
    )


@router.post("/auth/reset-password")
async def reset_password_post(
    request: Request,
    reset_password_token: str = Form(...),
    password: str = Form(...),
):
    api_response = await post_url_from_api(
        f"{request.app.api_url}/site-auth/reset-password",
        request.app.httpx_client,
        {"reset_password_token": reset_password_token, "password": password},
    )
    if api_response.ok:
        response = RedirectResponse(url="/settings/user/overview", status_code=303)
        set_login_cookie(response, api_response.return_value["token"])
        return response
    return request.app.templates.TemplateResponse(
        request,
        "auth/reset_password.html",
        _ctx(request, reset_password_token=reset_password_token, error=_error_of(api_response)),
    )


# --------------------------------------------------------------------------- #
# Resend verification email (logged-in user with an unverified address)
# --------------------------------------------------------------------------- #
@router.post("/auth/resend-verification", response_class=HTMLResponse)
async def resend_verification(request: Request):
    token = request.cookies.get("access-token")
    if not token:
        return '<span class="sm-text text-danger">Please log in first.</span>'
    api_response = await post_url_from_api(
        f"{request.app.api_url}/site-auth/{token}/resend-verification",
        request.app.httpx_client,
        {},
    )
    if api_response.ok:
        return (
            '<span class="sm-text text-success">Verification email sent. '
            "Please check your inbox.</span>"
        )
    error = "Couldn't send the verification email. Please try again."
    if isinstance(api_response.return_value, dict):
        error = api_response.return_value.get("detail", error)
    return f'<span class="sm-text text-danger">{error}</span>'


# --------------------------------------------------------------------------- #
# Set password (logged-in Telegram-first user adding email/password login)
# --------------------------------------------------------------------------- #
@router.post("/auth/set-password")
async def set_password_post(
    request: Request,
    password: str = Form(...),
    email: str = Form(None),
):
    token = request.cookies.get("access-token")
    if not token:
        return RedirectResponse(url="/auth/login", status_code=303)

    payload = {"password": password}
    if email:
        payload["email"] = email
    api_response = await post_url_from_api(
        f"{request.app.api_url}/site-auth/{token}/set-password",
        request.app.httpx_client,
        payload,
    )
    if api_response.ok and api_response.return_value.get("needs_verification"):
        return request.app.templates.TemplateResponse(
            request, "auth/register_sent.html", _ctx(request, email=email)
        )
    if api_response.ok:
        response = RedirectResponse(url="/settings/user/overview", status_code=303)
        return response
    return request.app.templates.TemplateResponse(
        request,
        "auth/message.html",
        _ctx(request, message=_error_of(api_response)),
    )
