from fastapi import HTTPException, Request, Response
from fastapi_login import LoginManager
from passlib.context import CryptContext
from ccdexplorer.mongodb import MongoMotor

from ccdexplorer.env import API_URL, LOGIN_SECRET

manager = LoginManager(
    LOGIN_SECRET,  # type: ignore
    "/auth/login",
    use_cookie=True,
    cookie_name="api.ccdexplorer.io",
    use_header=False,
)
pwd_context = CryptContext(schemes=["bcrypt"])


def set_session_cookie(response: Response, token: str) -> None:
    """The api's login cookie, with the flags fastapi_login leaves off.

    HttpOnly was all it set. Secure keeps it off plain http wherever the api
    is served over https (a local http api would never see it come back
    otherwise), and SameSite=Lax means another site's form or script cannot
    make the browser attach it to a POST.
    """
    response.set_cookie(
        key=manager.cookie_name,
        value=token,
        httponly=True,
        secure=str(API_URL).startswith("https://"),
        samesite="lax",
        path="/",
    )


async def require_htmx(request: Request) -> None:
    """Only a request made by the page's own htmx gets to change an account.

    A form or fetch from another site cannot add a custom header without a
    CORS preflight, which this api does not grant for them -- so this is a
    second wall behind SameSite for the routes that change plans and keys.
    """
    if request.headers.get("hx-request") != "true":
        raise HTTPException(status_code=403, detail="Forbidden.")


@manager.user_loader()
async def query_user(api_account_id: str, mongomotor: MongoMotor):
    """ """
    db = mongomotor.utilities_db
    result = await db["api_api_keys"].find({"api_account_id": api_account_id}).to_list(length=1)

    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail=f"User with api_account_id {api_account_id} is not found.",
        )


def hash_password(plaintext: str):
    """
    Hashes the plaintext password using bcrypt

    Args:
        plaintext: The password in plaintext

    Returns:
        The hashed password, including salt and algorithm information
    """
    return pwd_context.hash(plaintext)


def verify_password(plaintext: str, hashed: str):
    """
    Checks the plaintext password against the provided hashed password

    Args:
        plaintext: The password as provided by the user
        hashed: The password as stored in the db

    Returns:
        True if the passwords match
    """

    return pwd_context.verify(plaintext, hashed)
