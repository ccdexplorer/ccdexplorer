from fastapi import HTTPException
from fastapi_login import LoginManager
from passlib.context import CryptContext
from ccdexplorer.mongodb import MongoMotor

from ccdexplorer.env import LOGIN_SECRET

manager = LoginManager(
    LOGIN_SECRET,  # type: ignore
    "/auth/login",
    use_cookie=True,
    cookie_name="api.ccdexplorer.io",
    use_header=False,
)
pwd_context = CryptContext(schemes=["bcrypt"])


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
