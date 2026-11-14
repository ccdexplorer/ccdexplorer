"""One-time codes that stand in for the account token in a link.

SiteUser.token is how an account is found: the key its document is saved
under, what a session and a notification record point at. It was also being
used as a secret -- the bot's login link was /token/<token>, and the settings
page put it in the Telegram deep link -- and a value that identifies an
account for its whole life cannot also be a secret, because it can never be
changed once it has been seen.

A code is what goes in a link instead. It is random, it is deleted by the
first use, and it lapses a few minutes after it was made. Only its hash is
stored, so the collection holds nothing that works as a link.

The bot reaches Mongo with a synchronous client and the api with an
asynchronous one, so nothing here touches a collection: these build the
document and the filters, and each caller runs them its own way.

    issue    insert_one(code_document(code, user_token, purpose))
    redeem   is_live(find_one_and_delete(redeem_filter(code, purpose)))
    sweep    delete_many(expired_filter())
"""

import datetime as dt
import hashlib
import secrets
from enum import Enum

#: Long enough to switch from Telegram to a browser, short enough that a link
#: found later in a chat or a log is already dead.
LOGIN_CODE_TTL = dt.timedelta(minutes=10)


class LoginCodePurpose(str, Enum):
    """What a code may be spent on. One made for one is refused for the other."""

    #: Issued by the bot, redeemed by the site: log this browser in.
    LOGIN = "login"
    #: Issued by the site, redeemed by the bot: attach this chat to the account.
    LINK = "link"


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def new_code() -> str:
    """256 random bits as 43 characters of [A-Za-z0-9_-].

    That alphabet and length are what a Telegram deep link's start parameter
    allows (64 characters), so the same code works in either direction.
    """
    return secrets.token_urlsafe(32)


def code_id(code: str) -> str:
    """What a code is stored under: its SHA-256, never the code."""
    return hashlib.sha256(code.encode()).hexdigest()


def code_document(
    code: str, user_token: str, purpose: LoginCodePurpose, now: dt.datetime | None = None
) -> dict:
    """The document to insert when a code is issued."""
    return {
        "_id": code_id(code),
        "user_token": user_token,
        "purpose": purpose.value,
        "expires_at": (now or _now()) + LOGIN_CODE_TTL,
    }


def redeem_filter(code: str, purpose: LoginCodePurpose) -> dict:
    """Finds the document for `code`, if it was issued for `purpose`."""
    return {"_id": code_id(str(code)), "purpose": purpose.value}


def expired_filter(now: dt.datetime | None = None) -> dict:
    """Every code that has lapsed. Unused codes are otherwise never removed."""
    return {"expires_at": {"$lt": now or _now()}}


def is_live(document: dict | None, now: dt.datetime | None = None) -> bool:
    """Whether a found document may still be spent.

    A missing expiry counts as lapsed, so nothing without one stays usable.
    Mongo hands back naive datetimes; they are UTC.
    """
    if not document:
        return False
    expires = document.get("expires_at")
    if expires is None:
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=dt.timezone.utc)
    return (now or _now()) <= expires
