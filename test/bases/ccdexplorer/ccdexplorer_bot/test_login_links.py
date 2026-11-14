"""What the bot says when asked for a way in.

/login used to answer with a link holding the account token, in whatever chat
the command was typed. In a group that posted a permanent login to everyone
in it, and Telegram fetched the url for a preview on top.

Now the commands answer in private chats only, the link holds a code that
works once and lapses in minutes, and the preview is off -- a preview is a
GET, and a GET spends the code.
"""

import datetime as dt
import re
from types import SimpleNamespace

import mongomock
import pytest
from telegram import Chat, Message, Update, User

from ccdexplorer.ccdexplorer_bot.bot import telegram_logic
from ccdexplorer.ccdexplorer_bot.commands import command_handlers
from ccdexplorer.mongodb import CollectionsUtilities
from ccdexplorer.site_user import SiteUser
from ccdexplorer.site_user.login_codes import (
    LOGIN_CODE_TTL,
    LoginCodePurpose,
    code_document,
    new_code,
    redeem_filter,
)

CHAT = 7534089330
ACCOUNT_TOKEN = "3f0c1d6e-0000-4000-8000-000000000001"
SITE = "https://site.example"


class Replies:
    def __init__(self):
        self.sent = []

    async def reply_html(self, text, **kwargs):
        self.sent.append((text, kwargs))


class Users:
    """mongomock's bulk_write predates the `sort` pymongo now passes with a
    ReplaceOne, so that one call is carried out here; the rest is mongomock."""

    def __init__(self, collection):
        self._collection = collection

    def __getattr__(self, name):
        return getattr(self._collection, name)

    def bulk_write(self, operations):
        for op in operations:
            self._collection.replace_one(op._filter, op._doc, upsert=op._upsert)


class FakeBot(telegram_logic.Mixin):
    def __init__(self):
        db = SimpleNamespace(
            users=Users(mongomock.MongoClient().concordium_utilities.users),
            login_codes=mongomock.MongoClient().concordium_utilities.login_codes,
        )
        self.db = db
        self.users = {}
        self.connections = SimpleNamespace(
            tooter=SimpleNamespace(relay=lambda **kwargs: None),
            mongodb=SimpleNamespace(
                utilities={
                    CollectionsUtilities.users_v2_prod: db.users,
                    CollectionsUtilities.login_codes: db.login_codes,
                }
            ),
        )

    def users_collection(self):
        return self.db.users


@pytest.fixture
def bot(monkeypatch):
    monkeypatch.setattr(telegram_logic, "SITE_URL", SITE)
    return FakeBot()


def _update(chat_type="private"):
    return SimpleNamespace(
        effective_user=SimpleNamespace(
            id=CHAT, first_name="Ada", username="ada", language_code="en"
        ),
        effective_chat=SimpleNamespace(type=chat_type),
        message=Replies(),
    )


def _context(*args):
    return SimpleNamespace(args=list(args))


def _existing_user(bot):
    user = SiteUser(token=ACCOUNT_TOKEN, telegram_chat_id=CHAT, first_name="Ada")
    bot.users[str(CHAT)] = user
    bot.db.users.insert_one({"_id": str(CHAT), **user.model_dump(exclude_none=True)})
    return user


def _code_in(text):
    return re.search(rf"{re.escape(SITE)}/token/([A-Za-z0-9_-]+)", text).group(1)


# --- where the commands answer ----------------------------------------------


def _telegram_update(chat_type, text):
    chat = Chat(id=CHAT if chat_type == "private" else -100123, type=chat_type)
    message = Message(
        message_id=1,
        date=dt.datetime.now(dt.timezone.utc),
        chat=chat,
        text=text,
        from_user=User(id=CHAT, first_name="Ada", is_bot=False),
    )
    return Update(update_id=1, message=message)


def test_all_four_commands_are_registered():
    handlers = command_handlers(SimpleNamespace(user_login=1, user_win_time=2, user_me=3))
    assert sorted(c for h in handlers for c in h.commands) == ["login", "me", "start", "wintime"]


@pytest.mark.parametrize("chat_type", ["group", "supergroup", "channel"])
def test_no_command_answers_outside_a_private_chat(chat_type):
    handlers = command_handlers(SimpleNamespace(user_login=1, user_win_time=2, user_me=3))
    update = _telegram_update(chat_type, "/login")
    assert [bool(h.filters.check_update(update)) for h in handlers] == [False] * 4


def test_every_command_answers_in_a_private_chat():
    handlers = command_handlers(SimpleNamespace(user_login=1, user_win_time=2, user_me=3))
    update = _telegram_update("private", "/login")
    assert [bool(h.filters.check_update(update)) for h in handlers] == [True] * 4


async def test_login_in_a_group_says_nothing_even_if_it_gets_there(bot):
    """The filter is what keeps it out; this is for the day someone registers
    a handler without one."""
    _existing_user(bot)
    update = _update("supergroup")

    await bot.user_login(update, _context())

    assert update.message.sent == []
    assert bot.db.login_codes.count_documents({}) == 0


async def test_start_in_a_group_creates_no_account(bot):
    update = _update("group")

    await bot.user_login(update, _context())

    assert bot.db.users.count_documents({}) == 0


# --- what the link holds ------------------------------------------------------


async def test_the_login_link_does_not_hold_the_account_token(bot):
    _existing_user(bot)
    update = _update()

    await bot.user_login(update, _context())

    text, _ = update.message.sent[0]
    assert ACCOUNT_TOKEN not in text
    assert f"{SITE}/token/" in text


async def test_the_login_link_holds_a_code_that_names_the_account(bot):
    _existing_user(bot)
    update = _update()

    await bot.user_login(update, _context())

    code = _code_in(update.message.sent[0][0])
    stored = bot.db.login_codes.find_one(redeem_filter(code, LoginCodePurpose.LOGIN))
    assert stored["user_token"] == ACCOUNT_TOKEN


async def test_asking_twice_gives_two_different_links(bot):
    _existing_user(bot)
    first, second = _update(), _update()

    await bot.user_login(first, _context())
    await bot.user_login(second, _context())

    assert _code_in(first.message.sent[0][0]) != _code_in(second.message.sent[0][0])


async def test_the_link_is_sent_without_a_preview(bot):
    """Telegram builds a preview by fetching the url, which would spend the
    code before the person it was meant for clicked it."""
    _existing_user(bot)
    update = _update()

    await bot.user_login(update, _context())

    _, kwargs = update.message.sent[0]
    assert kwargs["link_preview_options"].is_disabled is True


async def test_the_message_says_the_link_is_short_lived(bot):
    _existing_user(bot)
    update = _update()

    await bot.user_login(update, _context())

    assert "once" in update.message.sent[0][0]


async def test_a_new_account_gets_a_code_too(bot):
    update = _update()

    await bot.user_login(update, _context())

    created = bot.db.users.find_one({"telegram_chat_id": CHAT})
    text, kwargs = update.message.sent[0]
    assert created["token"] not in text
    stored = bot.db.login_codes.find_one(redeem_filter(_code_in(text), LoginCodePurpose.LOGIN))
    assert stored["user_token"] == created["token"]
    assert kwargs["link_preview_options"].is_disabled is True


# --- connecting Telegram to an account made by email ------------------------


def _email_account(bot):
    bot.db.users.insert_one(
        {"_id": "email-1", "token": ACCOUNT_TOKEN, "email_address": "a@example.com"}
    )


def _link_code(bot, now=None, purpose=LoginCodePurpose.LINK):
    code = new_code()
    bot.db.login_codes.insert_one(code_document(code, ACCOUNT_TOKEN, purpose, now))
    return code


async def test_a_linking_code_connects_the_chat(bot):
    _email_account(bot)
    update = _update()

    await bot.user_login(update, _context(_link_code(bot)))

    assert bot.db.users.find_one({"token": ACCOUNT_TOKEN})["telegram_chat_id"] == CHAT
    assert bot.db.users.count_documents({}) == 1


async def test_a_linking_code_is_spent_by_linking(bot):
    _email_account(bot)

    await bot.user_login(_update(), _context(_link_code(bot)))

    assert bot.db.login_codes.count_documents({"purpose": "link"}) == 0


async def test_the_reply_to_linking_does_not_hold_the_account_token(bot):
    _email_account(bot)
    update = _update()

    await bot.user_login(update, _context(_link_code(bot)))

    text, kwargs = update.message.sent[0]
    assert ACCOUNT_TOKEN not in text
    assert kwargs["link_preview_options"].is_disabled is True


async def test_the_account_token_no_longer_links_a_chat(bot):
    """It was the deep-link secret, printed in the settings page's html.
    Anyone who had read it once could attach their own Telegram to the
    account and be sent a login."""
    _email_account(bot)
    update = _update()

    await bot.user_login(update, _context(ACCOUNT_TOKEN))

    assert "telegram_chat_id" not in bot.db.users.find_one({"token": ACCOUNT_TOKEN})
    assert "invalid or has expired" in update.message.sent[0][0]
    assert bot.db.users.count_documents({}) == 1


async def test_a_lapsed_linking_code_links_nothing(bot):
    _email_account(bot)
    long_ago = dt.datetime.now(dt.timezone.utc) - LOGIN_CODE_TTL - dt.timedelta(minutes=1)
    update = _update()

    await bot.user_login(update, _context(_link_code(bot, now=long_ago)))

    assert "telegram_chat_id" not in bot.db.users.find_one({"token": ACCOUNT_TOKEN})
    assert "invalid or has expired" in update.message.sent[0][0]


async def test_a_login_code_does_not_link_a_chat(bot):
    _email_account(bot)
    update = _update()

    await bot.user_login(update, _context(_link_code(bot, purpose=LoginCodePurpose.LOGIN)))

    assert "telegram_chat_id" not in bot.db.users.find_one({"token": ACCOUNT_TOKEN})
