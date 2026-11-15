"""Four things the bot's private-chat handlers got wrong at the edges.

* A first name goes into HTML the bot sends. Telegram takes names with
  angle brackets in them, and the reply is then refused -- or says something
  other than what was meant.
* /me from someone the bot does not know raised NameError, and from someone
  with nothing configured tried to send an empty message, which Telegram
  rejects.
* A new account was written to the production users collection by name, where
  every other read and write goes through users_collection() -- so a dev bot
  made accounts in production.
* /wintime made blocking gRPC calls on the event loop, one per account, for
  as long as they took -- and nothing else in the bot moved meanwhile.
"""

import datetime as dt
import threading
from types import SimpleNamespace

import mongomock
import pytest

from ccdexplorer.ccdexplorer_bot.bot import telegram_logic
from ccdexplorer.mongodb import CollectionsUtilities
from ccdexplorer.site_user import AccountForUser, SiteUser

CHAT = 7534089330
SITE = "https://site.example"


class Replies:
    def __init__(self):
        self.html = []
        self.text = []

    async def reply_html(self, text, **kwargs):
        self.html.append(text)

    async def reply_text(self, text, **kwargs):
        self.text.append(text)


class Users:
    """mongomock's bulk_write predates the `sort` pymongo now passes."""

    def __init__(self, collection):
        self._collection = collection

    def __getattr__(self, name):
        return getattr(self._collection, name)

    def bulk_write(self, operations):
        for op in operations:
            self._collection.replace_one(op._filter, op._doc, upsert=op._upsert)


class FakeBot(telegram_logic.Mixin):
    def __init__(self, grpcclient=None):
        client = mongomock.MongoClient().concordium_utilities
        self.db = SimpleNamespace(users=Users(client.users), login_codes=client.login_codes)
        self.users = {}
        self.relayed = []
        # users_v2_prod is deliberately not here: a handler that reaches for
        # it by name has nothing to find.
        self.connections = SimpleNamespace(
            tooter=SimpleNamespace(relay=lambda **kw: self.relayed.append(kw)),
            mongodb=SimpleNamespace(
                utilities={CollectionsUtilities.login_codes: self.db.login_codes}
            ),
            grpcclient=grpcclient,
        )

    def users_collection(self):
        return self.db.users


@pytest.fixture
def bot(monkeypatch):
    monkeypatch.setattr(telegram_logic, "SITE_URL", SITE)
    return FakeBot()


def _update(first_name="Ada"):
    return SimpleNamespace(
        effective_user=SimpleNamespace(
            id=CHAT, first_name=first_name, username="ada", language_code="en"
        ),
        effective_chat=SimpleNamespace(type="private"),
        message=Replies(),
    )


def _context(*args):
    return SimpleNamespace(args=list(args))


NASTY = "<i>Ada</i> & <script>x</script>"


# --- names ----------------------------------------------------------------


async def test_a_returning_users_name_is_escaped(bot):
    user = SiteUser(token="t" * 36, telegram_chat_id=CHAT, first_name=NASTY)
    bot.users[str(CHAT)] = user
    update = _update(NASTY)
    await bot.user_login(update, _context())
    text = update.message.html[0]
    assert "<script>" not in text and "<i>Ada" not in text
    assert "&lt;script&gt;" in text


async def test_a_new_users_name_is_escaped(bot):
    update = _update(NASTY)
    await bot.user_login(update, _context())
    text = update.message.html[0]
    assert "<script>" not in text and "<i>Ada" not in text
    assert "&lt;script&gt;" in text


# --- where a new account is written ----------------------------------------


async def test_a_new_account_goes_to_the_collection_the_bot_uses(bot):
    await bot.user_login(_update(), _context())
    assert bot.db.users.count_documents({"_id": str(CHAT)}) == 1


# --- /me ------------------------------------------------------------------


async def test_me_from_someone_unknown_gets_an_answer_not_a_traceback(bot):
    update = _update()
    await bot.user_me(update, _context())
    assert len(update.message.text) == 1
    assert "/start" in update.message.text[0]


async def test_me_with_nothing_configured_does_not_send_an_empty_message(bot):
    bot.users[str(CHAT)] = SiteUser(token="t" * 36, telegram_chat_id=CHAT, first_name="Ada")
    update = _update()
    await bot.user_me(update, _context())
    assert all(m.strip() for m in update.message.text)
    assert len(update.message.text) == 1  # something is said, and it is not blank


# --- /wintime -------------------------------------------------------------


class SlowNode:
    """Records which thread each blocking call ran on."""

    def __init__(self):
        self.threads = []

    def get_account_info(self, block, account_index):
        self.threads.append(threading.current_thread())
        return SimpleNamespace(stake=SimpleNamespace(baker=SimpleNamespace()))

    def get_baker_earliest_win_time(self, index):
        self.threads.append(threading.current_thread())
        return dt.datetime.now().astimezone(dt.timezone.utc) + dt.timedelta(minutes=5)


async def test_wintime_does_not_block_the_event_loop():
    node = SlowNode()
    bot = FakeBot(grpcclient=node)
    bot.users[str(CHAT)] = SiteUser(
        token="t" * 36,
        telegram_chat_id=CHAT,
        accounts={"12": AccountForUser(account_index=12, label="mine", account_id="x")},
    )
    await bot.user_win_time(_update(), _context())
    assert node.threads, "the node was never asked"
    assert all(t is not threading.main_thread() for t in node.threads)
