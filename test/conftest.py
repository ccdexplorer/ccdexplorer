# tests/conftest.py
# ruff: noqa: E402
import asyncio
import os

from dotenv import load_dotenv

load_dotenv()  # populate os.environ from .env before any ccdexplorer import below

# factory.py calls sentry_sdk.init() at import time, gated only by comparing
# SITE_URL to a literal string — fragile, and a real Sentry DSN in the ambient
# environment would make every test run (including its intentional error
# paths, like the smoke test hitting 404/500s) report as production incidents.
# Force this off before factory (or anything importing it) is ever imported.
os.environ["SENTRY_DSN"] = ""
os.environ["API_URL"] = "http://testserver"

# MongoDB/MongoMotor (components/ccdexplorer/mongodb/core.py) import MONGO_URI
# directly at module load time, so it must be pointed at the test instance
# before ccdexplorer.mongodb is ever imported (below) — a plain env var, not
# a constructor argument, is the only thing that reaches it in time. Points
# at a separate Mongo instance so test runs never contend with production for
# connections on the shared MONGO_URI host again.
if os.environ.get("TEST_MONGO_URI"):
    os.environ["MONGO_URI"] = os.environ["TEST_MONGO_URI"]
    # TEST_MONGO_URI is a secondary in the same replica set as production's
    # primary (not an independent instance): it rejects PRIMARY-affinity reads
    # server-side (NotPrimaryError), which is what MongoDB()/MongoMotor()
    # normally use. secondaryPreferred lets it actually serve reads.
    os.environ["MONGO_READ_PREFERENCE"] = "secondaryPreferred"

from pathlib import Path

import httpx2 as httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from ccdexplorer.ccdexplorer_api.app import factory
from ccdexplorer.ccdexplorer_api.app.factory import AppSettings
from ccdexplorer.ccdexplorer_bot.bot import Bot, Connections
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter.core import Tooter

import importlib

import ccdexplorer.env

importlib.reload(ccdexplorer.env)  # need to reload to pick up changed env vars
# Set environment variables for testing

# `Bot.users` (bot/__init__.py:read_users_from_collection) is keyed by telegram_chat_id,
# not by the seed document's _id/username/token. This is the telegram_chat_id on the
# users_v2_dev seed doc {"_id": "user_for_test", "username": "user_for_test", ...}.
TEST_USER_CHAT_ID = "913126895"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    try:
        yield loop
    finally:
        # give pending tasks a moment to cancel/finish if your teardown schedules any
        if not loop.is_closed():
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()


@pytest.fixture(scope="session")
def tooter() -> Tooter:
    """Provide a Tooter instance."""
    return Tooter()


@pytest.fixture(scope="session")
def motormongo(tooter) -> MongoMotor:
    """Provide a MongoMotor using the Tooter."""
    return MongoMotor(tooter, nearest=True, caller_name=__name__)


@pytest.fixture(scope="session")
def mongodb(tooter) -> MongoDB:
    """Provide a MongoDB instance using the Tooter."""
    return MongoDB(tooter, caller_name=__name__)


@pytest.fixture(scope="session")
def grpcclient() -> GRPCClient:
    """Provide a fresh GRPCClient instance."""
    return GRPCClient()


@pytest.fixture(scope="session")
def httpx_client() -> httpx.Client:
    """Provide a fresh httpx instance."""
    return httpx.Client()


@pytest.fixture(scope="session")
def bot(grpcclient: GRPCClient, mongodb: MongoDB, tooter: Tooter):
    bot = Bot(Connections(tooter=tooter, mongodb=mongodb, mongomoter=None, grpcclient=grpcclient))
    bot.do_initial_reads_from_collections()
    return bot


@pytest.fixture(autouse=True)
def _reset_bot_activity_state(request):
    """Reset the session-scoped bot's per-run activity state before each test.

    `bot` is session-scoped (do_initial_reads_from_collections() does live Mongo
    reads, too expensive to redo per test), but tests append to bot.event_queue
    and then index into it (bot.event_queue[0], [1], ...). Without a reset,
    events left over from earlier tests in the same session shift those indices
    onto the wrong event.
    """
    if "bot" in request.fixturenames:
        bot = request.getfixturevalue("bot")
        bot.event_queue = []
        bot.missed_rounds_by_id = {}
        bot.block_count_for_specials = 0
        bot.full_blocks_to_process = []
        bot.processing = False
    yield


def build_test_app(
    grpcclient=lambda: None, mongodb=lambda: None, motormongo=lambda: None, tooter=lambda: None
):
    async def fake_get_api_keys(*args, **kwargs):
        keys = {
            "test-key": {  # type: ignore
                "_id": "test-key",
                "scope": "http://testserver",
                "api_account_id": "test-group",
                "api_group": "ccdexplorer.io",
                "api_key_end_date": {"$date": "2035-10-06T20:06:59.792Z"},
            }
        }
        return keys

    HERE = Path(__file__).resolve().parent.parent / "projects/ccdexplorer_api"
    app_settings = AppSettings(
        static_dir=HERE / "static",
        templates_dir=HERE / "templates",
        node_modules_dir=HERE / "node_modules",
        get_api_keys_fn=fake_get_api_keys,
        api_url="http://testserver",
        ccdexplorer_api_key="test-key",
        mongo_factory=mongodb,
        motor_factory=motormongo,
        grpc_factory=grpcclient,
        tooter_factory=tooter,
        # UsageMiddleware's per-request write needs a writable primary; tests
        # point at a read-only Mongo secondary, so skip it here.
        enable_usage_tracking=False,
    )
    app = factory.create_app(app_settings)

    return app


@pytest.fixture(scope="session")
def test_app(mongodb, motormongo, grpcclient, tooter):
    def mongo_factory():
        return MongoDB(Tooter(), caller_name=__name__)  # built once per test session

    def motor_factory():
        return MongoMotor(Tooter(), nearest=True, caller_name=__name__)

    def grpc_factory():
        return GRPCClient()

    def tooter_factory():
        return Tooter()

    return build_test_app(
        mongodb=mongo_factory,
        motormongo=motor_factory,
        grpcclient=grpc_factory,
        tooter=tooter_factory,
    )


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def live_app(test_app):
    async with LifespanManager(test_app):
        yield test_app


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def client(live_app):
    # live_app already ran FastAPI startup/shutdown (lifespan) via LifespanManager above;
    # wrapping it in a second LifespanManager here would re-run lifespan and open a second
    # set of Mongo connections for no reason.
    transport = httpx.ASGITransport(app=live_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"x-ccdexplorer-key": "test-key"},
    ) as ac:
        yield ac
