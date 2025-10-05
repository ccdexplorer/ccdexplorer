# tests/conftest.py
import asyncio
import os
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from ccdexplorer.api.app import factory
from ccdexplorer.api.app.factory import AppSettings
from ccdexplorer.bot.bot import Bot, Connections
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter.core import Tooter

# Set environment variables for testing
os.environ["API_URL"] = "http://testserver"
import importlib

import ccdexplorer.env

importlib.reload(ccdexplorer.env)  # need to reload to pick up changed env vars
# Set environment variables for testing


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
    return MongoMotor(tooter, nearest=True)


@pytest.fixture(scope="session")
def mongodb(tooter) -> MongoDB:
    """Provide a MongoDB instance using the Tooter."""
    return MongoDB(tooter)


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

    HERE = Path(__file__).resolve().parent.parent / "projects/api"
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
    )
    app = factory.create_app(app_settings)

    return app


@pytest.fixture
def test_app(mongodb, motormongo, grpcclient, tooter):
    def mongo_factory():
        return MongoDB(Tooter())  # each test gets its own fresh instances

    def motor_factory():
        return MongoMotor(Tooter(), nearest=True)

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


@pytest_asyncio.fixture
async def live_app(test_app):
    async with LifespanManager(test_app):
        yield test_app


@pytest_asyncio.fixture
async def client(live_app):
    # Ensure FastAPI startup/shutdown (lifespan) runs:
    async with LifespanManager(live_app):
        transport = httpx.ASGITransport(app=live_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
            headers={"x-ccdexplorer-key": "test-key"},
        ) as ac:
            yield ac
