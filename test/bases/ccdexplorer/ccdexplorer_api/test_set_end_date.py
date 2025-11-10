import pytest
import datetime as dt
from unittest.mock import AsyncMock, MagicMock
from ccdexplorer.mongodb import CollectionsUtilities
from ccdexplorer.ccdexplorer_api.app.routers.account.account import set_end_date_for_plan
from ccdexplorer.ccdexplorer_api.app.models import APIPayment


@pytest.mark.asyncio
async def test_set_end_date_for_plan__no_payments(monkeypatch):
    """User with no payments should get 365 days if free, or -1 day if not."""
    fake_user = MagicMock()
    fake_user.payments = {}
    fake_user.plan = "free"
    fake_user.api_account_id = "api-user-1"

    bulk_write_mock = AsyncMock(return_value=None)
    motormongo_mock = {CollectionsUtilities.api_users: MagicMock(bulk_write=bulk_write_mock)}
    fake_request = MagicMock()
    fake_request.app.motormongo.utilities = motormongo_mock

    now = dt.datetime.now().astimezone(dt.UTC)

    await set_end_date_for_plan(fake_request, fake_user)

    # Check plan_end_date set properly (roughly +365 days)
    assert (fake_user.plan_end_date - now).days in (364, 365)
    bulk_write_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_set_end_date_for_plan__with_payments(monkeypatch):
    """User with payments , overlapping payments."""
    fake_user = MagicMock()
    fake_user.plan = "standard"
    fake_user.api_account_id = "api-user-1"

    # two payments: spaced 1 day apart
    payment1 = APIPayment(
        tx_hash="07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf",
        tx_date="2025-11-06",
        amount=50.0,
        token_id="EURR",
        paid_days_for_plan=50,
    )
    payment2 = APIPayment(
        tx_hash="6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
        tx_date="2025-11-06",
        amount=10.0,
        token_id="EURR",
        paid_days_for_plan=10,
    )
    fake_user.payments = {
        "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf": payment1,
        "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f": payment2,
    }

    tx1_json = {
        "_id": "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf",
        "index": 1,
        "energy_cost": 646,
        "hash": "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf",
        "type": {
            "type": "account_transaction",
            "contents": "token_update_effect",
            "additional_data": "transfer",
        },
        "account_transaction": {
            "cost": 535125,
            "sender": "3KHgL9eJ2gsTVbspTNbjME3wdELrmdVhJMenSm6171NpE56deS",
            "outcome": "success",
            "effects": {
                "token_update_effect": {
                    "events": [
                        {
                            "token_id": "EURR",
                            "transfer_event": {
                                "from_": {
                                    "account": "3KHgL9eJ2gsTVbspTNbjME3wdELrmdVhJMenSm6171NpE56deS"
                                },
                                "to": {
                                    "account": "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE"
                                },
                                "amount": {"value": "50000000", "decimals": 6},
                                "memo": "",
                            },
                        }
                    ]
                }
            },
        },
        "block_info": {
            "height": 38318766,
            "hash": "96f3ee1b70b341769869b9b36d023651600ccdbe4f404e1478a3d70f5ecb038d",
            "slot_time": "2025-11-06T07:23:28.420Z",
        },
    }
    tx2_json = {
        "_id": "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
        "index": 0,
        "energy_cost": 655,
        "hash": "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
        "type": {
            "type": "account_transaction",
            "contents": "token_update_effect",
            "additional_data": "transfer",
        },
        "account_transaction": {
            "cost": 468537,
            "sender": "4K6djiChfhoyuaQXvoT4z5pT6YXzPA91mwjw4jnfPdDNwcU1nK",
            "outcome": "success",
            "effects": {
                "token_update_effect": {
                    "events": [
                        {
                            "token_id": "EURR",
                            "transfer_event": {
                                "from_": {
                                    "account": "4K6djiChfhoyuaQXvoT4z5pT6YXzPA91mwjw4jnfPdDNwcU1nK"
                                },
                                "to": {
                                    "account": "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE"
                                },
                                "amount": {"value": "10000000", "decimals": 6},
                                "memo": "",
                            },
                        }
                    ]
                }
            },
        },
        "block_info": {
            "height": 38347765,
            "hash": "cd00591141df0c49a6589d7b28b2b27e7a0e5f837dd843db2aabc5480d18a112",
            "slot_time": "2025-11-06T23:30:25.790Z",
        },
    }
    # mock httpx response for both tx hashes
    # Build one mock per tx_hash
    response_tx1 = MagicMock()
    response_tx1.raise_for_status = MagicMock()
    response_tx1.json.return_value = tx1_json

    response_tx2 = MagicMock()
    response_tx2.raise_for_status = MagicMock()
    response_tx2.json.return_value = tx2_json

    async def fake_get(url, *_, **__):
        if "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf" in url:
            return response_tx1
        elif "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f" in url:
            return response_tx2
        raise ValueError(f"Unexpected URL {url}")

    httpx_client_mock = MagicMock()
    httpx_client_mock.get = AsyncMock(side_effect=fake_get)

    # mock Mongo write
    bulk_write_mock = AsyncMock(return_value=None)
    motormongo_mock = {CollectionsUtilities.api_users: MagicMock(bulk_write=bulk_write_mock)}

    fake_request = MagicMock()
    fake_request.app.motormongo.utilities = motormongo_mock
    fake_request.app.httpx_client = httpx_client_mock
    fake_request.app.api_url = "http://fake-api"

    # --- Act ---
    await set_end_date_for_plan(fake_request, fake_user)

    # --- Assert ---
    # It should call httpx for each tx
    assert httpx_client_mock.get.await_count == 2
    bulk_write_mock.assert_awaited_once()

    # The resulting plan_end_date should be after 2025-11-05
    assert isinstance(fake_user.plan_end_date, dt.datetime)
    assert fake_user.plan_end_date.date() == dt.date(2026, 1, 4)


@pytest.mark.asyncio
async def test_set_end_date_for_plan__with_payments_with_gap(monkeypatch):
    """User with payments , gap in payments, so previous payments expire before next payment."""
    fake_user = MagicMock()
    fake_user.plan = "standard"
    fake_user.api_account_id = "api-user-1"

    # two payments: spaced 1 day apart
    payment1 = APIPayment(
        tx_hash="07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf",
        tx_date="2025-10-01",
        amount=10.0,
        token_id="EURR",
        paid_days_for_plan=10,
    )
    payment2 = APIPayment(
        tx_hash="6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
        tx_date="2025-10-20",
        amount=20.0,
        token_id="EURR",
        paid_days_for_plan=20,
    )
    fake_user.payments = {
        "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf": payment1,
        "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f": payment2,
    }

    tx1_json = {
        "_id": "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf",
        "index": 1,
        "energy_cost": 646,
        "hash": "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf",
        "type": {
            "type": "account_transaction",
            "contents": "token_update_effect",
            "additional_data": "transfer",
        },
        "account_transaction": {
            "cost": 535125,
            "sender": "3KHgL9eJ2gsTVbspTNbjME3wdELrmdVhJMenSm6171NpE56deS",
            "outcome": "success",
            "effects": {
                "token_update_effect": {
                    "events": [
                        {
                            "token_id": "EURR",
                            "transfer_event": {
                                "from_": {
                                    "account": "3KHgL9eJ2gsTVbspTNbjME3wdELrmdVhJMenSm6171NpE56deS"
                                },
                                "to": {
                                    "account": "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE"
                                },
                                "amount": {"value": "10000000", "decimals": 6},
                                "memo": "",
                            },
                        }
                    ]
                }
            },
        },
        "block_info": {
            "height": 38318766,
            "hash": "96f3ee1b70b341769869b9b36d023651600ccdbe4f404e1478a3d70f5ecb038d",
            "slot_time": "2025-10-01T07:23:28.420Z",
        },
    }
    tx2_json = {
        "_id": "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
        "index": 0,
        "energy_cost": 655,
        "hash": "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
        "type": {
            "type": "account_transaction",
            "contents": "token_update_effect",
            "additional_data": "transfer",
        },
        "account_transaction": {
            "cost": 468537,
            "sender": "4K6djiChfhoyuaQXvoT4z5pT6YXzPA91mwjw4jnfPdDNwcU1nK",
            "outcome": "success",
            "effects": {
                "token_update_effect": {
                    "events": [
                        {
                            "token_id": "EURR",
                            "transfer_event": {
                                "from_": {
                                    "account": "4K6djiChfhoyuaQXvoT4z5pT6YXzPA91mwjw4jnfPdDNwcU1nK"
                                },
                                "to": {
                                    "account": "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE"
                                },
                                "amount": {"value": "20000000", "decimals": 6},
                                "memo": "",
                            },
                        }
                    ]
                }
            },
        },
        "block_info": {
            "height": 38347765,
            "hash": "cd00591141df0c49a6589d7b28b2b27e7a0e5f837dd843db2aabc5480d18a112",
            "slot_time": "2025-10-20T23:30:25.790Z",
        },
    }
    # mock httpx response for both tx hashes
    # Build one mock per tx_hash
    response_tx1 = MagicMock()
    response_tx1.raise_for_status = MagicMock()
    response_tx1.json.return_value = tx1_json

    response_tx2 = MagicMock()
    response_tx2.raise_for_status = MagicMock()
    response_tx2.json.return_value = tx2_json

    async def fake_get(url, *_, **__):
        if "07e233648ae832ee058753d7bcc184744657779202fd7f3f9152e656d5e1a3bf" in url:
            return response_tx1
        elif "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f" in url:
            return response_tx2
        raise ValueError(f"Unexpected URL {url}")

    httpx_client_mock = MagicMock()
    httpx_client_mock.get = AsyncMock(side_effect=fake_get)

    # mock Mongo write
    bulk_write_mock = AsyncMock(return_value=None)
    motormongo_mock = {CollectionsUtilities.api_users: MagicMock(bulk_write=bulk_write_mock)}

    fake_request = MagicMock()
    fake_request.app.motormongo.utilities = motormongo_mock
    fake_request.app.httpx_client = httpx_client_mock
    fake_request.app.api_url = "http://fake-api"

    # --- Act ---
    await set_end_date_for_plan(fake_request, fake_user)

    # --- Assert ---
    # It should call httpx for each tx
    assert httpx_client_mock.get.await_count == 2
    bulk_write_mock.assert_awaited_once()

    # The resulting plan_end_date should be after 2025-11-05
    assert isinstance(fake_user.plan_end_date, dt.datetime)
    assert fake_user.plan_end_date.date() == dt.date(2025, 10, 20) + dt.timedelta(days=20)
