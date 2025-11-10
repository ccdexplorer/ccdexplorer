import pytest
import math
from unittest.mock import AsyncMock, MagicMock
from ccdexplorer.mongodb import CollectionsUtilities
from ccdexplorer.ccdexplorer_api.app.routers.account.account import (
    get_payment_tx_and_update_payments,
    APIPlans,
)
from ccdexplorer.ccdexplorer_api.app.models import APIPayment, APIPlansDetail


@pytest.mark.asyncio
async def test_get_payment_tx_and_update_payments(monkeypatch):
    # --- Arrange ---
    fake_user = MagicMock()
    fake_user.alias_account_id = "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE"
    fake_user.api_account_id = "api-user-1"
    fake_user.plan = "standard"

    plans: dict[APIPlans, APIPlansDetail] = {}
    plans.update(
        {
            APIPlans.free: APIPlansDetail(
                plan_name=APIPlans.free.value,
                eur_rate=0.0000001,
                day_limit=100,
                min_limit=2,
            )
        }
    )
    plans.update(
        {
            APIPlans.standard: APIPlansDetail(
                plan_name=APIPlans.standard.value,
                eur_rate=1,
                day_limit=10_000,
                sec_limit=5,
            )
        }
    )
    plans.update(
        {
            APIPlans.pro: APIPlansDetail(
                plan_name=APIPlans.pro.value, eur_rate=3, day_limit=100_000, sec_limit=5
            )
        }
    )

    # patch in the plans dict where the function expects it
    monkeypatch.setitem(globals(), "plans", plans)

    # eur_plts mock
    eur_plts = ["EURR"]

    # fake txs mock
    txs = [
        {
            "_id": "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f-4F4cGJ8nMM7AGdHhE74L521sgot2G",
            "tx_hash": "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f",
            "impacted_address": "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE",
            "impacted_address_canonical": "4F4cGJ8nMM7AGdHhE74L521sgot2G",
            "effect_type": "token_update_effect",
            "balance_movement": {
                "plt_transfer_in": [
                    {
                        "event": {
                            "from_": {
                                "account": "4K6djiChfhoyuaQXvoT4z5pT6YXzPA91mwjw4jnfPdDNwcU1nK"
                            },
                            "to": {"account": "4F4cGJ8nMM7AGdHhE74L521sgot2GjZ1Pu417wfgQCkZoPXXfE"},
                            "amount": {"value": "10000000", "decimals": 6},
                            "memo": "",
                        },
                        "token_id": "EURR",
                    }
                ]
            },
            "block_height": 38347765,
            "included_in_flow": True,
            "date": "2025-11-06",
            "plt_token_id": "EURR",
        }
    ]

    # mock dependencies
    monkeypatch.setattr(
        "ccdexplorer.ccdexplorer_api.app.routers.account.account.get_plts_that_track_eur",
        AsyncMock(return_value=eur_plts),
    )
    monkeypatch.setattr(
        "ccdexplorer.ccdexplorer_api.app.routers.account.account.await_await",
        AsyncMock(return_value=txs),
    )
    monkeypatch.setattr(
        "ccdexplorer.ccdexplorer_api.app.routers.account.account.set_end_date_for_plan",
        AsyncMock(),
    )

    # mock motormongo + bulk_write
    bulk_write_mock = AsyncMock(return_value=None)
    motormongo_mock = {CollectionsUtilities.api_users: MagicMock(bulk_write=bulk_write_mock)}

    fake_request = MagicMock()
    fake_request.app.motormongo.utilities = motormongo_mock

    db_to_use = MagicMock()

    # --- Act ---
    await get_payment_tx_and_update_payments(fake_request, fake_user, db_to_use)

    # --- Assert ---
    # Payment object created correctly
    assert "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f" in fake_user.payments
    payment: APIPayment = fake_user.payments[
        "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f"
    ]
    assert math.isclose(payment.amount, 10.0)
    assert payment.token_id == "EURR"
    assert payment.paid_days_for_plan == 10  # 10.0 / 1.0
    assert payment.tx_hash == "6896da81ab39c08315217cdd028816813b0c641f81992c203f6c45d671edae7f"

    bulk_write_mock.assert_awaited_once()
    # set_end_date_for_plan.assert_awaited_once()
