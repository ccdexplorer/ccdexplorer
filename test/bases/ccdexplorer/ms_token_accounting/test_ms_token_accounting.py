from types import SimpleNamespace
from unittest.mock import MagicMock, patch, AsyncMock

import pytest
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.ms_token_accounting.heartbeat import Heartbeat
from ccdexplorer.ms_token_accounting.heartbeat.token_accounting_v2 import (
    BALANCE_LOOKUP_FAILED,
)
from ccdexplorer.tooter.core import Tooter
from pymongo import ReplaceOne


@pytest.mark.asyncio
async def test_token_accounting(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    mock_bulk_write = MagicMock()
    mock_collection = MagicMock()
    mock_collection.bulk_write = mock_bulk_write

    mock_result = SimpleNamespace(
        matched_count=3,
        modified_count=2,
        upserted_count=1,
        deleted_count=0,
    )
    mock_collection.bulk_write.return_value = mock_result
    # update_token_accounting_v2 also bulk_writes to tokens_token_addresses_v2
    # (heartbeat/token_accounting_v2.py:479); mock it too so the test never
    # attempts a real write.
    mock_token_addresses_collection = MagicMock()
    mock_token_addresses_collection.bulk_write.return_value = SimpleNamespace(
        matched_count=0, modified_count=0, upserted_count=0, deleted_count=0
    )
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {
                Collections.tokens_links_v3: mock_collection,
                Collections.tokens_token_addresses_v2: mock_token_addresses_collection,
            },
            clear=False,
        ),
        patch(
            "ccdexplorer.ms_token_accounting.heartbeat.token_accounting_v2.publish_to_celery",
            new=AsyncMock(),
        ) as mock_pub,
    ):
        net = "mainnet"
        heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, net)  # type: ignore
        block_height = 36841773
        block_hash = "5199e507ffddfcb7db95b45f34ba4751ee9a6e23d68aa70137cac4388ea994ea"
        await heartbeat.update_token_accounting_v2(net, block_height, block_hash)  # type: ignore

        mock_bulk_write.assert_called_once()
        args, _ = mock_bulk_write.call_args
        ops = args[0]
        op = ops[0]
        assert isinstance(op, ReplaceOne)
        assert op._doc == {
            "account_address": "4PK1FqxXb6Xnx7dWkVikVx5dW6g51KQ4JDFtvfZETHmN1Yw2NP",
            "account_address_canonical": "4PK1FqxXb6Xnx7dWkVikVx5dW6g51",
            "token_holding": {
                "token_address": "<9403,0>-da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831",
                "contract": "<9403,0>",
                "token_id": "da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831",
                "token_amount": "1",
            },
        }
        mock_pub.assert_awaited


@pytest.mark.asyncio
async def test_failed_balance_lookup_is_not_persisted(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    """A balanceOf that reverts or comes back empty yields BALANCE_LOOKUP_FAILED.

    That sentinel is not a balance and must never reach tokens_links_v3 — writing
    it stores a token_amount of "-1" as if it were a real holding. The link is
    left untouched instead, so no bulk_write happens at all for this block.
    """
    mock_collection = MagicMock()
    mock_token_addresses_collection = MagicMock()
    mock_token_addresses_collection.bulk_write.return_value = SimpleNamespace(
        matched_count=0, modified_count=0, upserted_count=0, deleted_count=0
    )
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {
                Collections.tokens_links_v3: mock_collection,
                Collections.tokens_token_addresses_v2: mock_token_addresses_collection,
            },
            clear=False,
        ),
        patch(
            "ccdexplorer.ms_token_accounting.heartbeat.token_accounting_v2.publish_to_celery",
            new=AsyncMock(),
        ),
        patch.object(
            Heartbeat,
            "determine_token_amount",
            new=AsyncMock(return_value=BALANCE_LOOKUP_FAILED),
        ),
    ):
        net = "mainnet"
        heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, net)  # type: ignore
        block_height = 36841773
        block_hash = "5199e507ffddfcb7db95b45f34ba4751ee9a6e23d68aa70137cac4388ea994ea"
        await heartbeat.update_token_accounting_v2(net, block_height, block_hash)  # type: ignore

        mock_collection.bulk_write.assert_not_called()
