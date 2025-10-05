from types import SimpleNamespace
from unittest.mock import MagicMock, patch, AsyncMock

import pytest
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.ms_token_accounting.heartbeat import Heartbeat
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
    )
    mock_collection.bulk_write.return_value = mock_result
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {Collections.tokens_links_v3: mock_collection},
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
        await heartbeat.update_token_accounting_v2(net, block_height)  # type: ignore

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
