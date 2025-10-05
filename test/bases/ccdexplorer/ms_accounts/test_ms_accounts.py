from ccdexplorer.tooter.core import Tooter
from pymongo import ReplaceOne
import pytest
from unittest.mock import patch, MagicMock

from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.ms_accounts.subscriber import Subscriber


@pytest.mark.asyncio
async def test_new_account(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    mock_bulk_write = MagicMock()
    mock_collection = MagicMock()
    mock_collection.bulk_write = mock_bulk_write
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {Collections.all_account_addresses: mock_collection},
            clear=False,
        ),
    ):
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
        net = NET.MAINNET
        block_height = 37061947
        await subscriber.process_new_address(net, block_height)
        mock_bulk_write.assert_called_once()
        args, kwargs = mock_bulk_write.call_args
        ops = args[0]
        op = ops[0]
        account_address = "3fknDNHU4gb1pqWHMbWFVnopshLT5hjVXvcqQxdbL18C6yuwP9"
        assert isinstance(op, ReplaceOne)
        assert op._doc == {
            "_id": account_address[:29],
            "account_address": account_address,
            "account_index": 103187,
        }
