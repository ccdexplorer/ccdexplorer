from types import SimpleNamespace

from ccdexplorer.concordium_client.core import ConcordiumClient
from ccdexplorer.tooter.core import Tooter
import pytest
from unittest.mock import MagicMock, patch

from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ShortBlockInfo
from ccdexplorer.domain.generic import NET
from ccdexplorer.ms_modules.subscriber import Subscriber


@pytest.mark.asyncio
async def test_new_module(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    concordium_client = ConcordiumClient(tooter=tooter)
    # process_new_module bulk_writes to Collections.modules (module.py:150);
    # mock it so the test never attempts a real write.
    mock_modules_collection = MagicMock()
    mock_modules_collection.bulk_write.return_value = SimpleNamespace(
        matched_count=0, modified_count=0, upserted_count=0, deleted_count=0
    )
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(mongodb.testnet, {Collections.modules: mock_modules_collection}, clear=False),
    ):
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb, concordium_client)
        net = NET.TESTNET
        block_info = grpcclient.get_finalized_block_at_height(33863882, net=net)
        # process_new_module reads tx.block_info.hash; in production tx comes from the
        # stored transactions collection (block_info embedded), the live grpc transaction
        # stream doesn't carry it, so attach it here to match.
        short_block_info = CCD_ShortBlockInfo(
            height=block_info.height, hash=block_info.hash, slot_time=block_info.slot_time
        )
        txs = [
            x
            for x in grpcclient.get_block_transaction_events(
                33863882, net=net
            ).transaction_summaries
        ]
        for tx in txs:
            tx.block_info = short_block_info
            if tx.account_transaction:
                if tx.account_transaction.effects.module_deployed:
                    module_ref = tx.account_transaction.effects.module_deployed
                    await subscriber.process_new_module(net, module_ref, tx)
                    await subscriber.verify_module(net, subscriber.concordium_client, module_ref)
                    if net == NET.MAINNET:
                        await subscriber.save_smart_contracts_overview(net)
