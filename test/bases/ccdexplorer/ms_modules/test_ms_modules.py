from ccdexplorer.concordium_client.core import ConcordiumClient
from ccdexplorer.tooter.core import Tooter
import pytest
from unittest.mock import patch

from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.ms_modules.subscriber import Subscriber


@pytest.mark.asyncio
async def test_new_module(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    concordium_client = ConcordiumClient(tooter=tooter)
    with patch.object(tooter, "send_to_tooter") as _:
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb, concordium_client)
        net = NET.TESTNET
        txs = [
            x
            for x in grpcclient.get_block_transaction_events(
                33863882, net=net
            ).transaction_summaries
        ]
        for tx in txs:
            if tx.account_transaction:
                if tx.account_transaction.effects.module_deployed:
                    module_ref = tx.account_transaction.effects.module_deployed
                    await subscriber.process_new_module(net, module_ref)
                    await subscriber.verify_module(net, subscriber.concordium_client, module_ref)
                    if net == NET.MAINNET:
                        await subscriber.save_smart_contracts_overview(net)
