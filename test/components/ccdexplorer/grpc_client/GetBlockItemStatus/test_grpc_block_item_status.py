import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(net="devnet")


def test_block_item_status_devnet(grpcclient_devnet: GRPCClient):
    """Devnet resets frequently, so look up a live transaction from the latest
    finalized block rather than pinning a specific tx hash/content."""
    block = grpcclient_devnet.get_block_transaction_events("last_final", net=NET.DEVNET)
    if not block.transaction_summaries:
        pytest.skip("No transactions in the latest devnet block to check.")

    tx = block.transaction_summaries[0]
    status = grpcclient_devnet.get_block_item_status(tx.hash, net=NET.DEVNET)
    print(status)

    assert status.finalized is not None
    outcome = status.finalized.outcome
    assert outcome.hash == tx.hash
    assert outcome.energy_cost >= 0
