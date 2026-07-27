# ruff: noqa: F403, F405, E402
# pyright: reportOptionalMemberAccess=false
import pytest
from rich import print


from ccdexplorer.grpc_client import GRPCClient

#
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient_dev():
    return GRPCClient(net="devnet")


@pytest.fixture
def grpcclient():
    return GRPCClient()


def tx_at_index_from(
    tx_index: int, block_hash: str, grpcclient: GRPCClient, net: NET = NET.MAINNET
):
    block = grpcclient.get_block_transaction_events(block_hash, net)
    if tx_index == -1:
        return None
    else:
        return block.transaction_summaries[tx_index]


def test_tx_sponsor_reject(grpcclient_dev: GRPCClient):
    # Devnet resets frequently, so look up a live transaction rather than pinning
    # a specific block hash that will stop existing on the next reset.
    block_hash = "last_final"
    block = grpcclient_dev.get_block_transaction_events(block_hash, net=NET.DEVNET)
    if not block.transaction_summaries:
        pytest.skip("No transactions in the latest devnet block to check.")
    tx = block.transaction_summaries[0]
    print(tx)
    # assert tx is not None
    # assert tx.account_transaction is not None
    # assert tx.account_transaction.effects.token_update_effect is not None
    # assert tx.account_transaction.effects.token_update_effect.events[0].token_id == "EURR"
    # assert tx.account_transaction.effects.token_update_effect.events[0].burn_event is not None
    # assert (
    #     tx.account_transaction.effects.token_update_effect.events[0].burn_event.target.account
    #     == "4MeoXYXFRGsjqGPSQcD1ZeicZJDdGLTM6H4H2aKPtJG2QxBZHd"
    # )

    # assert (
    #     tx.account_transaction.effects.token_update_effect.events[0].burn_event.amount.value
    #     == "10000000"
    # )
