# ruff: noqa: F403, F405, E402
# pyright: reportOptionalMemberAccess=false
import pytest
from rich import print


from ccdexplorer.grpc_client import GRPCClient

#
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient_dev():
    return GRPCClient(devnet=True)


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
    block_hash = "c7f287ced1bf0b23f6727861db8c7810c703befc65a0b6961eababb2c9524659"
    tx = tx_at_index_from(0, block_hash, grpcclient_dev)
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
