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


def test_tx_sponsor(grpcclient_dev: GRPCClient):
    block_hash = "b88261a9931de8577f0811b975a7a6f493d4f89f83d27cbe85e7583994b34860"
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


def test_tx_effect_account_transfer_plt_transfer2(grpcclient: GRPCClient):
    block_hash = "d0095dfc4f5946c0d0dd6a21591473cbeadf7a35e9c77c4e1393a6464131d307"
    tx = tx_at_index_from(0, block_hash, grpcclient, net=NET.TESTNET)
    assert tx is not None


def test_tx_effect_account_transfer_plt_mint_and_transfer(grpcclient: GRPCClient):
    block_hash = "1c2e1374d0939e9ca40366d4b84864d7ad38e13c52d304ba37716f19d35cb574"
    tx = tx_at_index_from(0, block_hash, grpcclient)
    assert tx is not None
    assert tx.account_transaction is not None
    tae = tx.account_transaction.effects
    assert tae.token_update_effect is not None
    assert tae.token_update_effect.events[0].token_id == "EURR"
    assert tae.token_update_effect.events[0].transfer_event is not None
    assert (
        tae.token_update_effect.events[0].transfer_event.from_.account
        == "3KmD82AmZupWF5qU7bqfmUuVZg9PgR1gpzkLEHuakKkfATnmZf"
    )
    assert (
        tae.token_update_effect.events[0].transfer_event.to.account
        == "4MeoXYXFRGsjqGPSQcD1ZeicZJDdGLTM6H4H2aKPtJG2QxBZHd"
    )
    assert tae.token_update_effect.events[0].transfer_event.amount.value == "10000000"
    assert tae.token_update_effect.events[0].transfer_event.amount.decimals == 6
