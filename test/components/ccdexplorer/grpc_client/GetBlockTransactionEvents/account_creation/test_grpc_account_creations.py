# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient


@pytest.fixture
def grpcclient():
    return GRPCClient()


def tx_at_index_from(tx_index: int, block_hash: str, grpcclient: GRPCClient):
    block = grpcclient.get_block_transaction_events(block_hash)
    return block.transaction_summaries[tx_index]


def test_tx_account_creation_initial(grpcclient: GRPCClient):
    block_hash = "0654963b69cb885596c3c01c33f09615fc81907054304090037851e6556bf916"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.hash == "ad57b9bfe67f8c257222f39020b0e00ff62550d5a6fadfc92a0586b436f5f6ef"
    assert tx.type.type == "account_creation"
    assert tx.type.contents == "initial"
    ac = tx.account_creation
    assert ac.credential_type == 0
    assert ac.address == "3Bd2m1js7XQgt2cqsH27E43JXmCRDdzbWWtGjC3QtUVFRg83ya"
    assert (
        ac.reg_id
        == "852774cac79edcaae348bc8967f1dba9356ffa6e79685cb0180d92127f9ebb8bb09a40095de93cd5c356d662bfb2b59f"
    )


def test_tx_account_creation_normal(grpcclient: GRPCClient):
    block_hash = "3b2296a5d8c21bad26659a904097d31168e573bcdade46748d14951b33a44b92"
    tx = tx_at_index_from(2, block_hash, grpcclient)

    assert tx.hash == "a08c7b42164541171bc14e62d4e6749e4a1da1f0a759a8ebfcbcaaaf5d64915b"
    # assert tx.type == {"type": "account_creation", "contents": "normal"}
    assert tx.type.type == "account_creation"
    assert tx.type.contents == "normal"

    ac = tx.account_creation
    assert ac.credential_type == 1
    assert ac.address == "3M7K5wnhFsesQtAuNQiybWsPRRSqpMMj2jZ63GTwknMQaiuVUQ"
    assert (
        ac.reg_id
        == "91064aa72cce83d9e8a0b67c0ffb6e769bda0676be58c4eec8f5ac10454d90505da5c1d468d271efd62b8f0448eabe5a"
    )
