import pytest


from ccdexplorer.grpc_client import GRPCClient


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_pool_info(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    pi = grpcclient.get_passive_delegation_info(block_hash)

    # print (pi.dict(exclude_none=True))
    # print (pi)
    assert pi.all_pool_total_capital == 8663567331383744
    assert pi.delegated_capital == 141046997927499
    assert pi.current_payday_delegated_capital == 139569794466750
    assert pi.current_payday_transaction_fees_earned == 237025826
    assert pi.commission_rates.baking == 0.12
