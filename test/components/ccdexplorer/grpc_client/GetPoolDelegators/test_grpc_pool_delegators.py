import pytest


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_delegators_pool(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    pool_id = 72723
    dr = grpcclient.get_delegators_for_pool(pool_id, block_hash)

    # print (dr.dict(exclude_none=True))
    print(dr)
    # assert dr.all_pool_total_capital == 8663567331383744
    # assert dr.delegated_capital == 141046997927499
    # assert dr.current_payday_delegated_capital == 139569794466750
    # assert dr.current_payday_transaction_fees_earned == 237025826
    # assert dr.commission_rates.baking == 0.12
