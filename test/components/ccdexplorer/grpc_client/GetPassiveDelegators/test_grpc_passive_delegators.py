import pytest


from ccdexplorer.grpc_client import GRPCClient


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_delegators_passive(grpcclient: GRPCClient):
    block_hash = "9c89b7b3cd65d3f5a96b1b9112c66530a0cea4dc831ba9b6c9dfddbeafbd8813"

    dr = grpcclient.get_delegators_for_passive_delegation(block_hash)

    assert dr[-1].account == "4UxjnJqo2obe627Q9bBjVdfd1J9SZGYCgMwatb1WrsN7JJYrzG"
    assert dr[-1].stake == 11000000000
    assert dr[-1].pending_change is None
    assert len(dr) == 682
    # assert dr.delegated_capital == 141046997927499
    # assert dr.current_payday_delegated_capital == 139569794466750
    # assert dr.current_payday_transaction_fees_earned == 237025826
    # assert dr.commission_rates.baking == 0.12
