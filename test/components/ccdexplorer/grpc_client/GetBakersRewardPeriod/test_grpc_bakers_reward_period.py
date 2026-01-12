import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_bakers_reward_period_list(grpcclient: GRPCClient):
    block_hash = "d8bfcc48f9d5721694e0e1108c79bd8345d46049bedc94bdc14d2bc1b02ebecb"
    bakers_list = grpcclient.get_bakers_reward_period(block_hash)
    validator_ids = [baker_info.baker.baker_id for baker_info in bakers_list]

    assert isinstance(bakers_list, list)
    assert len(bakers_list) == 79
    assert 72723 in validator_ids
    print(bakers_list)
