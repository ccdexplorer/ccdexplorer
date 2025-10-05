import pytest


from ccdexplorer.grpc_client import GRPCClient


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_winning_bakers(grpcclient: GRPCClient):
    wb = grpcclient.get_winning_bakers_epoch(genesis_index=7, epoch=3023)
    assert len(wb) == 1538
    pass
