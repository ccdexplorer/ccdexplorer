import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(net="devnet")


def test_token_authorizations_devnet(grpcclient_devnet: GRPCClient):
    block_hash = "last_final"
    token_list = grpcclient_devnet.get_token_list(block_hash, net=NET.DEVNET)
    assert len(token_list) > 0
    token_id = token_list[0]
    ta = grpcclient_devnet.get_token_authorizations(block_hash, token_id, net=NET.DEVNET)
    assert ta.token_id == token_id
    print(ta)
