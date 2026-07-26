import pytest

from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(devnet=True)


def test_token_authorizations_devnet(grpcclient_devnet: GRPCClient):
    token_id = "IMT"
    block_hash = "last_final"
    ta = grpcclient_devnet.get_token_authorizations(block_hash, token_id)
    assert ta.token_id == token_id
    print(ta)
