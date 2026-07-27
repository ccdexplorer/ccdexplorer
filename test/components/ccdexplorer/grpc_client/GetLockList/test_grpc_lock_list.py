import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(net="devnet")


def test_lock_list_devnet(grpcclient_devnet: GRPCClient):
    block_hash = "last_final"
    lock_list = grpcclient_devnet.get_lock_list(block_hash, net=NET.DEVNET)
    assert isinstance(lock_list, list)
    print(lock_list)
