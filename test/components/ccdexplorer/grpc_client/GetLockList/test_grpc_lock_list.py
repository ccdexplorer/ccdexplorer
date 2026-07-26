import pytest

from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(devnet=True)


def test_lock_list_devnet(grpcclient_devnet: GRPCClient):
    block_hash = "last_final"
    lock_list = grpcclient_devnet.get_lock_list(block_hash)
    assert isinstance(lock_list, list)
    print(lock_list)
