import pytest

from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(devnet=True)


def test_lock_info_devnet(grpcclient_devnet: GRPCClient):
    block_hash = "last_final"
    lock_list = grpcclient_devnet.get_lock_list(block_hash)
    assert len(lock_list) > 0
    lock_info = grpcclient_devnet.get_lock_info(block_hash, lock_list[0])
    assert lock_info.lock_info is not None
    print(lock_info)
