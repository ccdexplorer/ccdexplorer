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

    decoded = grpcclient_devnet.decode_lock_info(lock_info.lock_info)
    assert decoded.lock == lock_list[0]
    assert decoded.recipients == "any" or isinstance(decoded.recipients, list)
    assert decoded.controller.tokens
    print(decoded)


def test_lock_info_decoded_devnet(grpcclient_devnet: GRPCClient):
    block_hash = "last_final"
    for lock_id in grpcclient_devnet.get_lock_list(block_hash):
        lock_info = grpcclient_devnet.get_lock_info(block_hash, lock_id)
        decoded = grpcclient_devnet.decode_lock_info(lock_info.lock_info)
        assert decoded.lock == lock_id
        for grant in decoded.controller.grants:
            assert set(grant.roles) <= {"fund", "send", "return", "cancel"}
        print(decoded)
