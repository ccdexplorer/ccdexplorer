import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_token_list(grpcclient: GRPCClient):
    block_hash = "39580ad166b28570aeef8d5660de071cf990fad244cb8ab75f3d9a862958f2f6"
    token_list = grpcclient.get_token_list(block_hash)
    assert isinstance(token_list, list)
    assert token_list == ["USDR", "EURR"]
    print(token_list)


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(devnet=True)


def test_token_list_devnet(grpcclient_devnet: GRPCClient):
    block_hash = "last_final"
    token_list = grpcclient_devnet.get_token_list(block_hash)
    assert isinstance(token_list, list)
    # assert token_list == ["USDR", "EURR"]
    print(token_list)
