import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_blocks_at_height(grpcclient: GRPCClient):
    block_height = 5139916
    bi = grpcclient.get_blocks_at_height(block_height)
    print(f"Blocks: {bi}")

    assert bi[0] == "a87b03c9e363e8f6bc05089aad5b085a7759f1da95d22f41875ecbe6de734832"


def test_finalized_block_at_height(grpcclient: GRPCClient):
    block_height = 5140237
    bi = grpcclient.get_finalized_block_at_height(block_height)
    print(f"Block: {bi}")

    # assert bi[0] == 'a87b03c9e363e8f6bc05089aad5b085a7759f1da95d22f41875ecbe6de734832'
