import pytest


from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


# def test_finalized_block_info(grpcclient: GRPCClient):
#     # while True:
#     #     bi = grpcclient.get_finalized_blocks()
#     #     print (bi)

#     grpcclient.get_finalized_blocks()


def test_get_consensus_info(grpcclient: GRPCClient):
    block = grpcclient.get_consensus_info(net=NET.MAINNET)
    print(block)


def test_get_finalized_block_at_height_testnet(grpcclient: GRPCClient):
    block = grpcclient.get_finalized_block_at_height(2708607, NET("testnet"))
    assert block is not None
