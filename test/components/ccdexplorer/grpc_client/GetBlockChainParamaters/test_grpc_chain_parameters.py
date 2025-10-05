# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient

from rich import print
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_chain_parameters_v0(grpcclient: GRPCClient):
    block_hash = "d42f5a91a4bb1df78423cc7fa3bd7aa750e6aa9bced7611f8d1057f27766abaa"
    cp = grpcclient.get_block_chain_parameters(block_hash)

    print(cp)


def test_chain_parameters_v1(grpcclient: GRPCClient):
    block_hash = "1b9a4353dbc6561f5e4e410df78115263001a096bccad94772da1b074675744d"
    cp = grpcclient.get_block_chain_parameters(block_hash)

    print(cp)


def test_chain_parameters_v2(grpcclient: GRPCClient):
    block_hash = "2f841f65f622ec95e8372efcd7898699f4b11b3518c98fc55c1c59e6e4d8b667"
    cp = grpcclient.get_block_chain_parameters(block_hash, net=NET.TESTNET)

    print(cp)
