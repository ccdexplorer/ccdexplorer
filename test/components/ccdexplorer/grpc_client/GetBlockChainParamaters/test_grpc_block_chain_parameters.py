# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_block_chain_parameters_v1(grpcclient: GRPCClient):
    block_hash = "6f5f4c1526b868c647f67a66c34fc6cf8623ba5361347f3950b947ff5d8d19a0"
    cp = grpcclient.get_block_chain_parameters(block_hash)
    print(cp.v1)  # .dict(exclude_none=True))
    # for a in ai:


def test_block_chain_parameters_v0(grpcclient: GRPCClient):
    block_hash = "8f2a5150c20ce84d9ae98f940f7ab0ed1aa45978019d5ae9012b55c106bdafd8"
    cp = grpcclient.get_block_chain_parameters(block_hash)
    print(cp.v0)  # .dict(exclude_none=True))


def test_block_chain_parameters_v2_testnet(grpcclient: GRPCClient):
    block_hash = "2f841f65f622ec95e8372efcd7898699f4b11b3518c98fc55c1c59e6e4d8b667"
    net = NET.TESTNET

    cp = grpcclient.get_block_chain_parameters(block_hash, net=net)
    print(cp)  # .dict(exclude_none=True))
    # for a in ai:


def test_block_chain_parameters_v3_testnet(grpcclient: GRPCClient):
    block_hash = 24224608
    net = NET.TESTNET

    cp = grpcclient.get_block_chain_parameters(block_hash, net=net)
    print(cp)  # .dict(exclude_none=True))
    # for a in ai:
