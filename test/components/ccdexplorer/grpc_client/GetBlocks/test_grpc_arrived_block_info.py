# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


# @pytest.mark.skip(reason="Finalized blocks is a stream that never ends...")
def test_arrived_block_info(grpcclient: GRPCClient):
    bi = grpcclient.get_blocks()
    print(bi)

    grpcclient.get_finalized_blocks()


def test_get_finalized_block_at_height(grpcclient: GRPCClient):
    block = grpcclient.get_finalized_block_at_height(1_000_000_000)
    assert block is None


def test_get_finalized_block_at_height_testnet(grpcclient: GRPCClient):
    block = grpcclient.get_finalized_block_at_height(2708607, NET("testnet"))
    assert block is not None
