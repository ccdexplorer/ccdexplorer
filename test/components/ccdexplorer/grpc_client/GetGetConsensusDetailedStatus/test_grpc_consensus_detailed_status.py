import pytest
from ccdexplorer.domain.generic import NET


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_status(grpcclient: GRPCClient):
    cds = grpcclient.get_consensus_detailed_status(net=NET.TESTNET)
    print(cds)
