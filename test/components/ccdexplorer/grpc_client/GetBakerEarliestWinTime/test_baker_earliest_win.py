# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient

from ccdexplorer.domain.generic import NET

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_earliest_win_time_testnet(grpcclient: GRPCClient):
    baker_id = 1
    win_time = grpcclient.get_baker_earliest_win_time(baker_id=baker_id, net=NET.TESTNET)
    print(win_time)
