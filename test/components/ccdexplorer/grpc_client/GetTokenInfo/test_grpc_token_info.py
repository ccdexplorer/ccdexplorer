import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_token_info(grpcclient: GRPCClient):
    token_id = "EURR"
    block_hash = "a4b091d1c382e8f12aef20f3d56f7660382b6759a40c78f3a422d483818d7001"
    ti = grpcclient.get_token_info(block_hash, token_id)
    assert ti.token_id == token_id
    print(ti)
    # assert ai.address == account
    # assert ai.schedule.schedules[0].amount == 16358781149999
    # assert ai.schedule.schedules[0].timestamp == dt.datetime(
    #     2023, 2, 5, 21, 0, tzinfo=dt.timezone.utc
    # )
    # assert (
    #     ai.schedule.schedules[0].transactions[0]
    #     == "f33050060051c6b738549b60e99498fc7f59fb0b6c915ed9e90a11a7584f2d30"
    # )
