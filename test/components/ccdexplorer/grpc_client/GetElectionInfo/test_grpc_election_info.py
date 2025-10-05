import pytest


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_election_info_simple(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    ei = grpcclient.get_election_info(block_hash)

    print(ei)


def test_election_info_payday(grpcclient: GRPCClient):
    block_hash = "d2735387725c426c85e5242177f80b12a44c1ef6aaa8cc7453a7ed83f912dc10"
    ei = grpcclient.get_election_info(block_hash)

    print(ei)


def test_election_info_payday_2(grpcclient: GRPCClient):
    block_hash = "ce6aa85fd8b8518d5078cc1715fd1cdadf55303babe24a05000571e6e290c58e"
    ei = grpcclient.get_election_info(block_hash)

    print(ei)
