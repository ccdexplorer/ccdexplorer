# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_block_pending_update(grpcclient: GRPCClient):
    block_hash = "6f5f4c1526b868c647f67a66c34fc6cf8623ba5361347f3950b947ff5d8d19a0"
    ai = grpcclient.get_block_pending_updates(block_hash)
    print(ai)  # .dict(exclude_none=True))
    # for a in ai:


def test_block_special_event_finalization_reward(grpcclient: GRPCClient):
    block_hash = "8f2a5150c20ce84d9ae98f940f7ab0ed1aa45978019d5ae9012b55c106bdafd8"
    ai = grpcclient.get_block_special_events(block_hash)
    # print (ai)#.dict(exclude_none=True))

    for a in ai:
        print(a.dict(exclude_none=True))


def test_block_special_event_baking_reward(grpcclient: GRPCClient):
    block_hash = "3206f0d2ad13f258ce8109bc7f526ed32ebd0656d923743e1da783925fee8b90"
    ai = grpcclient.get_block_special_events(block_hash)
    # print (ai)#.dict(exclude_none=True))

    for a in ai:
        print(a.dict(exclude_none=True))


def test_block_special_event_payday(grpcclient: GRPCClient):
    block_hash = "6f5f4c1526b868c647f67a66c34fc6cf8623ba5361347f3950b947ff5d8d19a0"
    ai = grpcclient.get_block_special_events(block_hash)
    # print (ai)#.dict(exclude_none=True))

    for a in ai:
        print(a.dict(exclude_none=True))
