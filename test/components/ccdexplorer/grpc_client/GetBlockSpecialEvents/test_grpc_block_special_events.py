import pytest
from ccdexplorer.domain.generic import NET


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_block_special_event_pool_reward(grpcclient: GRPCClient):
    block_hash = "6f5f4c1526b868c647f67a66c34fc6cf8623ba5361347f3950b947ff5d8d19a0"
    ai = grpcclient.get_block_special_events(block_hash)
    print(ai)  # .dict(exclude_none=True))
    # for a in ai:
    #     print (a.dict(exclude_none=True))
    # assert ai.address == account
    # assert ai.schedule.schedules[0].amount == 16358781149999
    # assert ai.schedule.schedules[0].timestamp == dt.datetime(2023, 2, 5, 22, 0)
    # assert ai.schedule.schedules[0].transactions[0] == 'f33050060051c6b738549b60e99498fc7f59fb0b6c915ed9e90a11a7584f2d30'


def test_block_special_event_finalization_reward(grpcclient: GRPCClient):
    block_hash = "8f2a5150c20ce84d9ae98f940f7ab0ed1aa45978019d5ae9012b55c106bdafd8"
    ai = grpcclient.get_block_special_events(block_hash)
    # print (ai)#.dict(exclude_none=True))

    for a in ai:
        print(a.model_dump(exclude_none=True))


def test_block_special_event_baking_reward(grpcclient: GRPCClient):
    block_hash = "f06c9909576a6fc28640036c0e99aabf440f2f874b82d7916edfc549aadc2f0c"
    ai = grpcclient.get_block_special_events(block_hash)
    # print (ai)#.dict(exclude_none=True))

    for index, a in enumerate(ai):
        print(index, a.model_dump(exclude_none=True))


def test_block_special_event_block_reward(grpcclient: GRPCClient):
    block_hash = "6ce029891419428b1ad048837abf9c3b7158c1f52f171f0422dc6eb54f15f2f1"
    ai = grpcclient.get_block_special_events(block_hash)
    # print (ai)#.dict(exclude_none=True))

    for index, a in enumerate(ai):
        print(index, a.model_dump(exclude_none=True))


def test_block_special_event_pool_reward_baker_0(grpcclient: GRPCClient):
    block_hash = "3a2746add04f2e35832ddbde0ec3dc2b6471d1a3d962c38cb048b2a593404e18"
    ai = grpcclient.get_block_special_events(block_hash)
    for a in ai:
        if a.payday_pool_reward:
            if a.payday_pool_reward.pool_owner == 0:
                print(a)
    print(ai)  # .dict(exclude_none=True))


def test_block_special_event_block_1000000(grpcclient: GRPCClient):
    block_hash = "8f2a5150c20ce84d9ae98f940f7ab0ed1aa45978019d5ae9012b55c106bdafd8"
    ai = grpcclient.get_block_special_events(block_hash)
    for a in ai:
        if a.payday_pool_reward:
            if a.payday_pool_reward.pool_owner == 0:
                print(a)
    print(ai)  # .dict(exclude_none=True))


def test_block_special_event_block_5000000(grpcclient: GRPCClient):
    block_hash = "cf3e3ca47911a923c982a3fee7214cb542038126e79dea768df6ec208c93e3b3"
    ai = grpcclient.get_block_special_events(block_hash)
    for a in ai:
        if a.payday_pool_reward:
            if a.payday_pool_reward.pool_owner == 0:
                print(a)
    print(ai)  # .dict(exclude_none=True))


def test_block_special_event_block_5100000(grpcclient: GRPCClient):
    block_hash = "cdf9b5114021f0e32e7805e600159e08001a628d6d82ae99037c6482b0f0cfae"
    ai = grpcclient.get_block_special_events(block_hash)
    for a in ai:
        if a.payday_pool_reward:
            if a.payday_pool_reward.pool_owner == 0:
                print(a)
    print(ai)  # .dict(exclude_none=True))


def test_block_special_event_block_3232444(grpcclient: GRPCClient):
    block_hash = "ea4a52a04ba905c2692b4598aa344707327781d588573d374125b449a1ce0bcc "
    ai = grpcclient.get_block_special_events(block_hash)
    print(ai)  # .dict(exclude_none=True))


def test_block_special_event_block_1(grpcclient: GRPCClient):
    block_input = 2
    ai = grpcclient.get_block_special_events(block_input)
    print(block_input, ai)  # .dict(exclude_none=True))


def test_block_special_event_block_accrue(grpcclient: GRPCClient):
    block_input = "a5e052744c0f6c982eb59fa4ee1fe9e1bb6d5849280f1f26088d914065d3a436"
    ai = grpcclient.get_block_special_events(block_input)
    print(block_input, ai)  # .dict(exclude_none=True))


def test_block_special_event_primed(grpcclient: GRPCClient):
    block_input = 24224608
    se = grpcclient.get_block_special_events(block_input, net=NET.TESTNET)

    print(block_input, se)  # .dict(exclude_none=True))
