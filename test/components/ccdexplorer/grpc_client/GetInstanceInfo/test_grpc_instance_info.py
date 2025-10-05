import pytest


from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def grpcclient_testnet():
    return GRPCClient()


def test_instance_info_v1(grpcclient: GRPCClient):  # , grpcclient_testnet: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    index = 7076
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash)

    assert ii.v1.owner == "3BtQiJ1sNad8cp5JLdBrjuRYkoX6A8fanzHpoSshKEX6ghAWwi"
    assert len(ii.v1.methods) == 12
    assert ii.v1.amount == 0
    assert ii.v1.methods[0] == "BictoryCns.createSubdomain"
    assert ii.v1.name == "init_BictoryCns"
    assert ii.v1.source_module == "7508ce8d24e5cb736851612d94aec545d713d16333e1285df125fc5d25843927"


def test_instance_info_v0(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    index = 0
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash)
    print(ii)
    assert ii.v0.owner == "4rc1tjHwkFB5JP39rcEeRqsh1gMJZerjjDpWoqqTY6VHy2WZgj"
    assert len(ii.v0.methods) == 11
    assert ii.v0.methods[0] == "inventory.add_trader"
    assert ii.v0.name == "init_inventory"
    assert ii.v0.source_module == "d571155b495c0f9e6f56ae54a51c014ba2cc6895bdad337e2968b7b297066c11"


def test_instance_info_v1_testnet(
    grpcclient: GRPCClient,
):
    block_hash = "9dcb231135fff1555b1863e1abafb302d4bf026ebecb940e64a54b38b2964ddd"
    index = 0
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash, net=NET.TESTNET)
    assert ii is not None
    # assert ii.v1.owner == '3BtQiJ1sNad8cp5JLdBrjuRYkoX6A8fanzHpoSshKEX6ghAWwi'
    # assert len(ii.v1.methods) == 12
    # assert ii.v1.amount == 0
    # assert ii.v1.methods[0] == 'BictoryCns.createSubdomain'
    # assert ii.v1.name == 'init_BictoryCns'
    # assert ii.v1.source_module == '7508ce8d24e5cb736851612d94aec545d713d16333e1285df125fc5d25843927'


def test_instance_info_v1_9336_after_upgrade(
    grpcclient: GRPCClient,
):
    block_hash = "b8e63217463b3b7681f1dbc12b7dd5e7c55627c75157df9184e69697fac03ee3"
    index = 9336
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash, net=NET.MAINNET)
    print(ii)


def test_instance_info_v1_9336_before_upgrade(
    grpcclient: GRPCClient,
):
    block_hash = "d209bfd9470c603a180b338d05a2b2646b02f965bfa21de2d95f960bc8aeb5cd"
    index = 9336
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash, net=NET.MAINNET)
    print(ii)


def test_instance_info_v1_test(
    grpcclient: GRPCClient,
):  # , grpcclient_testnet: GRPCClient):
    block_hash = "8198818eee571af992c4d09555bfd799e3a5f672e4640da73006e48a2d1b85e8"
    index = 7085
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash)
    print(ii)
    assert ii.v1.owner == "3BtQiJ1sNad8cp5JLdBrjuRYkoX6A8fanzHpoSshKEX6ghAWwi"
    assert len(ii.v1.methods) == 12
    assert ii.v1.amount == 0
    assert ii.v1.methods[0] == "BictoryCns.createSubdomain"
    assert ii.v1.name == "init_BictoryCns"
    assert ii.v1.source_module == "7508ce8d24e5cb736851612d94aec545d713d16333e1285df125fc5d25843927"


def test_instance_info_EUROe(
    grpcclient: GRPCClient,
):
    block_hash = "last_final"
    index = 9390
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash, net=NET.MAINNET)
    print(ii)
