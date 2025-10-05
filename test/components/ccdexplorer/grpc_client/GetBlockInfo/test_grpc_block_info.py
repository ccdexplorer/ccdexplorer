# pyright: reportOptionalMemberAccess=false
import pytest
import datetime as dt

from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from ccdexplorer.grpc_client import GRPCClient

from rich import print
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_last_final(grpcclient: GRPCClient):
    bi = grpcclient.get_block_info("last_final")
    print(bi)


def tx_at_index_from(tx_index: int, block_hash: str, grpcclient: GRPCClient):
    block = grpcclient.get_block_transaction_events(block_hash)
    return block.transaction_summaries[tx_index]


def test_tx_block_info(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    bi = grpcclient.get_block_info(block_hash)

    assert bi.baker == 8
    assert bi.hash == block_hash
    assert bi.height == 5036415
    assert (
        bi.last_finalized_block
        == "946cfc98e6c64a3102d5fce8f326f725104004e6405d9aadd543ef83b7eeef12"
    )
    assert bi.parent_block == "946cfc98e6c64a3102d5fce8f326f725104004e6405d9aadd543ef83b7eeef12"
    assert bi.slot_number == 13966389
    assert bi.slot_time == dt.datetime(2023, 1, 22, 17, 58, 26, 750000, tzinfo=dt.timezone.utc)
    assert bi.era_block_height == 344569
    assert bi.finalized is True
    assert bi.genesis_index == 4
    assert bi.transaction_count == 9
    assert bi.transactions_energy_cost == 4455
    assert bi.transactions_size == 1494

    print(bi.dict(exclude_none=True))


def test_tx_block_info_genesis(grpcclient: GRPCClient):
    block_hash = "9dd9ca4d19e9393877d2c44b70f89acbfc0883c2243e5eeaecc0d1cd0503f478"
    bi: CCD_BlockInfo = grpcclient.get_block_info(block_hash)

    # TODO baker should be None, but proto3 can't distinguish between empty and 0...
    assert bi.baker is None
    assert bi.slot_time == dt.datetime(2021, 6, 9, 6, 0, tzinfo=dt.timezone.utc)

    print(bi)  # ).dict(exclude_none=True))

    # 2454999


def test_tx_block_info_protocol_6(grpcclient: GRPCClient):
    block_hash = "2f841f65f622ec95e8372efcd7898699f4b11b3518c98fc55c1c59e6e4d8b667"
    bi: CCD_BlockInfo = grpcclient.get_block_info(block_hash, net=NET.TESTNET)

    # # TODO baker should be None, but proto3 can't distinguish between empty and 0...
    # assert bi.baker == None
    # assert bi.slot_time == dt.datetime(2021, 6, 9, 6, 0, tzinfo=dt.timezone.utc)

    print(bi)  # ).dict(exclude_none=True))

    # 2454999


def test_tx_block_info_using_height(grpcclient: GRPCClient):
    block_input = 1_000_000
    bi: CCD_BlockInfo = grpcclient.get_block_info(block_input)

    # # TODO baker should be None, but proto3 can't distinguish between empty and 0...
    # assert bi.baker == None
    # assert bi.slot_time == dt.datetime(2021, 6, 9, 6, 0, tzinfo=dt.timezone.utc)

    print(bi)  # ).dict(exclude_none=True))


def test_tx_block_info_testnet(grpcclient: GRPCClient):
    block_hash = "2f841f65f622ec95e8372efcd7898699f4b11b3518c98fc55c1c59e6e4d8b667"

    net = NET.TESTNET
    bi = grpcclient.get_block_info(block_hash, net=net)

    assert bi.baker == 5
    assert bi.hash == block_hash
    assert bi.height == 3822240


def test_tx_block_info_random(grpcclient: GRPCClient):
    block_hash = "4033add0adee832883f0144517cfb82fd25823898273e8b8dc8854313325760b"

    net = NET.TESTNET
    bi = grpcclient.get_block_info(block_hash, net=net)
    print(bi)
    # assert bi.baker == 5
    # assert bi.hash == block_hash
    # assert bi.height == 3822240
    # # assert (
    #     bi.last_finalized_block
    #     == "946cfc98e6c64a3102d5fce8f326f725104004e6405d9aadd543ef83b7eeef12"
    # )
    # assert (
    #     bi.parent_block
    #     == "946cfc98e6c64a3102d5fce8f326f725104004e6405d9aadd543ef83b7eeef12"
    # )
    # assert bi.slot_number == 13966389
    # assert bi.slot_time == dt.datetime(
    #     2023, 1, 22, 17, 58, 26, 750000, tzinfo=dt.timezone.utc
    # )
    # assert bi.era_block_height == 344569
    # assert bi.finalized == True
    # assert bi.genesis_index == 4
    # assert bi.transaction_count == 9
    # assert bi.transactions_energy_cost == 4455
    # assert bi.transactions_size == 1494

    # print(bi.dict(exclude_none=True))
