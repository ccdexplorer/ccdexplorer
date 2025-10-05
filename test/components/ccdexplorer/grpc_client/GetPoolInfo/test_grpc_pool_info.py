# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_pool_info(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    pool_id = 72723
    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash)
    print(pi.dict(exclude_none=True))
    assert pi.address == "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm"
    assert pi.all_pool_total_capital == 8663567331383744
    assert pi.equity_capital == 1099583249741
    assert pi.equity_pending_change.reduce is None
    assert pi.equity_pending_change.remove is None
    assert pi.current_payday_info.baker_equity_capital == 1099037072012
    assert pi.current_payday_info.blocks_baked == 2
    assert pi.current_payday_info.delegated_capital == 892701163378
    assert pi.delegated_capital == 893070479201
    assert pi.pool_info.commission_rates.baking == 0.1
    assert pi.pool_info.open_status == "open_for_all"


def test_pool_info_payday(grpcclient: GRPCClient):
    block_hash = "d2735387725c426c85e5242177f80b12a44c1ef6aaa8cc7453a7ed83f912dc10"
    pool_id = 84252
    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash)
    print(pi.dict(exclude_none=True))
    assert pi.address == "3FYJZJkxgMtby5tqkwhq8dKWztLkAJYdVKKR3EbgNkNPbuN9Bi"
    assert pi.equity_capital == 36900000000
    # assert pi.current_payday_info.blocks_baked == 0
    # assert pi.current_payday_info.delegated_capital == 0
    assert pi.delegated_capital == 0
    assert pi.pool_info.commission_rates.baking == 0.1
    assert pi.pool_info.open_status == "open_for_all"


def test_pool_info_baked_block(grpcclient: GRPCClient):
    block_hash = "f6506bfe7ee969a3cc9246b9dc25fee65569e62cb09d820678ed90c77164359c"
    pool_id = 704
    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash)
    print(pi.dict(exclude_none=True))
    assert pi.current_payday_info.blocks_baked == 18


def test_pool_info_baked_block_72723(grpcclient: GRPCClient):
    block_hash_before = "aa6ea499c38bc1cd175f48bdf8d0a756d81439e6b7e24d92f4c32467c067e115"
    block_hash_baked_block = "d946715c8cb68c8f0bba795d138c8f06a17f269ab033f45ecbaaf248b36ba5b5"
    block_hash_after = "05ae5143a57560ee85a78af0ea9288a370dcbf908d78ccf669d3647b68118208"
    pool_id = 72723
    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash_before)
    assert pi.current_payday_info.blocks_baked == 0

    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash_baked_block)
    assert pi.current_payday_info.blocks_baked == 1

    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash_after)
    assert pi.current_payday_info.blocks_baked == 1


def test_pool_info_baker_7273(grpcclient: GRPCClient):
    block_hash = "5aaca3e7e1645b0d9a3999c80ca17629e6a0e6dc57cb297525f0870aa43c2701"
    pool_id = 72723

    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash)
    print(pi.model_dump(exclude_none=True))
    # assert pi.current_payday_info.blocks_baked == 18


def test_pool_info_baker_476(grpcclient: GRPCClient):
    block_hash = "last_final"
    pool_id = 476

    pi = grpcclient.get_pool_info_for_pool(pool_id, block_hash)
    print(pi.model_dump(exclude_none=True))
    # assert pi.current_payday_info.blocks_baked == 18
