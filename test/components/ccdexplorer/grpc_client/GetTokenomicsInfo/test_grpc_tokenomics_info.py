# pyright: reportOptionalMemberAccess=false
import pytest
import datetime as dt


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_tokenomics_info_v0(grpcclient: GRPCClient):
    block_hash = "8f2a5150c20ce84d9ae98f940f7ab0ed1aa45978019d5ae9012b55c106bdafd8"
    ti = grpcclient.get_tokenomics_info(block_hash)

    assert ti.v0.total_amount == 10310428389435147
    assert ti.v0.total_encrypted_amount == 0
    assert ti.v0.baking_reward_account == 73606304794
    assert ti.v0.finalization_reward_account == 4
    assert ti.v0.gas_account == 3
    assert ti.v0.protocol_version == 0


def test_tokenomics_info_v1(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    ti = grpcclient.get_tokenomics_info(block_hash)
    print(ti)  # .dict(exclude_none=True))
    assert ti.v1.total_amount == 11671992054603492
    assert ti.v1.total_encrypted_amount == 185491140399
    assert ti.v1.baking_reward_account == 417
    assert ti.v1.finalization_reward_account == 23
    assert ti.v1.gas_account == 11383330
    assert ti.v1.foundation_transaction_rewards == 1843098711
    assert ti.v1.next_payday_time == dt.datetime(
        2023, 1, 23, 8, 5, 9, 500000, tzinfo=dt.timezone.utc
    )
    assert ti.v1.next_payday_mint_rate.mantissa == 261157877
    assert ti.v1.next_payday_mint_rate.exponent == 12
    assert ti.v1.total_staked_capital == 8663567331383744
    assert ti.v1.protocol_version == 4


def test_tokenomics_info_v1_last(grpcclient: GRPCClient):
    block_hash = "last_final"
    ti = grpcclient.get_tokenomics_info(block_hash)
    print(ti)
