import pytest
import datetime as dt
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client.CCD_Types import CCD_AccountPending


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_cooldown_accounts(grpcclient: GRPCClient):
    ca = grpcclient.get_cooldown_accounts(24200160, net=NET.TESTNET)
    assert len(ca) == 3
    assert isinstance(ca[0], CCD_AccountPending)
    assert ca[0].account_index == 11411
    assert ca[0].first_timestamp == dt.datetime(
        2025, 2, 27, 12, 0, 24, 500000, tzinfo=dt.timezone.utc
    )


def test_precooldown_accounts(grpcclient: GRPCClient):
    bi = grpcclient.get_pre_cooldown_accounts(24200160, net=NET.TESTNET)
    print(f"Blocks: {bi}")


def test_preprecooldown_accounts(grpcclient: GRPCClient):
    bi = grpcclient.get_pre_pre_cooldown_accounts("last_final", net=NET.TESTNET)
    print(f"Blocks: {bi}")
