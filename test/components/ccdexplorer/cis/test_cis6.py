from ccdexplorer.grpc_client.core import GRPCClient
import pytest
from ccdexplorer.cis import CIS
from ccdexplorer.domain.cis import MetadataUrl


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


def test_cis6_create_item_event(cis: CIS):
    # from hash 2f0543677e2e2cfce44ae5d91e21432d52e7cdd1647378ed8e0b9c9b6488b0f7 TESTNET
    hex = "ed0801000000000000000000"
    parsed_result = cis.ItemCreatedEvent(hex)
    assert parsed_result.tag == 237
    assert parsed_result.item_id == "0100000000000000"
    assert parsed_result.initial_status == 0
    assert parsed_result.metadata == MetadataUrl(url="", checksum=None)


def test_cis6_item_changd_event(cis: CIS):
    # from hash 0615e505bb2c16819d7ac102bd106f13c230bee67fa701e5758a8ac482b5c675 TESTNET
    hex = "ec0813000000000000000300000000"
    parsed_result = cis.ItemStatusChangedEvent(hex)
    assert parsed_result.tag == 236
    assert parsed_result.item_id == "1300000000000000"
    assert parsed_result.new_status == 3
