from ccdexplorer.grpc_client.core import GRPCClient
import pytest
from ccdexplorer.cis import CIS
from ccdexplorer.domain.cis import MetadataUrl
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


# def test_contract_0_transfer(cis: CIS):
#     entrypoint = "inventory.transfer"

#     cis.instance_index = 0
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "030000df9131e3046c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb"
#     parsed_result = cis.s7_parameter(hex)
#     assert parsed_result.account_address == "3mQJ5AmBrd8tEmoHimahhCWqQZxQrKwNNtBygbbtGk3uDNk8YD"


# def test_contract_0_get_token(cis: CIS):
#     entrypoint = "inventory.get_token"

#     cis.instance_index = 0
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "030000df9131e304006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb0010a5d4e800000013007472616465722e6275795f63616c6c6261636b"
#     parsed_result = cis.s7_inventory_get_token(hex)
#     assert parsed_result.account_address == "3mQJ5AmBrd8tEmoHimahhCWqQZxQrKwNNtBygbbtGk3uDNk8YD"


def test_contract_0_buy_callback(cis: CIS):
    entrypoint = "trader.buy_callback"

    cis.instance_index = 378
    cis.instance_subindex = 0
    cis.entrypoint = entrypoint
    cis.net = NET.MAINNET
    hex = "030000df9131e3044e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf11304e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf11300a00000000000000006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb0010a5d4e8000000"
    parsed_result = cis.s7_trader_buy_callback(hex)
    assert parsed_result.amount == 1000000000000
    assert parsed_result.sender == "3mQJ5AmBrd8tEmoHimahhCWqQZxQrKwNNtBygbbtGk3uDNk8YD"
    assert parsed_result.creator == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
    assert parsed_result.custom_token_id == 352179698446368771


def test_s7_inventory_create(cis: CIS):
    entrypoint = "inventory.create"

    cis.instance_index = 0
    cis.instance_subindex = 0
    cis.entrypoint = entrypoint
    cis.net = NET.MAINNET
    hex = "030000df9131e304000a00000000000000"
    parsed_result = cis.s7_inventory_create_erc721_v1(hex)
    # assert parsed_result.creator == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
    assert parsed_result.custom_token_id == 352179698446368771
    assert parsed_result.royalty_percent == 2560


# def test_s7_inventory_create_erc721_v2(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 2
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "09000086f92b2c0400000000000000000042000000697066733a2f2f6261666b726569686d7270667675776169797537347a6c6769346e6967346c687a7766647572716e72673567646e697575347272723334676c7a71"
#     parsed_result = cis.s7_inventory_create_erc721_v2(hex)
#     # assert parsed_result.creator == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
#     assert parsed_result.custom_token_id == 352179698446368771
#     assert parsed_result.royalty_percent == 2560


def test_s7_inventory_create_event(cis: CIS):
    entrypoint = "inventory.create"

    cis.instance_index = 0
    cis.instance_subindex = 0
    cis.entrypoint = entrypoint
    cis.net = NET.MAINNET
    hex = "01030000df9131e304004e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf1130"
    parsed_result = cis.s7_inventory_transfer_event_erc721_v1(hex)
    assert parsed_result.to_ == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
    assert parsed_result.custom_token_id == 352179698446368771
