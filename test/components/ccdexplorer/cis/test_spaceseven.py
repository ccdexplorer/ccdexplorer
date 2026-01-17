from ccdexplorer.grpc_client.core import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
import pytest
from ccdexplorer.cis import CIS
from ccdexplorer.domain.s7 import SpaceSevenEvents

from ccdexplorer.domain.cis import MetadataUrl
from ccdexplorer.domain.generic import NET


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


@pytest.fixture
def s7():
    return SpaceSevenEvents()


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


# def test_contract_0_buy_callback(cis: CIS):
#     entrypoint = "trader.buy_callback"

#     cis.instance_index = 378
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "030000df9131e3044e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf11304e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf11300a00000000000000006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb0010a5d4e8000000"
#     parsed_result = cis.s7_trader_buy_callback(hex)
#     assert parsed_result.amount == 1000000000000
#     assert parsed_result.sender == "3mQJ5AmBrd8tEmoHimahhCWqQZxQrKwNNtBygbbtGk3uDNk8YD"
#     assert parsed_result.creator == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
#     assert parsed_result.custom_token_id == 352179698446368771


# def test_s7_inventory_create_378(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 0
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "030000df9131e304000a00000000000000"
#     parsed_result = cis.s7_inventory_create_erc721_v1(hex)
#     # assert parsed_result.creator == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
#     assert parsed_result.custom_token_id == 352179698446368771
#     assert parsed_result.royalty_percent == 2560


# def test_s7_inventory_create_21(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 20
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "0d00003622484c0001b6e60b502a3697903dee496a91696ef3d5a5bd143031f8af2111ec25a48e5c8d000000000000000042000000697066733a2f2f6261666b726569667079766d716b6c6f61726c7732636f7270687a706b376c7a65797a35366d6c7562706d6d68356477746e676136346274346d75"
#     parsed_result = cis.s7_inventory_create_erc721_v2_create_parameter(hex)
#     assert parsed_result.creator == "4LJ5QS8RW26jAgbeaAZp9zBXNRexr6wpnvbgsp5Czib6FLkP3D"
#     assert parsed_result.custom_token_id == 21471410002067469
#     assert parsed_result.url == "ipfs://bafkreifpyvmqkloarlw2corphzpk7lzeyz56mlubpmmh5dwtnga64bt4mu"


# def test_s7_trader_create_and_sell_21(cis: CIS):
#     entrypoint = "trader.create_and_sell"

#     cis.instance_index = 21
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "0d00003622484c00000000000000000042000000697066733a2f2f6261666b726569667079766d716b6c6f61726c7732636f7270687a706b376c7a65797a35366d6c7562706d6d68356477746e676136346274346d75008c7d0a0000000060e2b7127e0100000000000000000000"
#     parsed_result = cis.s7_trader_create_and_sell_erc721_v2(hex)
#     assert parsed_result.custom_token_id == 21471410002067469
#     assert parsed_result.price == 176000000


# def test_s7_trader_create_and_sell_21_2(cis: CIS):
#     entrypoint = "trader.create_and_sell"

#     cis.instance_index = 8
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "0d00003622484c00000000000000000042000000697066733a2f2f6261666b726569667079766d716b6c6f61726c7732636f7270687a706b376c7a65797a35366d6c7562706d6d68356477746e676136346274346d75008c7d0a0000000060e2b7127e0100000000000000000000"
#     parsed_result = cis.s7_trader_create_and_sell_erc1155_v1(hex)
#     assert parsed_result.custom_token_id == 21471410002067469
#     assert parsed_result.price == 176000000


# def test_s7_trader_create_8(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 8
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "0900009a5f9d4200020000000000000000000000000000000042000000697066733a2f2f6261666b7265696264616e65346c6a7978336c773762356d6f6a6c7735366933777936666a6561696479626a3368627337726c7334776332737534"
#     parsed_result = cis.s7_inventory_create_erc1155_v1(hex)
#     assert parsed_result.custom_token_id == 18750382394048521


# # def test_s7_inventory_create_erc721_v2(cis: CIS):
# #     entrypoint = "inventory.create"

# #     cis.instance_index = 2
# #     cis.instance_subindex = 0
# #     cis.entrypoint = entrypoint
# #     cis.net = NET.MAINNET
# #     hex = "09000086f92b2c0400000000000000000042000000697066733a2f2f6261666b726569686d7270667675776169797537347a6c6769346e6967346c687a7766647572716e72673567646e697575347272723334676c7a71"
# #     parsed_result = cis.s7_inventory_create_erc721_v2(hex)
# #     # assert parsed_result.creator == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
# #     assert parsed_result.custom_token_id == 352179698446368771
# #     assert parsed_result.royalty_percent == 2560


# def test_s7_inventory_create_event(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 0
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "01030000df9131e304004e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf1130"
#     parsed_result = cis.s7_inventory_transfer_event_erc721_v1(hex)
#     assert parsed_result.to_ == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
#     assert parsed_result.custom_token_id == 352179698446368771


# def test_s7_inventory_create_3(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 3
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "0800009ed89c8d03014feb8e5998fa692316d315a8f3c50b673b5ccb57f52180dc1abb512a117fb132000000000000000042000000697066733a2f2f6261666b726569656673696a74686e6c376d356d74796c36776163676836326f7a67377369357179737435716b7568786234716c37703773633734"
#     parsed_result = cis.s7_inventory_create_erc721_v2_create_parameter(hex)
#     # assert parsed_result.creator == "4LJ5QS8RW26jAgbeaAZp9zBXNRexr6wpnvbgsp5Czib6FLkP3D"
#     # assert parsed_result.custom_token_id == 21471410002067469
#     # assert parsed_result.url == "ipfs://bafkreifpyvmqkloarlw2corphzpk7lzeyz56mlubpmmh5dwtnga64bt4mu"


# def test_s7_inventory_create_event_8(cis: CIS):
#     entrypoint = "inventory.create"

#     cis.instance_index = 8
#     cis.instance_subindex = 0
#     cis.entrypoint = entrypoint
#     cis.net = NET.MAINNET
#     hex = "0900009a5f9d4200020000000000000000000000000000000042000000697066733a2f2f6261666b7265696264616e65346c6a7978336c773762356d6f6a6c7735366933777936666a6561696479626a3368627337726c7334776332737534"
#     parsed_result = cis.s7_inventory_create_erc1155_v1_create_parameter(hex)
#     assert parsed_result.creator is None
#     assert parsed_result.royalty_percent == 0
#     assert parsed_result.url == "ipfs://bafkreibdane4ljyx3lw7b5mojlw56i3wy6fjeaidybj3hbs7rls4wc2su4"
#     assert parsed_result.custom_token_id == 18750382394048521


# def test_s7_inventory_safe_transfer_from_event_2(cis: CIS):
#     hex = "0d0000548adbc40143238228c97ec6c20d0bc73e83dcde447333ae236550dd67ffd7794a2d82163d00"
#     parsed_result = cis.s7_inventory_safe_transfer_from_erc721_v2_parameter(hex)
#     assert parsed_result.from_ == "3TK9P9wmDTL2tnB6D56CVJJv88BG9TV9HQJD5sMfwEq5BXfJXz"
#     assert parsed_result.to_ is None
#     assert parsed_result.custom_token_id == 127468076634472461


# def test_s7_inventory__transfer_from_event_2(cis: CIS):
#     hex = "09000037d4adf60288992a2b3917b5c69a1843db5479fa3cdc3ea1221a60e2a3faf5cfe6c178ac5f014feb8e5998fa692316d315a8f3c50b673b5ccb57f52180dc1abb512a117fb132"
#     parsed_result = cis.s7_inventory_safe_transfer_from_erc721_v2_parameter(hex)
#     assert parsed_result.from_ == "3yuQ1JpjrY1Mk8Ftj5M5i3NiTViRuKAWNMPUMBaSHnubCMkw2M"
#     assert parsed_result.to_ == "3YwdVSnCvkBohd8dkW44Fn4oMU524h452PkBYecKmkkYNayp5F"
#     assert parsed_result.custom_token_id == 213549159314096137


def test_s7_erc721_v1_inventory_transfer_event_0(s7: SpaceSevenEvents, cis: CIS):
    hex = "01030000df9131e304014e7407cb2d660d9943608965b8197328d61497d3b9570268136d782357bf11306c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb"
    parsed_result = s7.s7_erc721_v1_inventory_transfer_event(cis, hex)
    assert parsed_result.type == 1
    assert parsed_result.custom_token_id == 352179698446368771
    assert parsed_result.from_ == "3YJAEf8Ah7EaAjMvRrYL2xtxjhEezfmQcWnhKbYBiSjQmCbGG3"
    assert parsed_result.to_ == "3mQJ5AmBrd8tEmoHimahhCWqQZxQrKwNNtBygbbtGk3uDNk8YD"


def test_s7_erc721_v1_inventory_on_sale_event_0(s7: SpaceSevenEvents, cis: CIS):
    # tx 5a641ec7121381c0e75caeac34358c1afb64a931d13409dc25d21a4fdfeda7fa
    hex = "04030000df9131e304006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb7a010000000000000000000000000000"
    parsed_result = s7.s7_erc721_v1_inventory_on_sale_event(cis, hex)
    assert parsed_result.type == 4
    assert parsed_result.custom_token_id == 352179698446368771
    assert parsed_result.trader == CCD_ContractAddress.from_index(378, 0)


def test_s7_erc721_v1_inventory_on_sale_event_0_0(s7: SpaceSevenEvents, cis: CIS):
    # tx ef69378175d83c31bcd9eed41a31ffe686a611d3082802a119db505021e67f83
    hex = "0404000036df4eca00006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb7a010000000000000000000000000000"
    parsed_result = s7.s7_erc721_v1_inventory_on_sale_event(cis, hex)
    assert parsed_result.type == 4
    assert parsed_result.custom_token_id == 56944665886195716
    assert parsed_result.trader == CCD_ContractAddress.from_index(378, 0)


# 04030000df9131e304 006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb7a010000000000000000000000000000
# 0404000036df4eca00 006c3625e2f83705219cfb438bda6a74b0c8adbd25554f3f145bf2109eb67d16cb7a010000000000000000000000000000
