# ruff: noqa :E501, F403, F405


import pytest


import io

from rich import print
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
from ccdexplorer.domain.generic import StandardIdentifiers
from ccdexplorer.cis import CIS
from ccdexplorer.cns import CNSDomain
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient

# from ccdexplorer.grpc_client.queries import _SharedConverters


@pytest.fixture
def grpcclient():
    return GRPCClient()


def translate_hex_to_bytes(parameter_hex: str) -> bytes:
    # parameter_hex = "0x" + parameter_hex
    # parameter_bytes = list(bytes.fromhex(parameter_hex[2:]))
    parameter_bytes = list(bytes.fromhex(parameter_hex))
    parameter_bytes.insert(0, 32)
    return bytes(parameter_bytes)


def test_invoke_instance_from_token_to_name(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 7073
    instance_subindex = 0
    entrypoint = "BictoryCnsNft.getTokenInfo"
    parameter_bytes = translate_hex_to_bytes(
        "7569ec94880f9697ee632b667fb21fb8a2f4527bb3e568bfdf2ea4752ee60946"
    )

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
    )
    res = ii.success.return_value
    bb = list(bytes.fromhex(res.decode()))
    domain_name = bytes(bb[5:-8]).decode()
    assert domain_name == "explorer.ccd"
    print(ii)


def test_invoke_instance_from_name_to_owner(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 7073
    instance_subindex = 0
    entrypoint = "BictoryCnsNft.getTokenExpiry"
    parameter_bytes = translate_hex_to_bytes(
        "7569ec94880f9697ee632b667fb21fb8a2f4527bb3e568bfdf2ea4752ee60946"
    )

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
    )
    res = ii.success.return_value
    bb = list(bytes.fromhex(res.decode()))
    byt = io.BytesIO(bytes(bb))
    (status, to) = CNSDomain().read_domain_owner(byt)  # type: ignore
    assert to == "49HsifoFPMGtsYiAQW2RppQcnQsLm8hVeksaA8XBCPg8ttaTCa"
    print(to)


def test_invoke_instance_provenance(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 2391
    instance_subindex = 0
    entrypoint = "Provenance-tag.view_state"
    parameter_bytes = translate_hex_to_bytes(
        "02882b901364fb58c8219c79f48557944ac47b6c8b67fc7a14595ef45c7939a0"
    )

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
        net=NET.TESTNET,
    )
    res = ii.success.return_value
    print(res)
    bb = list(bytes.fromhex(res.decode()))
    print(bb)
    # (status, to) = CNSDomain().read_domain_owner(byt)
    # assert to == "49HsifoFPMGtsYiAQW2RppQcnQsLm8hVeksaA8XBCPg8ttaTCa"
    # print(to)


# def test_invoke_instance_provenance_4433(grpcclient: GRPCClient):
#     block_hash = "last_final"
#     instance_index = 5
#     instance_subindex = 0
#     entrypoint = "Provenance-tag.supports"
#     entrypoint = "CIS2-NFT.supports"
#     ci = CIS()
#     parameter_bytes = ci.generate_supportsParameter()
#     # parameter_bytes = translate_hex_to_bytes(
#     #     "02882b901364fb58c8219c79f48557944ac47b6c8b67fc7a14595ef45c7939a0"
#     # )

#     ii = grpcclient.invoke_instance(  # noqa: F841
#         block_hash,
#         instance_index,
#         instance_subindex,
#         entrypoint,
#         parameter_bytes,
#         net=NET.TESTNET,
#     )
#     # pt = ProvenanceTag()
#     # rr = pt.generate_supportsParameter()

#     pass


# def test_invoke_instance_supports_9377(grpcclient: GRPCClient):
#     block_hash = "last_final"
#     instance_index = 9377
#     instance_subindex = 0
#     entrypoint = "ccd.supports"

#     ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
#     parameter_bytes = ci.supports_parameter(StandardIdentifiers.CIS_2)

#     ii = grpcclient.invoke_instance(
#         block_hash,
#         instance_index,
#         instance_subindex,
#         entrypoint,
#         parameter_bytes,
#     )
#     res = ii.success.return_value
#     support_result = ci.supports_response(res)
#     print(support_result)

#     print(ii.dict(exclude_none=True))


# def test_invoke_instance_tokenMetadata_9377(grpcclient: GRPCClient):
#     block_hash = "last_final"
#     instance_index = 9377
#     instance_subindex = 0
#     entrypoint = "ccd.tokenMetadata"

#     ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
#     parameter_bytes = bytes("'tokenID': '83200000'", encoding="ASCII")

#     ii = grpcclient.invoke_instance(
#         block_hash,
#         instance_index,
#         instance_subindex,
#         entrypoint,
#         parameter_bytes,
#     )
#     res = ii.success.return_value
#     support_result = ci.supports_response(res)
#     print(support_result)

#     print(ii.dict(exclude_none=True))


def test_invoke_instance_supports(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9334
    instance_subindex = 0
    entrypoint = "Climafi-Carbon-CIS2.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    parameter_bytes = ci.supports_parameter(StandardIdentifiers.CIS_2)

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
    )
    res = ii.success.return_value
    support_result = ci.supports_response(res)
    print(support_result)

    print(ii.dict(exclude_none=True))


def test_invoke_instance_supports_cis2_dsid(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9365
    instance_subindex = 0
    entrypoint = "cis2_dsid.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    parameter_bytes = ci.supports_parameter(StandardIdentifiers.CIS_2)

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
    )
    res = ii.success.return_value
    support_result = ci.supports_response(res)
    print(support_result)

    print(ii.dict(exclude_none=True))


def test_invoke_instance_supports_cis3(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9336
    instance_subindex = 0
    entrypoint = "mysomeid.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    parameter_bytes = ci.supports_parameter(StandardIdentifiers.CIS_3)

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
    )
    res = ii.success.return_value
    support_result = ci.supports_response(res)
    print(support_result)

    print(ii.dict(exclude_none=True))


def test_invoke_instance_supports_MOTODEX(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9333
    instance_subindex = 0
    entrypoint = "MOTODEX.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    # print(list(StandardIdentifiers))
    # print(list(LoggedEvents))
    for ss in StandardIdentifiers:
        print(f"{ss.value}")
        parameter_bytes = ci.supports_parameter(ss)

        ii = grpcclient.invoke_instance(
            block_hash,
            instance_index,
            instance_subindex,
            entrypoint,
            parameter_bytes,
        )
        res = ii.success.return_value
        lookup_result, support_result = ci.supports_response(res)
        print(f"{ss.value}: {lookup_result} | {support_result}")

    # print(ii.dict(exclude_none=True))


def test_invoke_instance_supports_CIS_6(grpcclient: GRPCClient):
    instance_index = 8901
    instance_subindex = 0
    entrypoint = "track_and_trace.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    print(ci.supports_standard(StandardIdentifiers.CIS_6))


def test_invoke_instance_supports_MOTODEX_clean(grpcclient: GRPCClient):
    instance_index = 4323
    instance_subindex = 0
    entrypoint = "MOTODEX.supports"
    # entrypoint = "Climafi-Carbon-CIS2.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    supports_cis_6 = ci.supports_standard(StandardIdentifiers.CIS_2)
    print(supports_cis_6)


def test_invoke_instance_supports_50(grpcclient: GRPCClient):
    instance_index = 50
    instance_subindex = 0
    entrypoint = "CIS2-wCCD.supports"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    print(ci.supports_standard(StandardIdentifiers.CIS_1))


def test_invoke_instance_view_history(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 4469
    instance_subindex = 0
    entrypoint = "provenance_tag_nft.view_owner_history"
    tokenID = "640b4238047a19b54126b69dbfa6c3f21febbaccd0dbabeed5d699a97870e459"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    parameter_bytes = ci.viewOwnerHistoryRequest(tokenID)

    ii = grpcclient.invoke_instance(
        block_hash,
        instance_index,
        instance_subindex,
        entrypoint,
        parameter_bytes,
        NET.TESTNET,
    )

    result = ii.success.return_value
    print(result)
    print(ci.viewOwnerHistoryResponse(result))

    # rr = ci.invoke_token_metadataUrl(tokenID)
    # print(ii.dict(exclude_none=True))


def test_invoke_instance_metadataURL(grpcclient: GRPCClient):
    instance_index = 4469
    instance_subindex = 0
    entrypoint = "provenance_tag_nft.tokenMetadata"
    tokenID = "640b4238047a19b54126b69dbfa6c3f21febbaccd0dbabeed5d699a97870e459"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    result = ci.invoke_token_metadataUrl(tokenID)
    print(result)


def test_cis_block16333(grpcclient: GRPCClient):
    instance_index = 9377
    instance_subindex = 0
    entrypoint = "ccd.tokenMetadata"
    tokenID = "70662000"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr = ci.invoke_token_metadataUrl(tokenID)
    print(rr)


def test_cis_metadata(grpcclient: GRPCClient):
    instance_index = 9755
    instance_subindex = 0
    entrypoint = "ptags.tokenMetadata"
    tokenID = "operator"

    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr = ci.invoke_token_metadataUrl(tokenID)
    print(rr)


def test_invoke_instance_balanceOf_tranfer_before_mint(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9338
    instance_subindex = 0
    entrypoint = "cis2-bridgeable.balanceOf"
    tokenID = ""
    account_id = "4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr[0]:,.0f} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)


def test_invoke_instance_balanceOf_poap(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9379
    instance_subindex = 0
    entrypoint = "poap.balanceOf"
    tokenID = "01"
    account_id = "3CLkmo5gaKbxNXjPdDyU3mtGobB5dvLphGySxbEALQGMH2fzm4"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])

    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr[0]} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)


def test_invoke_instance_balanceOf_provenance(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 8588
    instance_subindex = 0
    entrypoint = "provenance_tag_nft.balanceOf"
    tokenID = "379f909960e12a6203841caa89dd06a4fd689b379fffcd51791cc0b3809d6554"
    account_id = "3e4k3jejVcMDKUvtbHXzey5GCrDeagJXD7UYWf1m4PiHPp84FJ"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr[0]} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)


def test_invoke_instance_balanceOf_ft(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9330
    instance_subindex = 0
    entrypoint = "ft.balanceOf"
    tokenID = ""
    account_id = "4kx448ZVKoELUMZJqzxBW7vEKJeJyzagoK3FcBxK1Kuck7zKBA"
    account_id = "3TisM3rj9ceja4goMHGN3oeZy7XqiGYB6GqejReFM47GqQfGfh"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr[0]} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)


def test_invoke_instance_balanceOf_ft_9326(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9326
    instance_subindex = 0
    entrypoint = "ft.balanceOf"
    tokenID = ""

    account_id = "3TisM3rj9ceja4goMHGN3oeZy7XqiGYB6GqejReFM47GqQfGfh"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr[0]} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)


def test_invoke_instance_balanceOf_aesirx(grpcclient: GRPCClient):
    block_hash = "b7dd00cf2eb96e507d098d1bd58b88e73f945850cf5df7077fbde96f1d84f480"
    instance_index = 8602
    instance_subindex = 0
    entrypoint = "aesirx_web3_web3id.balanceOf"
    tokenID = "00000049"

    account_id = "3sRDTnphG8eaUMfFKhQdAJUwHYTLFui3nZPVtz4ucEcym1QVfL"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)
    assert rr == [1]


def test_invoke_instance_balanceOf_1856(grpcclient: GRPCClient):
    block_hash = "631a980c53528508a1bae6f34a69c2161564d8b2ff0d54de44ab8074423c7b17"
    instance_index = 1856
    instance_subindex = 0
    entrypoint = "nft_tutorial.balanceOf"
    tokenID = "00000111"

    account_id = "3bzmSxeKVgHR4M7pF347WeehXcu43kypgHqhSfDMs9SvcP5zto"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr[0]} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)
    assert rr == [0]

    account_id = "3AiAikC2v4H3Rv4L58oHvdX5N8LJoarsQwN3G7oNS7BoFsmt7N"
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.TESTNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, [account_id])
    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        print(
            f"{account_id} has {rr} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
        )
        print(ii.success)
    assert rr == [1]


def test_invoke_instance_balanceOf_EUROe(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9390
    instance_subindex = 0
    entrypoint = "euroe_stablecoin.balanceOf"
    tokenID = ""
    addresses = [
        "<9363,0>",
        "3oBosgGonmJYYUmEAtbPEuqiYcciLc1vqiQSJYa71SceEd8Zri",
        "3Xu1Lwkwyb1pGGquKiU8bSf3DZV9Rk6mE5KshYjUgdga8Jy3aD",
        "3qqr3uUHrLqYzTm4Ap5dxJpeovMvmRUQVN2wUj3ChMqLmU31Ua",
        "4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp",
        "3suZfxcME62akyyss72hjNhkzXeZuyhoyQz1tvNSXY2yxvwo53",
    ]
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.balanceOf(block_hash, tokenID, addresses)

    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        for index, address in enumerate(addresses):
            print(
                f"{address} has {rr[index]} tokens from <{instance_index},{instance_subindex}>-{tokenID}."
            )
        # print(ii.success)


# def test_list_cis2_instances(grpcclient: GRPCClient):
#     block_hash = "last_final"
#     il = grpcclient.get_instance_list(block_hash)

#     assert il[0] == CCD_ContractAddress(index=0, subindex=0)

#     instance_index = 9334
#     instance_subindex = 0
#     entrypoint = "Climafi-Carbon-CIS2.supports"

#     ci = CIS()
#     parameter_bytes = ci.supports_parameter(StandardIdentifiers.CIS_2)

#     ii = grpcclient.invoke_instance(
#         block_hash,
#         instance_index,
#         instance_subindex,
#         entrypoint,
#         parameter_bytes,
#     )
#     res = ii.success.return_value
#     support_result = ci.support_result(res)
#     print(support_result)
#     print(ii.dict(exclude_none=True))


def test_invoke_instance_balanceOf_CIS5(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9632
    instance_subindex = 0
    entrypoint = "gateway_smart_wallet.ccdBalanceOf"

    public_keys = [
        "005aa6fcf725349d4f0d1aa8959d26317f0e997fa3b45a5e3178c571d9e1d622",
        "b288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec",
        "67eab74667a263f842f715fcf2c223d4815643eb61eed0ddd656d8e3087c6563",
        "e7ce3cde50de74812b5a2c8575196e36e81c566c9d78ca05ce285b03450bcf3f",
        "a7e49c1c18c45ccc3440ee02a145bc0b35a718d73bbe8658697f93abd10ee96e",
    ]
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.CCDbalanceOf(block_hash, public_keys)

    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        for index, address in enumerate(public_keys):
            print(f"{address} has {rr[index]} tokens from <{instance_index},{instance_subindex}>")
        # print(ii.success)


def test_invoke_instance_CIS2balanceOf_CIS5(grpcclient: GRPCClient):
    block_hash = "last_final"
    instance_index = 9632
    instance_subindex = 0
    entrypoint = "gateway_smart_wallet.cis2BalanceOf"
    cis_2_contract = CCD_ContractAddress.from_index(9487, 0)
    token_id = ""
    public_keys = [
        "005aa6fcf725349d4f0d1aa8959d26317f0e997fa3b45a5e3178c571d9e1d622",
        "b288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec",
        "67eab74667a263f842f715fcf2c223d4815643eb61eed0ddd656d8e3087c6563",
        "b288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec",
        "a7e49c1c18c45ccc3440ee02a145bc0b35a718d73bbe8658697f93abd10ee96e",
        "4eacb875f06ce817263e9edfcd101776720000b96974c3aa9e5a8de28af20943",
    ]
    ci = CIS(grpcclient, instance_index, instance_subindex, entrypoint, NET.MAINNET)
    rr, ii = ci.CIS2balanceOf(block_hash, cis_2_contract, token_id, public_keys)

    if ii.failure.used_energy > 0:
        print(ii.failure)
    else:
        for index, address in enumerate(public_keys):
            print(f"{address} has {rr[index]} tokens from {cis_2_contract.to_str()}")
        # print(ii.success)
