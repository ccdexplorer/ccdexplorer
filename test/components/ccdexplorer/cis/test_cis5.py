from ccdexplorer.grpc_client.core import GRPCClient
import pytest
from ccdexplorer.cis import CIS


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


def test_cis5_deposit_ccd_event(cis: CIS):
    # from hash d01a6d9447f6f36ac7d2a00e8fec3f40e897a705ffaa9ae6298df99304f20f7b
    hex = "f9c0c408220000000001dd2500000000000000000000000000000c16d304b277522f7a4fcd839ce7ea9334b525388d373660b1fc86c5401a3101"
    parsed_result = cis.depositCCDEvent(hex)
    assert parsed_result.tag == 249
    assert parsed_result.ccd_amount == 571000000
    assert parsed_result.from_address == "<9693,0>"
    assert (
        parsed_result.to_public_key_ed25519
        == "0c16d304b277522f7a4fcd839ce7ea9334b525388d373660b1fc86c5401a3101"
    )


def test_cis5_transfer_ccd_event(cis: CIS):
    # from hash d4cc213b54b10b0dfa2c6a95e4e850ed9b92d465f45082bf59014e1830375f7f
    hex = "f50000000000000000c18c1cb58a9208343b67a3b9abadf542bf61c7527124e22092ed4bc1895166c4b288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec"
    parsed_result = cis.transferCCDEvent(hex)
    assert parsed_result.tag == 245
    assert parsed_result.ccd_amount == 0
    assert (
        parsed_result.from_public_key_ed25519
        == "c18c1cb58a9208343b67a3b9abadf542bf61c7527124e22092ed4bc1895166c4"
    )
    assert (
        parsed_result.to_public_key_ed25519
        == "b288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec"
    )


def test_cis5_witdhdraw_ccd_event(cis: CIS):
    # from hash d4cc213b54b10b0dfa2c6a95e4e850ed9b92d465f45082bf59014e1830375f7f
    hex = "f700d1a92000000000c18c1cb58a9208343b67a3b9abadf542bf61c7527124e22092ed4bc1895166c401dd250000000000000000000000000000"
    parsed_result = cis.withdrawCCDEvent(hex)
    assert parsed_result.tag == 247
    assert parsed_result.ccd_amount == 548000000
    assert (
        parsed_result.from_public_key_ed25519
        == "c18c1cb58a9208343b67a3b9abadf542bf61c7527124e22092ed4bc1895166c4"
    )
    assert parsed_result.to_address == "<9693,0>"


def test_cis5_deposit_cis2_tokens_event(cis: CIS):
    # from hash 7339f99a5d8c92a4ef2d337acf8ec504ee0f79e3c12bc60812cf69e7c5161977
    hex = "f880ade204000f25000000000000000000000000000000089c41280cc4940d8a82267c061c090929579f1506ea7f2407d07c27004b6597aad96b04b9f4dc1e524d59eeb066686ccaef2eef083389c030fdb33ac9c3a1f7"
    parsed_result = cis.depositCIS2TokensEvent(hex)
    assert parsed_result.tag == 248
    assert parsed_result.cis2_token_contract_address == "<9487,0>"
    assert parsed_result.token_amount == 10000000
    assert parsed_result.token_id == ""
    assert parsed_result.from_address == "31Y7mhjzj5tfrqMXPfqoqRNkZgP7VBjPe8xXQb6xeGtrjwVx6V"
    assert (
        parsed_result.to_public_key_ed25519
        == "aad96b04b9f4dc1e524d59eeb066686ccaef2eef083389c030fdb33ac9c3a1f7"
    )


def test_cis5_withdraw_cis2_tokens_event(cis: CIS):
    # from hash 88bc0df40de3d9173dd52223c84cf13a0b15e8c25fa805c0ea6e0c3c6748a6ba
    hex = "f680ade204000f2500000000000000000000000000000575166bc75da92bbf7b6c54bf96635177c7f00fd3681653fc84994eeea0b1db00089c41280cc4940d8a82267c061c090929579f1506ea7f2407d07c27004b6597"
    parsed_result = cis.withdrawCIS2TokensEvent(hex)
    assert parsed_result.tag == 246
    assert parsed_result.cis2_token_contract_address == "<9487,0>"
    assert parsed_result.token_amount == 10000000
    assert parsed_result.token_id == ""
    assert (
        parsed_result.from_public_key_ed25519
        == "0575166bc75da92bbf7b6c54bf96635177c7f00fd3681653fc84994eeea0b1db"
    )
    assert parsed_result.to_address == "31Y7mhjzj5tfrqMXPfqoqRNkZgP7VBjPe8xXQb6xeGtrjwVx6V"


def test_cis5_transfer_cis2_tokens_event(cis: CIS):
    # from hash b70019a8dfcb286d5efa32ee29087bee6c73041c95d3c634cd07f12fa3f9082c
    hex = "f400000f250000000000000000000000000000a45d7555ea8c73e37600e6ee9d3ca7c0ac711148dbcb5b98df174d575633ca2eb288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec"
    parsed_result = cis.transferCIS2TokensEvent(hex)
    assert parsed_result.tag == 244
    assert parsed_result.cis2_token_contract_address == "<9487,0>"
    assert parsed_result.token_amount == 0
    assert parsed_result.token_id == ""
    assert (
        parsed_result.from_public_key_ed25519
        == "a45d7555ea8c73e37600e6ee9d3ca7c0ac711148dbcb5b98df174d575633ca2e"
    )
    assert (
        parsed_result.to_public_key_ed25519
        == "b288c8518c8be158e5e22cb1ee8c748b1992a2cb3572643a7b6ceb1ccd6bf3ec"
    )


def test_cis5_nonce_event(cis: CIS):
    # from hash d4cc213b54b10b0dfa2c6a95e4e850ed9b92d465f45082bf59014e1830375f7f
    hex = "fa0e00000000000000c18c1cb58a9208343b67a3b9abadf542bf61c7527124e22092ed4bc1895166c4"
    parsed_result = cis.nonceEventCIS5(hex)
    assert parsed_result.tag == 250
    assert parsed_result.nonce == "14"  # Nonce can become too big, so store as str.
    assert (
        parsed_result.sponsoree
        == "c18c1cb58a9208343b67a3b9abadf542bf61c7527124e22092ed4bc1895166c4"
    )
