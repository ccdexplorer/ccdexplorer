from ccdexplorer.grpc_client.core import GRPCClient
import pytest
from ccdexplorer.cis import CIS
from ccdexplorer.domain.cis import MetadataUrl, SchemaRef


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


# TODO: find transactions for the untested events...
# CIS-4.credential_schemaref_event No tx found
# CIS-4.credential_metadata_event No tx found


def test_cis4_issuer_metadata_event(cis: CIS):
    # from hash bbd385427d03a35aad7712a3e62b88a52bb133a85794e0d41e5d470b0a152dde
    hex = "f7590068747470733a2f2f74656c656772616d2d7765623369642e6d61696e6e65742e636f6e636f726469756d2e736f6674776172652f6a736f6e2d736368656d61732f63726564656e7469616c2d6d657461646174612e6a736f6e00"
    parsed_result = cis.issuerMetaDataEvent(hex)
    assert parsed_result.tag == 247
    assert parsed_result.metadata == MetadataUrl(
        url="https://telegram-web3id.mainnet.concordium.software/json-schemas/credential-metadata.json",
        checksum=None,
    )


def test_cis4_register_credential_event(cis: CIS):
    # from hash df183114e26ce17d3a7184116e07749c11da1f20fe346229933bca3be2264f01
    hex = "f953686beb67985cf532ab4ee7648c764585062417349b55877ebcbdaefae749dc5d0068747470733a2f2f74656c656772616d2d7765623369642e6d61696e6e65742e636f6e636f726469756d2e736f6674776172652f6a736f6e2d736368656d61732f4a736f6e536368656d61323032332d74656c656772616d2e6a736f6e000854656c656772616d590068747470733a2f2f74656c656772616d2d7765623369642e6d61696e6e65742e636f6e636f726469756d2e736f6674776172652f6a736f6e2d736368656d61732f63726564656e7469616c2d6d657461646174612e6a736f6e00"
    parsed_result = cis.registerCredentialEvent(hex)
    assert parsed_result.tag == 249
    assert (
        parsed_result.credential_id
        == "53686beb67985cf532ab4ee7648c764585062417349b55877ebcbdaefae749dc"
    )
    assert parsed_result.schema_ref == SchemaRef(
        url="https://telegram-web3id.mainnet.concordium.software/json-schemas/JsonSchema2023-telegram.json",
        checksum=None,
    )
    assert parsed_result.credential_type == "54656c656772616d"


def test_cis4_revoke_credential_event(cis: CIS):
    # from hash df183114e26ce17d3a7184116e07749c11da1f20fe346229933bca3be2264f01
    hex = "f8aa47b7d76de79a423dde1e1f5caf4dcab9703b6b7bb560017b4ff4ac64e1dc2d0100"
    parsed_result = cis.revokeCredentialEvent(hex)
    assert parsed_result.tag == 248
    assert (
        parsed_result.credential_id
        == "aa47b7d76de79a423dde1e1f5caf4dcab9703b6b7bb560017b4ff4ac64e1dc2d"
    )
    assert parsed_result.revoker == "Holder"
    assert parsed_result.reason is None


def test_cis4_revocation_key_event(cis: CIS):
    # from hash efe43ff723957f26b6b0dbd3a04469e344d38bbae5cfa7034851a5f7116a6820
    hex = "f48fe0dc02ffbab8d30410233ed58b44a53c418b368ae91cdcdbcdb9e79358be8200"
    parsed_result = cis.revocationKeyEvent(hex)
    assert parsed_result.tag == 244
    assert (
        parsed_result.public_key_ed25519
        == "8fe0dc02ffbab8d30410233ed58b44a53c418b368ae91cdcdbcdb9e79358be82"
    )
    assert parsed_result.action == "Register"
