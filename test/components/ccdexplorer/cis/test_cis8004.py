from ccdexplorer.grpc_client.core import GRPCClient
import pytest
from ccdexplorer.cis import CIS


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


def test_cis8004_uri_updated_event(cis: CIS):
    # from tx hash ecc775b47014eb0c607d000da3c63de355c561d15d317eff27a2bb48d300fecf MAINNET
    # contract 10082/0, events[0], receive_name CIS-8004.setAgentURI
    # trailing 41 bytes: optional 32-byte URI hash + 8-byte slot time (1779959112514 ms)
    hex = (
        "f1080e00000000000000"
        "01"
        "60000000"
        "68747470733a2f2f6167656e7463617264732e736974652f"
        "33376858383134597a337a626568484d67786f7769586f573763657a38"
        "5268353467684e62624376656f4b4d66626d3746682f"
        "613939643139383066653632366231382e6a736f6e"
        "01a99d1980fe626b187cce6c8558bb37c231820c9d705a2c54607c0b474bc619bd"
        "42a3d46d9e010000"
    )
    parsed_result = cis.cis8004URIUpdatedEvent(hex)
    assert parsed_result.tag == 241
    assert parsed_result.agent_token_id == "0e00000000000000"
    assert parsed_result.agent_uri == (
        "https://agentcards.site/"
        "37hX814Yz3zbehHMgxowiXoW7cez8Rh54ghNbbCveoKMfbm7Fh/"
        "a99d1980fe626b18.json"
    )
    assert parsed_result.metadata_hash == (
        "a99d1980fe626b187cce6c8558bb37c231820c9d705a2c54607c0b474bc619bd"
    )


def test_cis8004_registered_event_with_metadata_hash(cis: CIS):
    # physid.ai agent — metadata_hash present, ext_ref absent
    # field order: uri → metadata_hash → ext_ref (implementation adds hash before spec ext_ref)
    hex = "f00811000000000000007afb28bbd5b5ce42efdcd00b536235d6a6517d2055b39f8b403999f88a422937013200000068747470733a2f2f7068797369642e61692f2e77656c6c2d6b6e6f776e2f6167656e74732f72656769737472792e6a736f6e011b2d2bbb830014910b9549bc224edbd7ccf95e48250f40ea40ab37284e4cdf5c00dbbb017d9e010000"
    parsed_result = cis.cis8004RegisteredEvent(hex)
    assert parsed_result.tag == 240
    assert parsed_result.agent_token_id == "1100000000000000"
    assert parsed_result.agent_uri == "https://physid.ai/.well-known/agents/registry.json"
    assert parsed_result.metadata_hash == (
        "1b2d2bbb830014910b9549bc224edbd7ccf95e48250f40ea40ab37284e4cdf5c"
    )
    assert parsed_result.external_reference is None


def test_cis8004_registered_event(cis: CIS):
    # from tx hash 68eec1a04f0dc80733c3d9e24f37e809fafdcc5acd5dcda04ab93b63093ed8a2 MAINNET
    # contract 10082/0, events[1], receive_name CIS-8004.register
    # Note: trailing 9 bytes after ext_ref are extra contract data not in spec (likely a timestamp)
    hex = (
        "f0080e00000000000000"
        "169a29e682453f43202a10bbb21b4c5813cff1662b9ca3b07d82f097e7ce154b"
        "01"
        "60000000"
        "68747470733a2f2f6167656e7463617264732e736974652f"
        "33376858383134597a337a626568484d67786f7769586f573763657a38"
        "5268353467684e62624376656f4b4d66626d3746682f"
        "386164383539646431393961646466302e6a736f6e"
        "00"
        "006274d46d9e010000"
    )
    parsed_result = cis.cis8004RegisteredEvent(hex)
    assert parsed_result.tag == 240
    assert parsed_result.agent_token_id == "0e00000000000000"
    assert parsed_result.owner == "37hX814Yz3zbehHMgxowiXoW7cez8Rh54ghNbbCveoKMfbm7Fh"
    assert parsed_result.agent_uri == (
        "https://agentcards.site/"
        "37hX814Yz3zbehHMgxowiXoW7cez8Rh54ghNbbCveoKMfbm7Fh/"
        "8ad859dd199addf0.json"
    )
    assert parsed_result.external_reference is None
    assert parsed_result.metadata_hash is None
