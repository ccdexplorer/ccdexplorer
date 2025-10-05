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


def test_cis2_mint_event(cis: CIS):
    # from hash 5382e43c9fe252d66b56b00a7ce84168531336118cf37c6fa09d21b3c79e1db6
    hex = "fe04619d104201005a398a949d41d7569a48051d34795ffa945dcf0afa4501689421c255a327d2db"
    parsed_result = cis.mintEvent(hex)
    assert parsed_result.tag == 254
    assert parsed_result.token_id == "619d1042"
    assert parsed_result.token_amount == 1
    assert parsed_result.to_address == "3dUr8c15HUhe2qUyAFghWt7g4EiRxdngDXKAzHxS3JbzWnr53F"


def test_cis2_transfer_event(cis: CIS):
    # from hash 8cdc4154bb56561699ddbfeae6b10814957be31cabe046b1b5087d91ccf115c8
    hex = "ff00ebcc030038163ebd41da206466a4593a1e3f3bdda81e774e22db299be83ee72582a7794d0193240000000000000000000000000000"
    parsed_result = cis.transferEvent(hex)
    assert parsed_result.tag == 255
    assert parsed_result.token_id == ""
    assert parsed_result.token_amount == 58987
    assert parsed_result.from_address == "3NSqmfYMUZLdA9jDYxccZMyh7gNrn64h7MTWy16u3xzq2Po78Y"
    assert parsed_result.to_address == "<9363,0>"


def test_cis2_burn_event(cis: CIS):
    # from hash 564a5dac400aba555e6a4a93478615f0cff81df9cfe61aff8a6bfe9c05827327
    hex = "fd008df980bdf6f09ad01b0035882ab4503e84735b486d981e517356bd138f8d64dee169a61243ab4537a0f4"
    parsed_result = cis.burnEvent(hex)
    assert parsed_result.tag == 253
    assert parsed_result.token_id == ""
    assert parsed_result.token_amount == 1990709264601070733
    assert parsed_result.from_address == "3MKaTzst2Rp7oDdoYr98ykMbti1Sydr9mddqwDZ3TTW39nAWsL"


def test_cis2_metadata_event(cis: CIS):
    # from hash 5382e43c9fe252d66b56b00a7ce84168531336118cf37c6fa09d21b3c79e1db6
    hex = "fb04619d1042440068747470733a2f2f697066732e696f2f697066732f516d566b513958734a63674d57357a38624367666f57726b3565514b696f383942517850333657327a4e6e5842702f00"
    parsed_result = cis.tokenMetaDataEvent(hex)
    assert parsed_result.tag == 251
    assert parsed_result.token_id == "619d1042"
    assert parsed_result.metadata == MetadataUrl(
        url="https://ipfs.io/ipfs/QmVkQ9XsJcgMW5z8bCgfoWrk5eQKio89BQxP36W2zNnXBp/",
        checksum=None,
    )


def test_cis2_operator_event(cis: CIS):
    # from hash a34c11d52386c6c8bfae9a7592b434bb257c87228ade5364aafc0ac1a1528019
    hex = "fc010054d5dfb13b51e7b755a3864b089d030bdd1d55ec348ed1f385abcc7133ddc1fa0177240000000000000000000000000000"
    parsed_result = cis.updateOperatorEvent(hex)
    assert parsed_result.tag == 252
    assert parsed_result.operator_update == "Add operator"
    assert parsed_result.owner == "3b7Bfus88w9pVu5ajYReuTj8WhAgStrxNibvPDBN5c2EhSXfhd"
    assert parsed_result.operator == "<9335,0>"
