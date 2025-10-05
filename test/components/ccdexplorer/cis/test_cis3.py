from ccdexplorer.grpc_client.core import GRPCClient
import pytest
from ccdexplorer.cis import CIS


@pytest.fixture
def grpcclient():
    return GRPCClient()


@pytest.fixture
def cis(grpcclient: GRPCClient):
    return CIS(grpcclient)


def test_cis3_nonce_event(cis: CIS):
    # TODO: need to find a suitable test tx
    pass
    # from hash 5382e43c9fe252d66b56b00a7ce84168531336118cf37c6fa09d21b3c79e1db6
    # hex = "fe04619d104201005a398a949d41d7569a48051d34795ffa945dcf0afa4501689421c255a327d2db"
    # parsed_result = cis.nonceEvent(hex)
    # assert parsed_result.tag == 250
