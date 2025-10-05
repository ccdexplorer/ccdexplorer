# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_anonymity_revokers(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    ar = grpcclient.get_anonymity_revokers(block_hash)

    print(ar)
    assert ar[0].description.name == "Anonymity Revoker 1"
    assert ar[1].identity == 2
    assert (
        ar[2].public_key
        == "b14cbfe44a02c6b1f78711176d5f437295367aa4f2a8c2551ee10d25a03adc69d61a332a058971919dad7312e1fc94c599ddd66d63a4763cc03a3155d59079b754beaea594d816de97bf5e021c973b804d20209d6830d04f1e73eb00741836fb"
    )
    assert ar[3].description.url == "https://ar.concordium.com/4"
