import pytest


from ccdexplorer.grpc_client import GRPCClient


@pytest.fixture
def grpcclient():
    return GRPCClient()


# @pytest.mark.skip(reason="Account list, slow (10 sec).")
# def test_account_list(grpcclient: GRPCClient):
#     block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
#     al = grpcclient.get_account_list(block_hash)
#     # print (al[:10])#.dict(exclude_none=True))

#     assert al[0] == "2wkEiDDDhWLset4GT4TPVutLArbvKfTsP7VYhhA776aTmvHcNB"
#     assert al[1] == "2wkMvqseiVVZQJD6R2EZ9PS6Y23So84mPkzWFyRWJYzoLzyY2d"
