import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_module_list(grpcclient: GRPCClient):
    block_hash = "d8bfcc48f9d5721694e0e1108c79bd8345d46049bedc94bdc14d2bc1b02ebecb"
    module_list = grpcclient.get_module_list(block_hash)
    assert isinstance(module_list, list)
    assert "d161eb4ba17b1679dd020ad7883b32c18960404f71ca002d72265f686713be1f" in module_list
    assert len(module_list) == 330
    print(module_list)
