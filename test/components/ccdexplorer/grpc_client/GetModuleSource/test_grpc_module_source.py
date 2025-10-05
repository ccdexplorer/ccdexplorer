import pytest
from ccdexplorer.domain.generic import NET


from ccdexplorer.grpc_client import GRPCClient
from rich import print
import io
from ccdexplorer.wasm_decoder import parse_module


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_module_source_v0(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    module_ref = "48682eecb17547b46a2a62fa05b1cbf95054be4804db99db8878db3766c04799"

    ms = grpcclient.get_module_source(module_ref, block_hash)
    bs = io.BytesIO(bytes.fromhex(ms.v0))  # type: ignore
    # n = int.from_bytes(bs.read(8), byteorder='little')

    module = parse_module(bs.read())
    print(module.keys())
    print(module["type"])
    print(module["memory"])
    print(module["import"])

    # assert ii.v1.owner == '3BtQiJ1sNad8cp5JLdBrjuRYkoX6A8fanzHpoSshKEX6ghAWwi'
    # assert len(ii.v1.methods) == 12
    # assert ii.v1.amount == 0
    # assert ii.v1.methods[0] == 'BictoryCns.createSubdomain'
    # assert ii.v1.name == 'init_BictoryCns'
    # assert ii.v1.source_module == '7508ce8d24e5cb736851612d94aec545d713d16333e1285df125fc5d25843927'


def test_module_source_v1_CIS2(grpcclient: GRPCClient):
    block_hash = "last_final"
    module_ref = "16a13518f92fd3904cfa43b3d5fe33a09b6f34f77d6715b0231d8b08793a75e9"

    ms = grpcclient.get_module_source(module_ref, block_hash)
    bs = io.BytesIO(bytes.fromhex(ms.v1))  # type: ignore

    module = parse_module(bs.read())
    print(module.keys())
    print(module["type"])
    print(module["memory"])
    print(module["export"])
    print(module["import"])
    print("****************")
    del module["data"]
    del module["code"]
    print(module)

    # import multiprocessing

    # pool = multiprocessing.Pool()
    # module["code"] = pool.map(parse_code, module["code"], 100)

    # with open("source.txt", "w") as f:
    #     for l in module["code"]:
    #         for ll in l:
    #             f.write(ll)


def test_module_source_v1(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    block_hash = "last_final"
    index = 9360
    subindex = 0
    ii = grpcclient.get_instance_info(index, subindex, block_hash)
    print(ii)
    # assert ii.v0.owner == "4rc1tjHwkFB5JP39rcEeRqsh1gMJZerjjDpWoqqTY6VHy2WZgj"
    # assert len(ii.v0.methods) == 11
    # assert ii.v0.methods[0] == "inventory.add_trader"
    # assert ii.v0.name == "init_inventory"
    # assert (
    #     ii.v0.source_module
    #     == "d571155b495c0f9e6f56ae54a51c014ba2cc6895bdad337e2968b7b297066c11"
    # )


def test_module_source_8901(grpcclient: GRPCClient):
    block_hash = "last_final"
    module_ref = "7f24e586bb7dcac318ca0c2876d3e4b5c07f36d48d0c7b20f8d82dad0552f1e3"
    ms = grpcclient.get_module_source(module_ref, block_hash, NET.TESTNET)
    bs = io.BytesIO(bytes.fromhex(ms.v1))  # type: ignore
    #
    module = parse_module(bs.read())
    # print(module.keys())
    # print(module["type"])
    # print(module["memory"])
    # print(module["import"])
    print(module)
