import copy
import subprocess

import json
import os
from enum import Enum
from ccdexplorer.domain.generic import NET
from ccdexplorer.env import GRPC_MAINNET, GRPC_TESTNET

from rich.console import Console

console = Console()

CONCORDIUM_CLIENT_PREFIX = os.environ.get("CONCORDIUM_CLIENT_PREFIX", "")
REQUESTOR_NODES = os.environ.get("REQUESTOR_NODES", "localhost")
REQUESTOR_NODES = REQUESTOR_NODES.split(",")


class RequestorType(Enum):
    accountInfo = "{"
    GetAccountList = "["
    consensus = "E"


class Requestor:
    """
    This class is performing the requests to concordium-client.
    """

    total_count = 0
    failing_nodes = {}
    mainnet_nodes = GRPC_MAINNET
    testnet_nodes = GRPC_TESTNET

    # failing_nodes = {k: 0 for k in nodes}

    def request_failed(self, result):
        if result.returncode == 1:
            return True
        else:
            if result.stdout.decode("utf-8") in [
                "Cannot establish connection to GRPC endpoint.\n",
                "gRPC error: not enough bytes\n",
            ]:
                return True
            else:
                return False
        # return result.returncode == 1

    def __init__(self, args, net: NET, check_nodes=False, timeout=5):
        Requestor.total_count += 1
        self.net = net
        if net == NET.MAINNET:
            self.std_args = [
                [
                    f"{CONCORDIUM_CLIENT_PREFIX}concordium-client",
                    "--grpc-retry",
                    "3",
                    "--grpc-ip",
                    x["host"],
                    "--grpc-port",
                    str(x["port"]),
                ]
                for x in self.mainnet_nodes
            ]
        else:
            self.std_args = [
                [
                    f"{CONCORDIUM_CLIENT_PREFIX}concordium-client",
                    "--grpc-retry",
                    "3",
                    "--grpc-ip",
                    x["host"],
                    "--grpc-port",
                    str(x["port"]),
                ]
                for x in self.testnet_nodes
            ]
        self.timeout = timeout
        self.args = args
        if not check_nodes:
            self.ask_the_client_with_backup()
        else:
            self.check_nodes_with_heights()

    def check_nodes(self):
        results = {}
        for arg in self.std_args:
            arg.extend(["raw", "GetBlockInfo"])
            result = subprocess.run(arg, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            results[arg[2]] = not self.request_failed(result)
        self.nodes_ok = results

    def check_nodes_with_heights(self):
        results = {}
        for arg in self.std_args:
            arg.extend(["raw", "GetBlockInfo"])
            result = subprocess.run(arg, stdout=subprocess.PIPE)
            json_result = json.loads(result.stdout.decode("utf-8"))
            results[arg[2]] = json_result["blockHeight"]
        self.nodes_ok = results

    def ask_the_client_with_backup(self):
        result = subprocess.CompletedProcess([], returncode=1)
        node_index = 0
        while self.request_failed(result):
            len_nodes = (
                len(self.mainnet_nodes) if self.net == NET.MAINNET else len(self.testnet_nodes)
            )
            if node_index == len_nodes:
                node_index = 0
                # time.sleep(0.001)
            self.arguments = copy.deepcopy(self.std_args[node_index])
            self.arguments.extend(self.args)
            assert (len(self.arguments)) == (len(self.std_args[node_index]) + len(self.args))

            try:
                result = subprocess.run(
                    self.arguments, timeout=self.timeout, stdout=subprocess.PIPE
                )
                if self.request_failed(result):
                    node_index += 1

            except Exception as e:
                print(e)
                node_index += 1

        self.result = result
        # if result.stdout.decode("utf-8") is None:
        #     console.log(self.arguments, result.stdout)


class ConcordiumClient:
    def __init__(self, tooter):
        self.tooter = tooter

    def save_module(self, net: NET, module_ref: str):
        result = Requestor(
            ["module", "show", module_ref, "--out", f"tmp/{module_ref}.out"], net
        ).result

        return result
