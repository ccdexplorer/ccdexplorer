# ruff: noqa: F403, F405, E402
from __future__ import annotations

from .service_pb2_grpc import QueriesStub
from ccdexplorer.env import GRPC_MAINNET, GRPC_TESTNET
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from .types_pb2 import *
import grpc
from ccdexplorer.domain.generic import NET
import os
from rich.console import Console
import threading
import time
import random

from prometheus_client import Counter


console = Console()

GRPC_NET_UNRESPONSIVE_TOTAL = Counter(
    "ccd_grpc_net_unresponsive_total",
    "Number of times gRPC calls failed for a net after all retries",
    ["net", "reason"],
)
_RETRYABLE = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}

HOME_IP = os.environ.get("HOME_IP", "")

from .queries._GetPoolInfo import (
    Mixin as _GetPoolInfo,
)
from .queries._GetPoolDelegatorsRewardPeriod import (
    Mixin as _GetPoolDelegatorsRewardPeriod,
)
from .queries._GetPassiveDelegatorsRewardPeriod import (
    Mixin as _GetPassiveDelegatorsRewardPeriod,
)
from .queries._GetAccountList import (
    Mixin as _GetAccountList,
)
from .queries._GetBakerList import (
    Mixin as _GetBakerList,
)
from .queries._GetBlocksAtHeight import (
    Mixin as _GetBlocksAtHeight,
)
from .queries._GetBlocks import (
    Mixin as _GetBlocks,
)
from .queries._GetFinalizedBlocks import (
    Mixin as _GetFinalizedBlocks,
)
from .queries._GetInstanceInfo import (
    Mixin as _GetInstanceInfo,
)
from .queries._GetInstanceList import (
    Mixin as _GetInstanceList,
)
from .queries._GetAnonymityRevokers import (
    Mixin as _GetAnonymityRevokers,
)
from .queries._GetIdentityProviders import (
    Mixin as _GetIdentityProviders,
)
from .queries._GetPoolDelegators import (
    Mixin as _GetPoolDelegators,
)
from .queries._GetPassiveDelegators import (
    Mixin as _GetPassiveDelegators,
)
from .queries._GetAccountInfo import (
    Mixin as _GetAccountInfo,
)
from .queries._GetBlockInfo import (
    Mixin as _GetBlockInfo,
)
from .queries._GetElectionInfo import (
    Mixin as _GetElectionInfo,
)
from .queries._GetTokenomicsInfo import (
    Mixin as _GetTokenomicsInfo,
)
from .queries._GetPassiveDelegationInfo import (
    Mixin as _GetPassiveDelegationInfo,
)
from .queries._GetBlockTransactionEvents import (
    Mixin as _GetBlockTransactionEvents,
)
from .queries._GetBlockSpecialEvents import (
    Mixin as _GetBlockSpecialEvents,
)
from .queries._GetBlockPendingUpdates import (
    Mixin as _GetBlockPendingUpdates,
)
from .queries._GetModuleSource import (
    Mixin as _GetModuleSource,
)
from .queries._GetBlockChainParameters import (
    Mixin as _GetBlockChainParameters,
)
from .queries._InvokeInstance import (
    Mixin as _InvokeInstance,
)
from .queries._GetConsensusInfo import (
    Mixin as _GetConsensusInfo,
)

from .queries._GetBakerEarliestWinTime import (
    Mixin as _GetBakerEarliestWinTime,
)
from .queries._CheckHealth import (
    Mixin as _CheckHealth,
)
from .queries._GetGetConsensusDetailedStatus import (
    Mixin as _GetGetConsensusDetailedStatus,
)
from .queries._GetScheduledReleaseAccounts import (
    Mixin as _GetScheduledReleaseAccounts,
)
from .queries._GetCooldownAccounts import (
    Mixin as _GetCooldownAccounts,
)
from .queries._GetPreCooldownAccounts import (
    Mixin as _GetPreCooldownAccounts,
)
from .queries._GetPrePreCooldownAccounts import (
    Mixin as _GetPrePreCooldownAccounts,
)

from .queries._GetTokenInfo import (
    Mixin as _GetTokenInfo,
)

from .queries._GetTokenList import (
    Mixin as _GetTokenList,
)

from .queries._GetWinningBakersEpoch import (
    Mixin as _GetWinningBakersEpoch,
)
from .queries._GetModuleList import (
    Mixin as _GetModuleList,
)
from .queries._GetBakersRewardPeriod import (
    Mixin as _GetBakersRewardPeriod,
)


class GRPCClient(  # type: ignore
    _GetPoolInfo,
    _GetAccountList,
    _GetBakerList,
    _GetInstanceInfo,
    _GetInstanceList,
    _GetBlocks,
    _GetFinalizedBlocks,
    _GetBlocksAtHeight,
    _GetIdentityProviders,
    _GetAnonymityRevokers,
    _GetPassiveDelegationInfo,
    _GetPassiveDelegators,
    _GetPoolDelegators,
    _GetPoolDelegatorsRewardPeriod,
    _GetPassiveDelegatorsRewardPeriod,
    _GetAccountInfo,
    _GetBlockInfo,
    _GetElectionInfo,
    _GetBlockTransactionEvents,
    _GetBlockSpecialEvents,
    _GetBlockPendingUpdates,
    _GetTokenomicsInfo,
    _GetModuleSource,
    _GetBlockChainParameters,
    _InvokeInstance,
    _GetConsensusInfo,
    _GetBakerEarliestWinTime,
    _CheckHealth,
    _GetGetConsensusDetailedStatus,
    _GetScheduledReleaseAccounts,
    _GetCooldownAccounts,
    _GetPreCooldownAccounts,
    _GetPrePreCooldownAccounts,
    _GetTokenInfo,
    _GetTokenList,
    _GetWinningBakersEpoch,
    _GetModuleList,
    _GetBakersRewardPeriod,
    # _SendBlockItem,
):
    def __init__(
        self,
        net: str = "mainnet",
        devnet: bool = False,
        *,
        warmup: bool = True,
        warmup_timeout_s: float = 1.0,
        warmup_attempts: int = 1,
        warmup_nets: tuple[NET, ...] | None = None,
    ):
        self.net = NET(net)
        self._was_ready = {NET.MAINNET: False, NET.TESTNET: False}
        self.host_index = {NET.MAINNET: 0, NET.TESTNET: 0}
        self.hosts: dict[NET, list[dict]] = {}
        self._down_until: dict[tuple[NET, int], float] = {}
        self._cooldown_s = 30.0
        # Configure hosts
        self.hosts[NET.MAINNET] = GRPC_MAINNET
        if devnet:
            self.hosts[NET.MAINNET] = [
                {"host": "--secure--grpc.devnet-p10-1.concordium.com", "port": 20000}
            ]

        self.hosts[NET.TESTNET] = GRPC_TESTNET

        # Reconnect locks (single-process stampede protection)
        self._reconnect_lock = {
            NET.MAINNET: threading.Lock(),
            NET.TESTNET: threading.Lock(),
        }

        # Build channels/stubs if host lists exist
        self.channel_mainnet = None
        self.stub_mainnet = None
        self.channel_testnet = None
        self.stub_testnet = None

        if self.hosts.get(NET.MAINNET):
            self._connect_net(NET.MAINNET)

        if self.hosts.get(NET.TESTNET):
            self._connect_net(NET.TESTNET)

        # Optional warmup: bounded, never infinite
        if warmup:
            if warmup_nets is None:
                # Default: warm the configured net only (safer startup)
                warmup_nets = (self.net,)

            for n in warmup_nets:
                if self.hosts.get(n):
                    # bounded check; does not loop forever
                    self.check_connection(n, attempts=warmup_attempts, timeout_s=warmup_timeout_s)

        # def connect(self):
        #     host = self.hosts[NET.MAINNET][self.host_index[NET.MAINNET]]["host"]
        #     port = self.hosts[NET.MAINNET][self.host_index[NET.MAINNET]]["port"]

        #     use_secure = "--secure--" in host
        #     host = host.replace("--secure--", "")
        #     address = f"{host}:{port}"

        #     if use_secure:
        #         creds = grpc.ssl_channel_credentials()
        #         options = [("grpc.ssl_target_name_override", "grpc.devnet-p10-1.concordium.com")]
        #         self.channel_mainnet = grpc.secure_channel("52.48.67.53:20000", creds, options)
        #     else:
        #         self.channel_mainnet = grpc.insecure_channel(address)

        #     try:
        #         grpc.channel_ready_future(self.channel_mainnet).result(timeout=3)
        #         console.log(f"GRPCClient for {NET.MAINNET.value} connected on: {address}")
        #     except grpc.FutureTimeoutError:
        #         console.log(f"GRPC connection to {address} timed out.")

        #     host = self.hosts[NET.TESTNET][self.host_index[NET.TESTNET]]["host"]
        #     port = self.hosts[NET.TESTNET][self.host_index[NET.TESTNET]]["port"]
        #     self.channel_testnet = grpc.insecure_channel(f"{host}:{port}")
        #     try:
        #         grpc.channel_ready_future(self.channel_testnet).result(timeout=1)
        #         console.log(f"GRPCClient for {NET.TESTNET.value} connected on: {host}:{port}")
        #     except grpc.FutureTimeoutError:
        #         pass

        #     self.stub_mainnet = QueriesStub(self.channel_mainnet)
        #     self.stub_testnet = QueriesStub(self.channel_testnet)

        #     self.channel = grpc.insecure_channel(
        #         f"{self.hosts[self.net][self.host_index[self.net]]['host']}:{self.hosts[self.net][self.host_index[self.net]]['port']}"
        #     )

        #     self.stub = QueriesStub(self.channel)

        def stub_on_net(
            self,
            net: NET,
            method_name: str,
            request,
            *,
            timeout: float = 30.0,
            retries: int = 2,
            streaming: bool = False,
            connect_timeout_s: float = 0.5,
        ):
            """
            Unary: returns the response message.
            Streaming: returns a list of streamed messages (consumes the stream).
            """

            for attempt in range(retries + 1):
                # Adjust readiness timeout for secure endpoints (you already have _current_is_secure, or inline the logic)
                rt = connect_timeout_s
                if "--secure--" in self.hosts[net][self.host_index[net]]["host"]:
                    rt = max(rt, 2.0)

                # Fast readiness gate (bounded)
                ok = self.check_connection(net, attempts=1, timeout_s=rt)
                if not ok:
                    # If the channel is not ready, treat as retryable within our bounded loop
                    if attempt < retries:
                        if len(self.hosts[net]) > 1:
                            self._mark_host_down(net)
                            self._rotate_host(net)
                        self._reconnect_net(net)
                        self._backoff(attempt)
                        continue

                    # All readiness attempts exhausted → net considered unresponsive for this call
                    GRPC_NET_UNRESPONSIVE_TOTAL.labels(
                        net=net.value,
                        reason="connect_not_ready",
                    ).inc()
                    raise grpc.RpcError(f"gRPC channel not ready for {net.value}")

                stub = self.stub_mainnet if net == NET.MAINNET else self.stub_testnet
                method = getattr(stub, method_name, None)
                if method is None:
                    raise AttributeError(f"No gRPC method {method_name}")

                try:
                    if not streaming:
                        # Unary call
                        return method(request, timeout=timeout)

                    # Server-streaming: call and consume the iterator so we can retry cleanly
                    stream_iter = method(request, timeout=timeout)
                    out = []
                    for item in stream_iter:
                        out.append(item)
                    return out

                except grpc.RpcError as e:
                    retryable = self._is_retryable(e)

                    if retryable and attempt < retries:
                        # Retryable error: mark not-ready, switch host if possible, then retry
                        self._was_ready[net] = False
                        if len(self.hosts[net]) > 1:
                            self._mark_host_down(net)
                            self._rotate_host(net)
                        self._reconnect_net(net)
                        self._backoff(attempt)
                        continue

                    # Retries exhausted or non-retryable error → net considered unresponsive for this call
                    if retryable:
                        reason = "rpc_retry_exhausted"
                    else:
                        reason = "rpc_non_retryable"

                    GRPC_NET_UNRESPONSIVE_TOTAL.labels(
                        net=net.value,
                        reason=reason,
                    ).inc()

                    raise

    def _is_retryable(self, e: grpc.RpcError) -> bool:
        try:
            return e.code() in _RETRYABLE  # type: ignore[attr-defined]
        except Exception:
            return False

    def _pick_next_host(self, net: NET) -> None:
        n = len(self.hosts[net])
        now = time.monotonic()

        for _ in range(n):
            self.host_index[net] = (self.host_index[net] + 1) % n
            if self._down_until.get((net, self.host_index[net]), 0.0) <= now:
                return
        # If all are cooled down, just keep current index (we'll try anyway)

    def _backoff(self, attempt: int) -> None:
        # Small bounded exponential backoff with jitter (keeps incidents from flapping)
        base = 0.1 * (2**attempt)  # 0.1, 0.2, 0.4 ...
        sleep_s = min(0.5, base) + random.uniform(0.0, 0.05)
        time.sleep(sleep_s)

    def _current_is_secure(self, net: NET) -> bool:
        return "--secure--" in self.hosts[net][self.host_index[net]]["host"]

    def check_connection(
        self, net: NET = NET.MAINNET, *, attempts: int = 2, timeout_s: float = 1.0
    ) -> bool:
        channel_to_check = self.channel_mainnet if net == NET.MAINNET else self.channel_testnet

        for _ in range(attempts):
            try:
                grpc.channel_ready_future(channel_to_check).result(timeout=timeout_s)

                if not self._was_ready[net]:
                    console.log(
                        f"GRPCClient channel ready for {net.value}: "
                        f"{self.hosts[net][self.host_index[net]]['host']}:"
                        f"{self.hosts[net][self.host_index[net]]['port']}"
                    )

                self._was_ready[net] = True
                return True
            except grpc.FutureTimeoutError:
                self._was_ready[net] = False

                if len(self.hosts[net]) > 1:
                    self._mark_host_down(net)
                    self._rotate_host(net)

                self._reconnect_net(net)

        return False

    def _mark_host_down(self, net: NET) -> None:
        self._down_until[(net, self.host_index[net])] = time.monotonic() + self._cooldown_s

    def _rotate_host(self, net: NET) -> None:
        import time

        n = len(self.hosts[net])
        now = time.monotonic()
        for _ in range(n):
            self.host_index[net] = (self.host_index[net] + 1) % n
            if self._down_until.get((net, self.host_index[net]), 0.0) <= now:
                return
        # all cooled down, keep current index

    def _reconnect_net(self, net: NET) -> None:
        # prevent stampede inside a single process
        with self._reconnect_lock[net]:
            self._connect_net(net)

    def _connect_net(self, net: NET) -> None:
        # connect ONLY this net; leave the other untouched
        if net == NET.MAINNET:
            self.channel_mainnet, self.stub_mainnet = self._build_channel_and_stub(net)
        else:
            self.channel_testnet, self.stub_testnet = self._build_channel_and_stub(net)

    def _build_channel_and_stub(self, net: NET):
        host_cfg = self.hosts[net][self.host_index[net]]
        host = host_cfg["host"]
        port = host_cfg["port"]

        use_secure = "--secure--" in host
        host = host.replace("--secure--", "")
        address = f"{host}:{port}"

        # Common options: conservative defaults, no behavior change yet
        options = [
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 30_000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]

        if use_secure:
            creds = grpc.ssl_channel_credentials()

            # Keep your existing override behavior
            # options.append(("grpc.ssl_target_name_override", "grpc.devnet-plt-beta.concordium.com"))

            channel = grpc.secure_channel(address, creds, options=options)
        else:
            channel = grpc.insecure_channel(address, options=options)

        stub = QueriesStub(channel)

        console.log(
            f"GRPCClient building channel for {net.value} on {address} "
            f"({'secure' if use_secure else 'insecure'})"
        )

        return channel, stub

    def connection_info(self, caller: str, tooter: Tooter, ADMIN_CHAT_ID: int) -> None:
        message = f"<code>{caller}</code> connection status\n<code>mainnet</code> - {self.hosts[NET.MAINNET][self.host_index[NET.MAINNET]]['host']}:{self.hosts[NET.MAINNET][self.host_index[NET.MAINNET]]['port']}\n<code>testnet</code> - {self.hosts[NET.TESTNET][self.host_index[NET.TESTNET]]['host']}:{self.hosts[NET.TESTNET][self.host_index[NET.TESTNET]]['port']}\n"
        tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=message,
            notifier_type=TooterType.INFO,
        )

    async def aconnection_info(self, caller: str, tooter: Tooter, ADMIN_CHAT_ID: int) -> None:
        message = f"<code>{caller}</code> connection status\n<code>mainnet</code> - {self.hosts[NET.MAINNET][self.host_index[NET.MAINNET]]['host']}:{self.hosts[NET.MAINNET][self.host_index[NET.MAINNET]]['port']}\n<code>testnet</code> - {self.hosts[NET.TESTNET][self.host_index[NET.TESTNET]]['host']}:{self.hosts[NET.TESTNET][self.host_index[NET.TESTNET]]['port']}\n"
        tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=message,
            notifier_type=TooterType.INFO,
        )
