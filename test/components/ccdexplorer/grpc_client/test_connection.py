import grpc
import pytest
from types import SimpleNamespace
import time
from ccdexplorer.grpc_client.core import GRPCClient, NET


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode):
        super().__init__()
        self._code = code

    def code(self):
        return self._code


class FakeUnaryMethod:
    def __init__(self, side_effects):
        """
        side_effects: list of either Exceptions or return values
        """
        self.side_effects = list(side_effects)
        self.calls = []

    def __call__(self, request, timeout=None, metadata=None):
        self.calls.append({"request": request, "timeout": timeout, "metadata": metadata})
        effect = self.side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


class FakeStreamMethod:
    def __init__(self, side_effects):
        """
        side_effects: list of either Exceptions or iterables (yield items)
        """
        self.side_effects = list(side_effects)
        self.calls = []

    def __call__(self, request, timeout=None, metadata=None):
        self.calls.append({"request": request, "timeout": timeout, "metadata": metadata})
        effect = self.side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        # effect is iterable; return iterator as gRPC would
        return iter(effect)


@pytest.fixture
def grpc_client():
    c = GRPCClient.__new__(GRPCClient)  # bypass __init__

    # minimal state required by stub_on_net/check_connection
    c.hosts = {
        NET.MAINNET: [
            {"host": "m1", "port": 1},
            {"host": "m2", "port": 2},
            {"host": "m3", "port": 3},
        ],
        NET.TESTNET: [
            {"host": "t1", "port": 1},
            {"host": "t2", "port": 2},
            {"host": "t3", "port": 3},
        ],
    }
    c.host_index = {NET.MAINNET: 0, NET.TESTNET: 0}
    c._was_ready = {NET.MAINNET: False, NET.TESTNET: False}

    # patchable hooks
    c._backoff = lambda attempt: None  # no sleeping in unit tests

    # Default: check_connection succeeds
    c.check_connection = lambda net, attempts=1, timeout_s=0.5: True

    # Track reconnect/rotate calls
    c._rotate_calls = []
    c._reconnect_calls = []

    def _rotate_host(net):
        c._rotate_calls.append(net)
        c.host_index[net] = (c.host_index[net] + 1) % len(c.hosts[net])

    def _reconnect_net(net):
        c._reconnect_calls.append(net)
        # no-op; in real life it rebuilds channel/stub

    c._rotate_host = _rotate_host
    c._reconnect_net = _reconnect_net

    # Provide stub containers
    c.stub_mainnet = SimpleNamespace()
    c.stub_testnet = SimpleNamespace()

    return c


def test_unary_retries_and_rotates(grpc_client):
    # Arrange: fail twice, then succeed
    method = FakeUnaryMethod(
        [
            FakeRpcError(grpc.StatusCode.UNAVAILABLE),
            FakeRpcError(grpc.StatusCode.UNAVAILABLE),
            {"ok": True},
        ]
    )
    grpc_client.stub_mainnet.GetAccountInfo = method

    # Act
    res = grpc_client.stub_on_net(
        NET.MAINNET, "GetAccountInfo", request={"a": 1}, retries=2, timeout=1.0
    )

    # Assert
    assert res == {"ok": True}
    assert len(method.calls) == 3
    assert grpc_client._rotate_calls == [NET.MAINNET, NET.MAINNET]
    assert grpc_client._reconnect_calls == [NET.MAINNET, NET.MAINNET]


def test_unary_does_not_retry_on_invalid_argument(grpc_client):
    method = FakeUnaryMethod([FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT)])
    grpc_client.stub_mainnet.GetAccountInfo = method

    with pytest.raises(grpc.RpcError):
        grpc_client.stub_on_net(NET.MAINNET, "GetAccountInfo", request={"a": 1}, retries=2)

    assert len(method.calls) == 1
    assert grpc_client._rotate_calls == []
    assert grpc_client._reconnect_calls == []


def test_streaming_retries_and_returns_list(grpc_client):
    method = FakeStreamMethod(
        [
            FakeRpcError(grpc.StatusCode.UNAVAILABLE),
            [1, 2, 3],
        ]
    )
    grpc_client.stub_testnet.GetTokenList = method

    res = grpc_client.stub_on_net(
        NET.TESTNET, "GetTokenList", request={"bh": "x"}, streaming=True, retries=1
    )

    assert res == [1, 2, 3]
    assert len(method.calls) == 2
    assert grpc_client._rotate_calls == [NET.TESTNET]
    assert grpc_client._reconnect_calls == [NET.TESTNET]


def test_channel_not_ready_retries_then_raises(grpc_client):
    # Fail readiness always
    grpc_client.check_connection = lambda net, attempts=1, timeout_s=0.5: False

    with pytest.raises(Exception):
        grpc_client.stub_on_net(NET.MAINNET, "GetAccountInfo", request={"a": 1}, retries=2)

    # Should have rotated/reconnected for each retry attempt
    assert len(grpc_client._rotate_calls) == 2
    assert len(grpc_client._reconnect_calls) == 2


def test_cooldown_skips_failed_host(monkeypatch, grpc_client):
    t = 1000.0
    monkeypatch.setattr(time, "monotonic", lambda: t)

    grpc_client._down_until = {}
    grpc_client._cooldown_s = 30.0

    # Replace rotate with cooldown-aware implementation you use in prod
    def rotate_with_cooldown(net):
        n = len(grpc_client.hosts[net])
        now = time.monotonic()
        for _ in range(n):
            grpc_client.host_index[net] = (grpc_client.host_index[net] + 1) % n
            if grpc_client._down_until.get((net, grpc_client.host_index[net]), 0.0) <= now:
                return

    grpc_client._rotate_host = rotate_with_cooldown

    # Mark current host down
    grpc_client._down_until[(NET.MAINNET, 1)] = t + 30.0
    grpc_client.host_index[NET.MAINNET] = 0

    grpc_client._rotate_host(NET.MAINNET)
    assert grpc_client.host_index[NET.MAINNET] == 2  # skipped 1 due to cooldown
