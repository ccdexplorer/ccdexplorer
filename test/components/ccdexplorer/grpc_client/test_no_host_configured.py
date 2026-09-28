"""A net with no gRPC host configured must say so, not raise IndexError.

env/settings.py makes GRPC_MAINNET and GRPC_TESTNET `[]` when the matching
environment variable is unset, and __init__ already handles that: it skips
_connect_net, so the stub stays None. stub_on_net did not, and its first
statement indexes the host list:

    if "--secure--" in self.hosts[net][self.host_index[net]]["host"]:

On an empty list that is IndexError: list index out of range -- 17 separate
issues across 17 endpoints when a container ran with no GRPC_* variables set,
none of which said anything about configuration.

A misconfigured net is not an unreachable node, and the message should not have
to be guessed at.
"""

import pytest

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient


@pytest.fixture(scope="module")
def client() -> GRPCClient:
    """A client with no warmup, so nothing here waits on a real node.

    Channel construction is lazy in grpc, so this performs no I/O.
    """
    return GRPCClient(warmup=False)


@pytest.fixture
def unconfigured(client, monkeypatch) -> NET:
    """Testnet exactly as an unset GRPC_TESTNET leaves it.

    __init__ skips _connect_net when a host list is empty, so the channel and
    stub stay None as well. Emptying only `hosts` would leave a live channel
    behind and test a state that cannot occur.
    """
    monkeypatch.setitem(client.hosts, NET.TESTNET, [])
    monkeypatch.setitem(client.host_index, NET.TESTNET, 0)
    monkeypatch.setitem(client._was_ready, NET.TESTNET, False)
    monkeypatch.setattr(client, "channel_testnet", None)
    monkeypatch.setattr(client, "stub_testnet", None)
    return NET.TESTNET


def test_a_net_without_hosts_reports_configuration_not_an_index_error(client, unconfigured):
    with pytest.raises(ConnectionError) as exc:
        client.stub_on_net(unconfigured, "GetModuleList", None, retries=0)

    assert "testnet" in str(exc.value)
    assert "host" in str(exc.value).lower()


def test_it_is_not_an_indexerror(client, unconfigured):
    """The 17 issues were all IndexError, which said nothing useful."""
    with pytest.raises(ConnectionError):
        client.stub_on_net(unconfigured, "GetModuleList", None, retries=0)


def test_rotating_hosts_with_none_configured_is_already_safe(client, unconfigured):
    """Not a bug, pinned so it stays that way.

    `(i + 1) % n` would divide by zero, but it sits inside `for _ in range(n)`,
    which does not run when n is 0.
    """
    client._rotate_host(unconfigured)


def test_checking_the_connection_of_an_unconfigured_net_is_false_not_an_error(
    client, unconfigured
):
    """It answers the question rather than raising on a None channel."""
    assert client.check_connection(unconfigured, attempts=1, timeout_s=0.01) is False


def test_a_configured_net_still_goes_past_the_guard(client, monkeypatch):
    """The guard must only catch the no-host case, not short-circuit a real one.

    A host that cannot be reached must still fail as a readiness problem, which
    is a different message and a different cause.
    """
    monkeypatch.setitem(client.hosts, NET.TESTNET, [{"host": "127.0.0.1", "port": 1}])
    monkeypatch.setitem(client.host_index, NET.TESTNET, 0)
    monkeypatch.setitem(client._was_ready, NET.TESTNET, False)
    # Stubbed rather than dialled: this asserts which branch was taken, and a
    # real connect attempt would depend on what is listening on the machine.
    monkeypatch.setattr(client, "check_connection", lambda *a, **k: False)

    with pytest.raises(ConnectionError) as exc:
        client.stub_on_net(NET.TESTNET, "GetModuleList", None, retries=0)

    assert "not ready" in str(exc.value)
