"""Which address the site believes a visitor has.

X-Forwarded-For is a list, and each proxy appends the peer it saw. The client
controls the front of it; only the last entry was written by the proxy this
site sits behind. Taking the first meant a caller could choose their own
throttle key by sending the header themselves, whenever the proxy appends
rather than replaces -- which is a setting, not a guarantee.
"""

from types import SimpleNamespace

from ccdexplorer.ccdexplorer_site.app.utils import client_ip


def _request(forwarded=None, peer="10.0.0.9"):
    headers = {"x-forwarded-for": forwarded} if forwarded is not None else {}
    return SimpleNamespace(headers=headers, client=SimpleNamespace(host=peer))


def test_a_single_entry_is_the_visitor():
    assert client_ip(_request("203.0.113.7")) == "203.0.113.7"


def test_the_proxy_appended_entry_wins_over_what_the_client_sent():
    assert client_ip(_request("6.6.6.6, 203.0.113.7")) == "203.0.113.7"


def test_a_long_forged_chain_still_ends_at_the_proxys_entry():
    chain = ", ".join(["1.1.1.1"] * 20 + ["203.0.113.7"])
    assert client_ip(_request(chain)) == "203.0.113.7"


def test_no_header_falls_back_to_the_peer():
    assert client_ip(_request(None, peer="198.51.100.4")) == "198.51.100.4"


def test_garbage_in_the_header_falls_back_to_the_peer():
    """The value becomes a Redis key and a header sent on; it has to be an
    address, not whatever fits in a header."""
    assert client_ip(_request("not-an-ip", peer="198.51.100.4")) == "198.51.100.4"


def test_our_own_hop_after_the_visitor_is_skipped():
    """A private entry is another proxy of ours, not the visitor."""
    assert client_ip(_request("203.0.113.7, 10.0.0.1")) == "203.0.113.7"


def test_a_forged_prefix_cannot_hide_behind_our_own_hop():
    assert client_ip(_request("6.6.6.6, 203.0.113.7, 172.18.0.2")) == "203.0.113.7"


def test_an_all_private_chain_is_an_internal_caller():
    assert client_ip(_request("10.0.0.5, 10.0.0.1")) == "10.0.0.1"


def test_an_ipv6_address_is_accepted():
    assert client_ip(_request("2001:db8::1")) == "2001:db8::1"


def test_no_peer_and_no_header_is_empty():
    req = SimpleNamespace(headers={}, client=None)
    assert client_ip(req) == ""
