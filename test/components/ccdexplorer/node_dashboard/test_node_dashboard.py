"""The dashboard_nodes refresher, after extraction from dagster_recurring.

Two of these pin bugs rather than behaviour:

  * devnet has no node dashboard, but the URL was chosen with
    `if net == "testnet" ... else <mainnet url>`, so every devnet run wrote
    mainnet's nodes into concordium_devnet -- 132 of them were sitting there
    when this was found.
  * an empty response must not empty the collection. The comment in the
    original records why: it once did delete_many() then bulk_write(), and
    readers saw an empty nodes table every minute while the write ran.
"""

import logging

import pytest

log = logging.getLogger("test")


def _mod():
    from ccdexplorer import node_dashboard

    return node_dashboard


class FakeResult:
    def __init__(self, deleted=0):
        self.deleted_count = deleted


class FakeCollection:
    def __init__(self, docs=0):
        self.docs = docs
        self.bulk = None
        self.deleted_filter = None
        self.replaced = []

    def count_documents(self, _q):
        return self.docs

    def bulk_write(self, ops):
        if not ops:
            raise ValueError("bulk_write([]) raises, as the original noted")
        self.bulk = ops
        return FakeResult()

    def delete_many(self, flt):
        self.deleted_filter = flt
        return FakeResult(deleted=2)

    def replace_one(self, q, doc, upsert=False):
        self.replaced.append((q, doc, upsert))


class FakeMongo:
    def __init__(self):
        from ccdexplorer.mongodb import Collections

        self.nodes = FakeCollection(docs=7)
        self.helpers = FakeCollection()
        d = {Collections.dashboard_nodes: self.nodes, Collections.helpers: self.helpers}
        self.mainnet = d
        self.testnet = d
        self.devnet = d


# --- the devnet bug -------------------------------------------------------


def test_devnet_has_no_dashboard_and_is_refused():
    m = _mod()

    assert "devnet" not in m.DASHBOARDS
    with pytest.raises(ValueError, match="no node dashboard"):
        m.refresh_dashboard_nodes(log, FakeMongo(), "devnet")


def test_each_known_net_has_its_own_url():
    m = _mod()

    assert "testnet" in m.DASHBOARDS["testnet"]
    assert "mainnet" in m.DASHBOARDS["mainnet"]
    assert m.DASHBOARDS["mainnet"] != m.DASHBOARDS["testnet"]


# --- the empty-response guard --------------------------------------------


def test_an_empty_dashboard_keeps_what_is_on_record(monkeypatch):
    """No bulk_write, no delete, and the count is reported."""
    m = _mod()
    mongo = FakeMongo()

    class Resp:
        status_code = 200
        elapsed = __import__("datetime").timedelta(seconds=0.1)

        def json(self):
            return []

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, _url):
            return Resp()

    monkeypatch.setattr(m.core.httpx, "Client", Client)

    got = m.refresh_dashboard_nodes(log, mongo, "mainnet")

    assert got == 0
    assert mongo.nodes.bulk is None
    assert mongo.nodes.deleted_filter is None


def test_a_failed_fetch_raises_and_writes_nothing(monkeypatch):
    m = _mod()
    mongo = FakeMongo()

    class Resp:
        status_code = 503
        elapsed = __import__("datetime").timedelta(seconds=0.1)

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, _url):
            return Resp()

    monkeypatch.setattr(m.core.httpx, "Client", Client)

    with pytest.raises(Exception, match="Failed to fetch"):
        m.refresh_dashboard_nodes(log, mongo, "mainnet")
    assert mongo.nodes.bulk is None
    assert mongo.nodes.deleted_filter is None
