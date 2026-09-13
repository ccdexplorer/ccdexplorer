"""An agent registry is defined by what the contract reports, not by a list.

CIS-8004 is the standard that defines agent registration, so any contract
reporting support for it is a registry. Deriving the set that way means a new
registry is recognised the day it deploys, and a contract that never claimed
the standard cannot be mistaken for one -- neither of which a hand-kept list
or an env var gives you.
"""

from ccdexplorer.cis import (
    AGENT_REGISTRY_STANDARD,
    agent_registry_contracts,
    agent_registry_filter,
    is_agent_registry,
)


def test_standard_is_cis_8004():
    assert AGENT_REGISTRY_STANDARD == "CIS-8004"


def test_filter_matches_the_cached_standards_array():
    # Mongo matches a scalar against an array field element-wise, so this
    # selects instances whose cis_support.standards contains the standard.
    assert agent_registry_filter() == {"cis_support.standards": "CIS-8004"}


def test_is_agent_registry_reads_the_cached_answer():
    assert is_agent_registry({"cis_support": {"standards": ["CIS-0", "CIS-2", "CIS-8004"]}})
    assert not is_agent_registry({"cis_support": {"standards": ["CIS-0", "CIS-2"]}})


def test_unresolved_instances_are_not_registries():
    """Absent is not the same as false, but it must not read as true.

    An instance whose support has never been resolved says nothing about
    CIS-8004. Treating that as a registry would sweep in every contract the
    backfill has not reached yet.
    """
    assert not is_agent_registry({})
    assert not is_agent_registry({"cis_support": None})
    assert not is_agent_registry({"cis_support": {}})
    assert not is_agent_registry(None)


def test_contracts_helper_returns_ids(monkeypatch):
    class FakeCollection:
        def __init__(self):
            self.query = None

        def find(self, query, projection):
            self.query = query
            return [{"_id": "<10082,0>"}, {"_id": "<20000,0>"}]

    collection = FakeCollection()
    assert agent_registry_contracts(collection) == ["<10082,0>", "<20000,0>"]
    assert collection.query == agent_registry_filter()
