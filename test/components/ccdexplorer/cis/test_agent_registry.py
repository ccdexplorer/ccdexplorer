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


def test_token_display_name_prefers_the_most_informative_source():
    """The token id is unique and useless -- it must be the last resort.

    A name can live in three places depending on who published it: CIS-2
    metadata, an agent card's `name`, or -- for registries that publish no
    name at all -- the agent's address. Falling back to the token id means a
    row can always be told apart, but only after every name has been tried.
    """
    from ccdexplorer.ccdexplorer_site.app.utils import token_display_name

    cis2 = {"token_metadata": {"name": "Some NFT"}}
    assert token_display_name(cis2, "01") == "Some NFT"

    # An agent registry that publishes an A2A-style card.
    card = {"token_metadata": {}, "raw_metadata": {"name": "PersonalAgent"}}
    assert token_display_name(card, "0300") == "PersonalAgent"

    # agentverse publishes no name, only the agent's address.
    address_only = {"token_metadata": {}, "raw_metadata": {"address": "agent1qv5"}}
    assert token_display_name(address_only, "3300") == "agent1qv5"

    # CIS-2 wins over the card when both carry a name -- they agree in
    # practice, and the parsed field is the one the rest of the site uses.
    both = {"token_metadata": {"name": "quiet_ledger_63"}, "raw_metadata": {"name": "other"}}
    assert token_display_name(both, "0000") == "quiet_ledger_63"


def test_token_display_name_survives_metadata_without_a_name():
    """Metadata carrying a description but no name used to raise KeyError."""
    from ccdexplorer.ccdexplorer_site.app.utils import token_display_name

    assert token_display_name({"token_metadata": {"description": "d"}}, "02") == "02"
    assert token_display_name({"raw_metadata": {"agent_index": 2398}}, "5e09") == "5e09"
    assert token_display_name({}, "99") == "99"
    assert token_display_name(None, "99") == "99"
    # No token id either: fall back to whatever identifies the document.
    assert token_display_name({"_id": "<1,0>-99"}, None) == "<1,0>-99"


def test_token_display_name_finds_a_label_inside_agent_metadata():
    """Some registries name the agent one level down, not at the top."""
    from ccdexplorer.ccdexplorer_site.app.utils import token_display_name

    card = {
        "token_metadata": {},
        "raw_metadata": {
            "agent_index": 1682,
            "agent_metadata": {"label": "mainnet-dryrun-claims-status", "model": "claude-sonnet-5"},
        },
    }
    assert token_display_name(card, "9206") == "mainnet-dryrun-claims-status"


def test_nft_table_names_agents_the_same_way_as_the_heading():
    """The tag table and the token page must not disagree about a name."""
    from ccdexplorer.ccdexplorer_site.app.utils import (
        create_dict_for_tabulator_display_for_nft_tokens,
        token_display_name,
    )

    row = {
        "contract": "<10082,0>",
        "token_id": "0300000000000000",
        "last_height_processed": 1,
        "token_metadata": {},
        "raw_metadata": {"name": "PersonalAgent"},
    }
    cell = create_dict_for_tabulator_display_for_nft_tokens("mainnet", None, None, {}, row)
    assert "PersonalAgent" in cell["token_id"]
    assert cell["token_id_download"] == token_display_name(row, row["token_id"])

    # An agent whose card carries no name is still identifiable by its id.
    unnamed = {**row, "token_id": "5e09000000000000", "raw_metadata": {"agent_index": 2398}}
    cell = create_dict_for_tabulator_display_for_nft_tokens("mainnet", None, None, {}, unnamed)
    assert "5e09000000000000" in cell["token_id"]
