"""Which symbols the nightrunner fetches a historical rate for.

`get_price_from` names the symbol that prices a token, not whether to price it.
Read as a flag, with the token's own _id used as the key, this put ten
partitions in the set that nothing can act on -- tETH, tUSDC, tWBTC and the
rest have no CoinGecko id, so coingecko_historical has nothing to request.
They never failed, because perform_historical_rates_update returns True for
anything outside its allowlist, so the cost was silent: accounts_repo asks for
a run per partition every night, and each of those paid a process start and a
definitions import to do nothing.

_partitions builds its StaticPartitionsDefinition at import, which means one
Mongo query at import. These tests therefore install a stub before the module
is first imported, rather than patching after the fact.
"""

import sys
from types import SimpleNamespace

import pytest
from ccdexplorer.mongodb import Collections


class _TagCollection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, *args, **kwargs):
        return list(self.docs)


@pytest.fixture
def partitions_module(monkeypatch):
    """Import _partitions against a stubbed Mongo, freshly each time."""

    def _load(token_docs, plt_docs=()):
        from ccdexplorer.dagster_nightrunner.src import _resources

        mongodb = SimpleNamespace(
            mainnet={
                Collections.tokens_tags: _TagCollection(token_docs),
                Collections.plts_tags: _TagCollection(plt_docs),
            }
        )
        monkeypatch.setattr(_resources, "shared_mongodb", lambda: mongodb)
        monkeypatch.delitem(
            sys.modules, "ccdexplorer.dagster_nightrunner.src._partitions", raising=False
        )
        import importlib

        return importlib.import_module("ccdexplorer.dagster_nightrunner.src._partitions")

    return _load


def test_a_wrapped_token_is_fetched_under_the_symbol_that_prices_it(partitions_module):
    mod = partitions_module(
        [
            {"_id": "tETH", "get_price_from": "ETH"},
            {"_id": "ETH", "get_price_from": "ETH"},
            {"_id": "tWBTC", "get_price_from": "BTC"},
        ]
    )

    # One partition for ETH, not one for ETH and a second for tETH that no
    # consumer reads and CoinGecko cannot resolve.
    assert mod.current_token_keys() == ["BTC", "ETH"]


def test_a_symbol_no_token_is_named_after_is_still_included(partitions_module):
    """tPOL is priced by MATIC; keying on _id never produced MATIC."""
    mod = partitions_module([{"_id": "tPOL", "get_price_from": "MATIC"}])

    assert mod.current_token_keys() == ["MATIC"]


def test_tokens_without_a_price_source_are_left_out(partitions_module):
    mod = partitions_module(
        [
            {"_id": "CCD", "get_price_from": "CCD"},
            {"_id": "NOPRICE"},
            {"_id": "X", "get_price_from": None},
        ]
    )

    assert mod.current_token_keys() == ["CCD"]


def test_plts_are_included_by_their_symbol_too(partitions_module):
    mod = partitions_module(
        [{"_id": "CCD", "get_price_from": "CCD"}], [{"_id": "EURR", "get_price_from": "EURR"}]
    )

    assert mod.current_token_keys() == ["CCD", "EURR"]


def test_the_partition_definition_is_built_from_those_symbols(partitions_module):
    """accounts_repo asks for one run per key in this set every night."""
    mod = partitions_module(
        [{"_id": "tETH", "get_price_from": "ETH"}, {"_id": "tUSDC", "get_price_from": "USDC"}]
    )

    assert sorted(mod.partitions_def_tokens.get_partition_keys()) == ["ETH", "USDC"]
