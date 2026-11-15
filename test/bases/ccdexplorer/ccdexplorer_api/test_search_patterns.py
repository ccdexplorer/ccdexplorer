"""A search box is not a regex box.

Four search routes compiled whatever the caller sent as a case-insensitive
regular expression and ran it unanchored against Mongo. An invalid pattern
was a 500; an expensive one burned Mongo CPU; and the account search first
ran an $addFields over the entire collection, with no limit on the way out.

The text is now literal, capped in length, and the query is bounded in both
rows and time.
"""

import re

import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_api.app import utils
from ccdexplorer.ccdexplorer_api.app.routers.v2 import (
    accounts_v2,
    contracts_v2,
    modules_v2,
    tokens_v2,
)

# --- the helper -----------------------------------------------------------


def test_regex_characters_are_taken_literally():
    pattern = utils.search_pattern("a.c")
    assert pattern.search("a.c")
    assert not pattern.search("abc")


def test_matching_is_case_insensitive():
    assert utils.search_pattern("Wccd").search("a wCCD token")


def test_an_invalid_regex_is_just_text_not_a_500():
    assert utils.search_pattern("(unclosed").search("x (unclosed y")


def test_a_pattern_built_for_catastrophic_backtracking_is_inert():
    pattern = utils.search_pattern("(a+)+$")
    assert not pattern.search("a" * 40 + "!")


def test_an_overlong_search_is_refused_not_run():
    with pytest.raises(HTTPException) as e:
        utils.search_pattern("x" * (utils.SEARCH_MAX_LENGTH + 1))
    assert e.value.status_code == 422


def test_the_longest_allowed_search_still_works():
    assert utils.search_pattern("x" * utils.SEARCH_MAX_LENGTH)


# --- the routes -----------------------------------------------------------


class _Captured:
    def __init__(self):
        self.pipeline = None
        self.kwargs = None

    async def __call__(self, db, collection, pipeline, length=None, **kwargs):
        self.pipeline = pipeline
        self.kwargs = kwargs
        return []


def _regexes(node):
    """Every $regex value anywhere in a pipeline."""
    found = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "$regex":
                found.append(v)
            else:
                found.extend(_regexes(v))
    elif isinstance(node, list):
        for v in node:
            found.extend(_regexes(v))
    return found


CASES = [
    (accounts_v2, "search_accounts"),
    (contracts_v2, "search_contracts"),
    (modules_v2, "search_modules"),
    (tokens_v2, "search_tokens"),
]


async def _run(module, name, value, monkeypatch):
    cap = _Captured()
    monkeypatch.setattr(module, "await_await", cap)
    monkeypatch.setattr(module, "net_db", lambda *a, **k: {})
    fn = getattr(module, name)
    fn = getattr(fn, "__wrapped__", fn)
    await fn(request=None, net="mainnet", value=value, mongomotor=None, api_key="k")
    return cap


@pytest.mark.parametrize("module,name", CASES)
async def test_the_user_text_reaches_mongo_as_a_literal(module, name, monkeypatch):
    cap = await _run(module, name, "a.c(", monkeypatch)
    patterns = _regexes(cap.pipeline)
    assert patterns
    for p in patterns:
        compiled = p if hasattr(p, "search") else re.compile(p)
        assert compiled.search("a.c(")
        assert not compiled.search("abcc")


@pytest.mark.parametrize("module,name", CASES)
async def test_the_query_is_bounded_in_time(module, name, monkeypatch):
    cap = await _run(module, name, "abc", monkeypatch)
    assert cap.kwargs.get("maxTimeMS") == utils.SEARCH_MAX_TIME_MS


@pytest.mark.parametrize("module,name", CASES)
async def test_the_query_is_bounded_in_rows(module, name, monkeypatch):
    cap = await _run(module, name, "abc", monkeypatch)
    assert {"$limit": utils.SEARCH_LIMIT} in cap.pipeline
