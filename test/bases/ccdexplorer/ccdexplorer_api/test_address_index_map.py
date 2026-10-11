"""The whole address-to-index map, in one response.

The site used to hold this as two committed pickles -- 79MB of tracked
binary, refreshed by hand, and 437MB resident once unpickled because the
larger one rehydrated a document per account. It needs two flat maps, so the
api hands it the two columns those are built from and nothing else.

Streamed rather than assembled, so serving 105,000 accounts does not cost
the api a copy of all of them.
"""

from ccdexplorer.ccdexplorer_api.app.routers.v2.accounts_v2 import (
    ADDRESS_MAP_CHUNK,
    address_map_chunks,
    address_map_row,
)


def _doc(address: str, index: int) -> dict:
    """A stable_address_info document, as to_collection() writes it."""
    return {"_id": address[:29], "account_address": address, "account_index": index}


ADDRESS = "3Y1RLgi5pW3x96xZ7CiHUKTOrOrgnSk2tzMEXBhWhVvHDOwgkC"


async def _cursor(docs):
    """Stands in for the motor cursor the route hands it."""
    for doc in docs:
        yield doc


async def _body(docs) -> str:
    return "".join([chunk async for chunk in address_map_chunks(_cursor(docs))])


def test_a_row_is_the_address_and_the_index():
    assert address_map_row(_doc(ADDRESS, 1234)) == [ADDRESS, 1234]


def test_the_key_is_not_sent_because_it_is_derivable():
    """_id is account_address[:29] -- AccountInfoStable.to_collection sets it
    that way -- so sending it too would be 3MB of data the caller can slice
    off the address it already has."""
    row = address_map_row(_doc(ADDRESS, 1234))

    assert len(row) == 2
    assert row[0][:29] == _doc(ADDRESS, 1234)["_id"]


async def test_the_chunks_are_one_json_array():
    docs = [_doc(ADDRESS, 1), _doc(ADDRESS, 2)]

    body = await _body(docs)

    assert body.startswith("[")
    assert body.endswith("]")
    assert body.count("],[") == 1, "rows are comma separated, not concatenated"


async def test_the_chunks_parse_as_the_rows_that_went_in():
    import json

    docs = [_doc(ADDRESS, index) for index in range(5)]

    parsed = json.loads(await _body(docs))

    assert parsed == [[ADDRESS, index] for index in range(5)]


async def test_no_accounts_is_an_empty_array_not_a_broken_one():
    """A fresh devnet has none, and "[" alone is not parseable json."""
    import json

    assert json.loads(await _body([])) == []


async def test_one_account_needs_no_separator():
    import json

    assert json.loads(await _body([_doc(ADDRESS, 7)])) == [[ADDRESS, 7]]


async def test_the_rows_are_handed_out_in_batches():
    """The point of streaming: the response must not be one string holding
    every account. Measured at 6.6MB for mainnet, which is what the api
    would otherwise buffer per caller."""
    docs = [_doc(ADDRESS, index) for index in range(ADDRESS_MAP_CHUNK * 2 + 1)]

    chunks = [chunk async for chunk in address_map_chunks(_cursor(docs))]

    assert len(chunks) > 3, "everything came out in one piece"
    assert max(chunk.count("],[") + 1 for chunk in chunks) <= ADDRESS_MAP_CHUNK


def test_the_chunk_size_is_big_enough_to_be_worth_batching():
    assert ADDRESS_MAP_CHUNK >= 1000
