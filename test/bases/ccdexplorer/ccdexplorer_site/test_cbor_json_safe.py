"""A registered-data payload must survive `tojson` in the template.

`tx_events.html` renders a Data Registered event's decoded payload with

    <pre class="ccd">{{ event.emit | tojson(indent=2) }}</pre>

and the payload is whatever `cbor2.loads` made of on-chain bytes -- a map whose
keys and values are entirely the sender's choice. CBOR allows map keys JSON does
not (any type at all, including tags), and Jinja's `tojson` passes
`sort_keys=True`, so a map with two differently-typed keys is sorted before it is
even rejected:

    TypeError: '<' not supported between instances of 'int' and 'cbor2.CBORTag'
      templates/tx/tx_events.html:20
      jinja2/filters.py:1721 in do_tojson

That was reached by /mainnet/transaction/a7f3141234e70033... -- a transaction
page that cannot be viewed at all. Anyone can register data, so anyone can mint
an unviewable transaction; the decoded payload has to be made JSON-safe before
it reaches a template.
"""

import json

import cbor2
import jinja2
import pytest

from ccdexplorer.ccdexplorer_site.app.utils import json_safe


def _render(value):
    """Through a real `tojson`, with the sort_keys Jinja actually applies."""
    env = jinja2.Environment(autoescape=True)
    return env.from_string("{{ d | tojson(indent=2) }}").render(d=value)


def test_a_map_with_mixed_key_types_renders():
    """The reported crash: an int key and a tag key in one map."""
    payload = {1: "a", cbor2.CBORTag(42, "x"): "b"}

    rendered = _render(json_safe(payload))

    assert json.loads(rendered.replace("&#34;", '"'))


def test_a_tag_as_a_value_renders():
    payload = {"hash": cbor2.CBORTag(42, "deadbeef")}

    rendered = _render(json_safe(payload))

    assert "deadbeef" in rendered


def test_bytes_become_hex_rather_than_disappearing():
    """A memo's raw bytes are worth showing, and hex is how the site shows them."""
    payload = {"hash": b"\xde\xad\xbe\xef"}

    assert json_safe(payload) == {"hash": "deadbeef"}


def test_keys_that_json_cannot_hold_become_strings():
    payload = {1: "int key", (2, 3): "tuple key", None: "none key"}

    safe = json_safe(payload)

    assert all(isinstance(key, str) for key in safe)
    assert set(safe.values()) == {"int key", "tuple key", "none key"}


def test_a_nested_payload_is_sanitised_all_the_way_down():
    payload = {"outer": {cbor2.CBORTag(7, "inner"): [b"\x01", {2: b"\x02"}]}}

    rendered = _render(json_safe(payload))

    assert "01" in rendered
    assert "02" in rendered


def test_a_payload_that_was_already_json_safe_is_unchanged():
    """The common case must not be reshaped -- the hex hash the site already sets."""
    payload = {"hash": "abc123", "n": 4, "ok": True, "list": [1, 2], "nil": None}

    assert json_safe(payload) == payload


def test_a_set_becomes_a_list():
    """cbor2 decodes tag 258 as a set, which json has no form for."""
    safe = json_safe({"s": {1, 2}})

    assert sorted(safe["s"]) == [1, 2]


@pytest.mark.parametrize(
    "hex_payload",
    [
        # {1: "a", 42("x"): "b"} -- the reported shape, as CBOR on the wire.
        cbor2.dumps({1: "a", cbor2.CBORTag(42, "x"): "b"}).hex(),
        cbor2.dumps({"hash": b"\xde\xad"}).hex(),
        cbor2.dumps({cbor2.CBORTag(2, b"\x01"): {3: b"\x02"}}).hex(),
    ],
)
def test_a_registered_data_event_from_the_wire_can_be_rendered(hex_payload):
    """End to end: the bytes a transaction carries, through the decode the site does."""
    from ccdexplorer.ccdexplorer_site.app.utils import decode_registered_data

    decoded = decode_registered_data(hex_payload)

    assert isinstance(decoded, dict)
    _render(decoded)  # must not raise


# The route that actually crashed
#
# json_safe only helps if the code building the Data Registered event uses it.
# These drive MakeUp, which is what /{net}/transaction/{tx_hash} builds the
# events_list with, so they fail if dressingroom goes back to decoding CBOR
# straight into the template's hands.


def _data_registered_tx(hex_payload: str):
    import datetime as dt

    from ccdexplorer.grpc_client.CCD_Types import (
        CCD_AccountTransactionDetails,
        CCD_AccountTransactionEffects,
        CCD_ShortBlockInfo,
        CCD_TransactionType,
        CCD_BlockItemSummary,
    )

    return CCD_BlockItemSummary(
        index=0,
        energy_cost=0,
        hash="ab" * 32,
        type=CCD_TransactionType(type="account_transaction", contents="register_data"),
        block_info=CCD_ShortBlockInfo(
            hash="cd" * 32,
            height=1,
            slot_time=dt.datetime(2026, 9, 27, tzinfo=dt.timezone.utc),
        ),
        account_transaction=CCD_AccountTransactionDetails(
            cost=0,
            sender="3" * 50,
            outcome="success",
            effects=CCD_AccountTransactionEffects(data_registered=hex_payload),
        ),
    )


async def _events_for(hex_payload: str):
    from types import SimpleNamespace

    import httpx2 as httpx

    from ccdexplorer.ccdexplorer_site.app.classes.dressingroom import (
        MakeUp,
        MakeUpRequest,
        RequestingRoute,
    )

    async with httpx.AsyncClient() as client:
        made_up = await MakeUp(
            makeup_request=MakeUpRequest(
                net="mainnet",
                httpx_client=client,
                tags={},
                user=None,
                # Only the alias lookup touches the app, and an empty map is the
                # answer for a sender that is not an alias.
                app=SimpleNamespace(addresses_to_indexes_complete={"mainnet": {}}),
                requesting_route=RequestingRoute.other,
            )
        ).prepare_for_display(_data_registered_tx(hex_payload), None, account_view=False)

    return made_up.events_list


async def test_the_transaction_that_could_not_be_viewed_now_renders():
    """a7f3141234e70033... -- mixed int and tag keys in one CBOR map."""
    hex_payload = cbor2.dumps({1: "a", cbor2.CBORTag(42, "x"): "b"}).hex()

    events = await _events_for(hex_payload)

    assert len(events) == 1
    _render(events[0].emit)  # must not raise


async def test_registered_bytes_still_reach_the_page_as_hex():
    """The `hash` key the old code special-cased must not lose its value."""
    hex_payload = cbor2.dumps({"hash": b"\xde\xad\xbe\xef"}).hex()

    events = await _events_for(hex_payload)

    assert events[0].emit == {"hash": "deadbeef"}


async def test_registered_json_is_still_read_as_json():
    """The non-CBOR fallback has to survive the change too."""
    hex_payload = json.dumps({"name": "hello"}).encode("utf-8").hex()

    events = await _events_for(hex_payload)

    assert events[0].emit == {"name": "hello"}


async def test_undecodable_bytes_leave_no_payload_rather_than_crashing():
    events = await _events_for("ff" * 8)

    assert events[0].emit is None
