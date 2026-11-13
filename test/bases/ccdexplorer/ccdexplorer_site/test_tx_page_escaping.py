"""What a transaction carries must reach its page as text, not as markup.

The transaction page prints its events with `|safe`, because the strings are
built here with links in them. Everything interpolated into those strings is
therefore markup unless it is escaped first, and a good deal of it is written
by whoever sent the transaction: a memo is free bytes, a contract's init and
receive names take punctuation, and a schema is embedded in the sender's own
module -- so both the keys and the values of anything "schema parsed" are
theirs to choose.

Escaping covers text. A link is a second problem: an escaped
`href="javascript:..."` is still a link that runs script, so URLs go through
safe_url as well.
"""

import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import cbor2
import jinja2
import pytest

PAYLOAD = "<img src=x onerror=alert(document.domain)>"
#: No whitespace, so it is a legal contract or entrypoint name.
NAME_PAYLOAD = "<svg/onload=alert(1)>"
SCRIPT_URL = "javascript:alert(document.domain)"

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"


def _utils():
    """Resolved per call: a full-suite run re-imports the app package."""
    from ccdexplorer.ccdexplorer_site.app import utils

    return utils


def _app():
    return SimpleNamespace(
        api_url="http://api.invalid",
        canonical_addresses={"mainnet": {}},
        addresses_to_indexes={"mainnet": {}},
        schema_cache={"mainnet": {}},
        plt_cache={"mainnet": {}},
    )


def _schema(value):
    return _utils().print_schema_dict(value, "mainnet", None, {}, _app())


# --- shorten_address -------------------------------------------------------
#
# Used as a filter with |safe on token and account pages, and inside
# print_schema_dict. It wraps its argument in a div and returned it unescaped.


@pytest.mark.parametrize("value", [PAYLOAD[:16], PAYLOAD])
def test_shorten_address_escapes_what_it_wraps(value):
    out = str(_utils().shorten_address(value))

    assert "<img" not in out
    assert "&lt;img" in out
    assert "class=" in out and "<div" in out  # its own markup is untouched


def test_shorten_address_escapes_the_shortened_form_too():
    out = str(_utils().shorten_address("<b>" + "a" * 20 + "<i>", True))

    assert "<b>" not in out and "<i>" not in out


def test_shorten_address_is_not_escaped_twice_by_a_template():
    """Some templates use it without |safe; its own div must survive both."""
    env = jinja2.Environment(autoescape=True)
    env.filters["shorten_address"] = _utils().shorten_address

    plain = env.from_string("{{ v|shorten_address }}").render(v="abc")
    marked = env.from_string("{{ v|shorten_address|safe }}").render(v="a&b")

    assert plain == "<div class='ccd'>abc</div>"
    assert marked == "<div class='ccd'>a&amp;b</div>"


# --- print_schema_dict -----------------------------------------------------


def test_a_schema_value_is_escaped():
    out = _schema({"note": PAYLOAD[:20]})

    assert "<img" not in out
    assert "&lt;img" in out


def test_a_schema_key_is_escaped():
    out = _schema({PAYLOAD: "1"})

    assert "<img" not in out


def test_a_long_schema_value_that_is_no_address_is_text():
    """Anything 29 characters or longer was taken for an account address and
    handed to account_link, which read the `<` as a contract address and
    raised -- so the same value that injected markup also took the page down."""
    out = _schema({"note": PAYLOAD})

    assert "<img" not in out
    assert "&lt;img" in out


@pytest.mark.parametrize("key", ["owner", "from", "to", "amount", "data", "tokens"])
def test_the_keys_with_their_own_layout_escape_too(key):
    out = _schema({key: PAYLOAD})

    assert "<img" not in out


def test_a_nested_value_is_escaped():
    out = _schema({"outer": {"inner": PAYLOAD}})

    assert "<img" not in out


def test_an_account_variant_holding_markup_is_text():
    out = _schema({"to": {"Account": [PAYLOAD]}})

    assert "<img" not in out


def test_a_parameter_that_is_not_a_mapping_is_escaped():
    out = _schema(PAYLOAD)

    assert "<img" not in out
    assert "&lt;img" in out


@pytest.mark.parametrize(
    "value",
    [
        {"url": SCRIPT_URL},
        {"meta": {"url": SCRIPT_URL}},
        {"meta": {"metadata_url": {"url": SCRIPT_URL}}},
    ],
)
def test_a_script_url_is_never_a_link(value):
    out = _schema(value)

    assert "href='javascript:" not in out
    assert 'href="javascript:' not in out


@pytest.mark.parametrize(
    "value",
    [
        {"url": "x' onmouseover='alert(1)"},
        {"meta": {"url": "x' onmouseover='alert(1)"}},
    ],
)
def test_a_url_cannot_break_out_of_its_attribute(value):
    assert "' onmouseover='" not in _schema(value)


def test_a_real_url_is_still_a_link():
    out = _schema({"meta": {"url": "https://example.org/m.json"}})

    assert "<a href='https://example.org/m.json'>" in out


def test_an_ordinary_parameter_renders_as_it_did():
    address = "3" * 50
    out = _schema({"to": {"Account": [address]}, "amount": "100", "note": "hello"})

    assert f'href="/mainnet/account/{int(address)}"' in out
    assert "amount: <span class='ccd'>100</span><br/>" in out
    assert "note: hello<br/>" in out


# --- account_link ----------------------------------------------------------


def test_account_link_survives_a_value_that_is_no_address():
    out = _utils().account_link(PAYLOAD, "mainnet", app=_app())

    assert "<img" not in out


def test_account_link_escapes_an_address_it_does_not_know():
    out = _utils().account_link('x"onmouseover="alert(1)', "mainnet", tags={}, app=_app())

    assert '"onmouseover="' not in out


# --- the memo --------------------------------------------------------------


def test_a_memo_is_escaped_for_display():
    hex_memo = cbor2.dumps(PAYLOAD).hex()

    out = _utils().memo_for_display(hex_memo)

    assert out == "Memo: &lt;img src=x onerror=alert(document.domain)&gt;"


def test_a_memo_that_decodes_to_a_structure_is_escaped():
    out = _utils().memo_for_display(cbor2.dumps({"k": PAYLOAD}).hex())

    assert "<img" not in out


def test_no_memo_is_no_line():
    assert _utils().memo_for_display(None) is None
    assert _utils().memo_for_display("") is None


# --- through MakeUp, which is what the page is built from ------------------


def _tx(effects: dict, contents: str):
    from ccdexplorer.grpc_client.CCD_Types import (
        CCD_AccountTransactionDetails,
        CCD_AccountTransactionEffects,
        CCD_BlockItemSummary,
        CCD_ShortBlockInfo,
        CCD_TransactionType,
    )

    return CCD_BlockItemSummary(
        index=0,
        energy_cost=0,
        hash="ab" * 32,
        type=CCD_TransactionType(type="account_transaction", contents=contents),
        block_info=CCD_ShortBlockInfo(
            hash="cd" * 32,
            # Past the CNS era, which has a code path of its own.
            height=30_000_000,
            slot_time=dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc),
        ),
        account_transaction=CCD_AccountTransactionDetails(
            cost=0,
            sender="3" * 50,
            outcome="success",
            effects=CCD_AccountTransactionEffects(**effects),
        ),
    )


async def _made_up(monkeypatch, tx, route="transaction"):
    import httpx2 as httpx

    from ccdexplorer.ccdexplorer_site.app.classes import dressingroom

    async def no_api(url, httpx_client):
        return SimpleNamespace(ok=False, return_value=None)

    async def no_schema(self, contract_address):
        return None, None

    monkeypatch.setattr(dressingroom, "get_url_from_api", no_api)
    monkeypatch.setattr(dressingroom.MakeUp, "get_schema_from_source", no_schema)

    async with httpx.AsyncClient() as client:
        return await dressingroom.MakeUp(
            makeup_request=dressingroom.MakeUpRequest(
                net="mainnet",
                httpx_client=client,
                tags={},
                user=None,
                app=_app(),
                requesting_route=dressingroom.RequestingRoute(route),
            )
        ).prepare_for_display(tx, "", False)


def _all_markup(made_up) -> str:
    """Everything tx_events.html prints with |safe."""
    parts = []
    for event in made_up.events_list:
        for value in (event.event, event.update, event.emit):
            if value is not None and not isinstance(value, dict):
                parts.append(str(value))
        for logged in event.logged_events or []:
            parts.extend(str(v) for v in (logged.event, logged.update) if v is not None)
    return "\n".join(parts)


async def test_a_transfer_memo_reaches_the_page_escaped(monkeypatch):
    tx = _tx(
        {
            "account_transfer": {
                "amount": 1_000_000,
                "receiver": "4" * 50,
                "memo": cbor2.dumps(PAYLOAD).hex(),
            }
        },
        "account_transfer",
    )

    made_up = await _made_up(monkeypatch, tx)
    page = _all_markup(made_up)

    assert "<img" not in page
    assert "Memo: &lt;img" in page
    assert "<a href=" in page  # the from/to links are still links


async def test_the_memo_kept_for_export_is_not_escaped(monkeypatch):
    """It becomes a CSV cell, where an escaped ampersand is corruption."""
    tx = _tx(
        {
            "account_transfer": {
                "amount": 1,
                "receiver": "4" * 50,
                "memo": cbor2.dumps("Tom & Jerry").hex(),
            }
        },
        "account_transfer",
    )

    made_up = await _made_up(monkeypatch, tx)

    assert made_up.dct["memo"] == "Memo: Tom & Jerry"


async def test_a_scheduled_transfer_memo_is_escaped(monkeypatch):
    tx = _tx(
        {
            "transferred_with_schedule": {
                "receiver": "4" * 50,
                "amount": [{"timestamp": dt.datetime(2027, 1, 1, tzinfo=dt.UTC), "amount": 5}],
                "memo": cbor2.dumps(PAYLOAD).hex(),
            }
        },
        "transfer_with_schedule",
    )

    page = _all_markup(await _made_up(monkeypatch, tx))

    assert "<img" not in page
    assert "Memo: &lt;img" in page


async def test_a_plt_transfer_memo_is_escaped(monkeypatch):
    """Printed on the transaction page and, joined into one cell, in the PLT
    lock transactions table."""
    tx = _tx(
        {
            "token_update_effect": {
                "events": [
                    {
                        "token_id": "EURX",
                        "transfer_event": {
                            "from": {"account": "3" * 50},
                            "to": {"account": "4" * 50},
                            "amount": {"value": "100", "decimals": 2},
                            "memo": cbor2.dumps(PAYLOAD).hex(),
                        },
                    }
                ]
            }
        },
        "token_update",
    )

    page = _all_markup(await _made_up(monkeypatch, tx))

    assert "<img" not in page
    assert "Memo: &lt;img" in page


async def test_a_contract_name_is_escaped_on_init(monkeypatch):
    tx = _tx(
        {
            "contract_initialized": {
                "contract_version": 1,
                "origin_ref": "ef" * 32,
                "address": {"index": 1, "subindex": 0},
                "amount": 0,
                "init_name": f"init_{NAME_PAYLOAD}",
                "events": [],
            }
        },
        "contract_initialized",
    )

    page = _all_markup(await _made_up(monkeypatch, tx))

    assert "<svg" not in page
    assert "init_&lt;svg" in page
    assert 'href="/mainnet/module/' in page  # the module link is still a link


async def test_an_entrypoint_name_is_escaped_on_update(monkeypatch):
    tx = _tx(
        {
            "contract_update_issued": {
                "effects": [
                    {
                        "updated": {
                            "contract_version": 1,
                            "address": {"index": 1, "subindex": 0},
                            "instigator": {"account": "3" * 50},
                            "amount": 0,
                            "parameter": "00",
                            "receive_name": f"{NAME_PAYLOAD}.{NAME_PAYLOAD}",
                            "events": [],
                        }
                    }
                ]
            }
        },
        "contract_update_issued",
    )

    page = _all_markup(await _made_up(monkeypatch, tx))

    assert "<svg" not in page
    assert "Function: &lt;svg" in page


async def test_a_validator_metadata_url_with_a_script_scheme_is_no_link(monkeypatch):
    tx = _tx(
        {
            "baker_configured": {
                "events": [{"baker_set_metadata_url": {"baker_id": 1, "url": SCRIPT_URL}}]
            }
        },
        "baker_configured",
    )

    page = _all_markup(await _made_up(monkeypatch, tx))

    assert "href='javascript:" not in page
    # Still shown, so the reader can see what was set.
    assert "javascript:alert" in page


async def test_a_validator_metadata_url_is_still_a_link(monkeypatch):
    tx = _tx(
        {
            "baker_configured": {
                "events": [
                    {"baker_set_metadata_url": {"baker_id": 1, "url": "https://example.org/v"}}
                ]
            }
        },
        "baker_configured",
    )

    page = _all_markup(await _made_up(monkeypatch, tx))

    assert "<a href='https://example.org/v'>" in page


def test_a_schema_event_name_is_escaped():
    """The event's name comes out of the schema, which is the sender's."""
    from ccdexplorer.ccdexplorer_site.app.classes import dressingroom

    schema = SimpleNamespace(event_to_json=lambda name, raw: {PAYLOAD: {"n": 1}})
    make_up = dressingroom.MakeUp.__new__(dressingroom.MakeUp)

    logged, ok = make_up.try_schema_parsing_for_event(
        [], schema, "module", "00", "mainnet", None, {}, _app()
    )

    assert ok
    assert "<img" not in logged[0].event
    assert "&lt;img" in logged[0].event


# --- the logged-event templates --------------------------------------------
#
# Autoescape already covers their text. Their links are the gap: a CIS-2
# metadata url, a CIS-4 schema ref and a CIS-8004 agent uri are all set by the
# contract and were dropped into href as they came.


def _render(template: str, **context) -> str:
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(PROJECT / "templates"), autoescape=True)
    env.filters["safe_url"] = _utils().safe_url
    env.filters["shorten_address"] = _utils().shorten_address
    return env.get_template(template).render(net="mainnet", **context)


def _event(**fields):
    return SimpleNamespace(model_fields_set=set(fields), **fields)


@pytest.mark.parametrize(
    "template, result",
    [
        ("tx/logged_events/cis-2.html", _event(metadata=SimpleNamespace(url=SCRIPT_URL))),
        ("tx/logged_events/cis-4.html", _event(metadata=SimpleNamespace(url=SCRIPT_URL))),
        ("tx/logged_events/cis-4.html", _event(schema_ref=SimpleNamespace(url=SCRIPT_URL))),
        ("tx/logged_events/cis-8004.html", _event(agent_uri=SCRIPT_URL)),
    ],
)
def test_a_logged_event_never_links_to_a_script_url(template, result):
    out = _render(template, result=result, token_address_as_class=None)

    assert "href='javascript:" not in out


def test_a_logged_event_still_links_to_a_real_url():
    out = _render(
        "tx/logged_events/cis-4.html",
        result=_event(metadata=SimpleNamespace(url="https://example.org/m.json")),
    )

    assert "href='https://example.org/m.json'" in out
