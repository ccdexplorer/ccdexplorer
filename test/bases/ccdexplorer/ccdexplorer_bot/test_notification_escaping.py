"""Values a notification did not write itself are text, not markup.

Every body here is HTML: Telegram parses it, and so does a mail client. A
token name, a method name or a domain is chosen by whoever put it on chain,
and a label by whoever typed it, so each is escaped where it is placed.
"""

import datetime as dt
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_bot.bot import messages_logic
from ccdexplorer.ccdexplorer_bot.bot.messages_logic import Mixin
from ccdexplorer.ccdexplorer_bot.bot.utils import esc
from ccdexplorer.ccdexplorer_bot.notification_classes import (
    AddressType,
    CCD_AccountAddress_Complete,
    CCD_Address_Complete,
    EventType,
    EventTypeAccount,
    EventTypeContract,
    EventTypeOther,
    ImpactedAddress,
    NotificationEvent,
    TokenEvent,
)
from ccdexplorer.domain.cis import mintEvent
from ccdexplorer.domain.mongo import MongoTypeTokensTag
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
from ccdexplorer.site_user import SiteUser

MARKUP = "<a href='https://example.org'>claim</a> & more"
ESCAPED = "&lt;a href=&#x27;https://example.org&#x27;&gt;claim&lt;/a&gt; &amp; more"
ACCOUNT = "3pGQi9V9j9YUWXZjdxwCJjnn2Jy76r16LAD9TUvirhv5Wbn6yy"
ACCOUNT_INDEX = 72723
CONTRACT = CCD_ContractAddress(index=9882, subindex=0)


@pytest.fixture
def bot():
    """The message mixin with only what the builders read."""
    mixin = object.__new__(Mixin)
    mixin.labeled_accounts = {}
    mixin.contracts_with_tag_info = {}
    return mixin


def _user() -> SiteUser:
    return SimpleNamespace(accounts={})


def _account(label=None) -> ImpactedAddress:
    return ImpactedAddress(
        label=label,
        address=CCD_Address_Complete(
            account=CCD_AccountAddress_Complete(id=ACCOUNT, index=ACCOUNT_INDEX)
        ),
        address_type=AddressType.account,
    )


def _contract() -> ImpactedAddress:
    return ImpactedAddress(
        address=CCD_Address_Complete(contract=CONTRACT),
        address_type=AddressType.contract,
    )


def _event(event_type: EventType, addresses) -> NotificationEvent:
    return NotificationEvent(
        event_type=event_type,
        block_height=54_154_643,
        block_hash="62ac8e6297a4262a9c294cf353bc63cf715e49692da07b254d31ca8a501b23fb",
        block_slot_time=dt.datetime(2026, 11, 11, 13, 48, 9),
        tx_hash="f62b64e7390f1a0399140b2969a83a041ba2d518f8ab4efadc6d59a310a02628",
        impacted_addresses=addresses,
    )


def _tag(display_name: str) -> MongoTypeTokensTag:
    return MongoTypeTokensTag(
        **{
            "_id": "TAG",
            "contracts": [CONTRACT.to_str()],
            "token_type": "non-fungible",
            "display_name": display_name,
        }
    )


def _both(response) -> list[str]:
    return [response.message_telegram, response.message_email]


# --- the helper -----------------------------------------------------------


def test_markup_becomes_text():
    assert esc(MARKUP) == ESCAPED


def test_a_number_is_still_shown():
    """A label is the account index when nobody named the account."""
    assert esc(72723) == "72723"


def test_nothing_is_shown_as_nothing():
    assert esc(None) == ""


# --- on-chain values ------------------------------------------------------


def test_a_method_name_is_text(bot):
    event = _event(
        EventType(contract=EventTypeContract(receive_name=MARKUP)),
        [_contract()],
    )

    response = bot.define_contract_update_issued_message(None, event, _user())

    for body in _both(response):
        assert ESCAPED in body
        assert MARKUP not in body


def test_a_contract_display_name_is_text(bot):
    bot.contracts_with_tag_info = {CONTRACT.to_str(): _tag(MARKUP)}
    event = _event(
        EventType(contract=EventTypeContract(receive_name="token.mint")),
        [_contract()],
    )

    response = bot.define_contract_update_issued_message(None, event, _user())

    for body in _both(response):
        assert ESCAPED in body
        assert MARKUP not in body


def _mint(token_name) -> NotificationEvent:
    return _event(
        EventType(
            account=EventTypeAccount(
                token_event=TokenEvent(
                    result=mintEvent(tag=254, token_id="00002051", token_amount=1),
                    token_address=f"{CONTRACT.to_str()}-00002051",
                    token_name=token_name,
                )
            )
        ),
        [_account(), _contract()],
    )


def test_a_token_name_is_text(bot):
    bot.contracts_with_tag_info = {CONTRACT.to_str(): _tag("Collection")}

    response = bot.define_token_event_message(_mint(MARKUP), _user())

    for body in _both(response):
        assert f"Name: {ESCAPED}" in body
        assert MARKUP not in body


def test_a_token_display_name_is_text(bot):
    bot.contracts_with_tag_info = {CONTRACT.to_str(): _tag(MARKUP)}

    response = bot.define_token_event_message(_mint("Clark1407"), _user())

    for body in _both(response):
        assert f"Token: {ESCAPED}" in body
        assert MARKUP not in body


def test_a_domain_name_is_text(bot):
    event = _event(EventType(other=EventTypeOther(domain_name_minted=MARKUP)), [])

    response = bot.define_domain_name_minted_message(event, _user())

    for body in _both(response):
        assert ESCAPED in body
        assert MARKUP not in body


# --- labels ---------------------------------------------------------------


def test_a_label_in_the_footer_is_text(bot):
    event = _event(EventType(other=EventTypeOther(domain_name_minted="a.ccd")), [_account(MARKUP)])

    footer = bot.footer(event)

    assert ESCAPED in footer
    assert MARKUP not in footer


def test_a_label_in_a_body_is_text(bot):
    bot.contracts_with_tag_info = {CONTRACT.to_str(): _tag("Collection")}
    bot.labeled_accounts = {ACCOUNT_INDEX: SimpleNamespace(label=MARKUP)}

    response = bot.define_token_event_message(_mint("Clark1407"), _user())

    for body in _both(response):
        assert f"Account {ESCAPED} was affected" in body
        assert MARKUP not in body


def test_a_mail_subject_is_not_markup(bot):
    """A subject is plain text; escaping it would show the entities."""
    bot.contracts_with_tag_info = {CONTRACT.to_str(): _tag("Collection")}
    bot.labeled_accounts = {ACCOUNT_INDEX: SimpleNamespace(label="R&D")}

    response = bot.define_token_event_message(_mint("Clark1407"), _user())

    assert "Account R&D was affected" in response.title_email


# --- every place a value is put -------------------------------------------

BUILDERS = sorted(Path(messages_logic.__file__).parent.glob("messages_definitions_*.py")) + [
    Path(messages_logic.__file__)
]

#: Free text, wherever it comes from. An interpolation of one of these that
#: is not wrapped in esc() is markup waiting for someone to supply it.
FREE_TEXT = re.compile(
    r"\{[^{}]*("
    r"\.label|token_name\b|display_name|module_name|receive_name|init_name|origin_ref"
    r"|contract_info\.id|token_id|lock_id|data_registered|module_deployed"
    r"|message_\b|specification_url|description\.|initialization_parameters"
    r"|domain_name_minted|account_created|metadata_url|node_version_upgrade"
    r")[^{}]*\}"
)


def _unescaped(path: Path) -> list[str]:
    found = []
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("#") or '"title_email"' in line:
            continue
        # A commented-out template after live code on the same line.
        live = line.split("# f", 1)[0]
        for match in FREE_TEXT.finditer(live):
            if not match.group(0).startswith("{esc("):
                found.append(f"{path.name}:{number}: {match.group(0)}")
    return found


@pytest.mark.parametrize("path", BUILDERS, ids=lambda p: p.name)
def test_no_builder_places_free_text_unescaped(path):
    assert _unescaped(path) == []
