"""The notification footer's own spacing.

Two faults, both visible in a payday notification received on 2026-11-07 and
neither of them fixed by `for_telegram` -- which normalises `<br/>` runs and
cannot help here, because the extra newlines come from empty interpolations
and the mismatched rules were simply written out by hand:

    ___________________________________        <- 35 underscores
    Timestamp: 2026-11-07 09:00:21
    Block: 53,973,452
                                               <- footer_tx is ""
    Account: reza
                                               <- footer_notification_limit is ""
    Notification type: Account - Payday account reward
    (overline rule)                            <- 55 overlines

`footer` only ever reads `self.line_top` and `self.line_bottom`, so it is
called here against a stub rather than a Bot: the rest of Mixin wants gRPC and
Mongo, and none of it is involved in laying out these lines.
"""

from types import SimpleNamespace

import datetime as dt

import pytest

from ccdexplorer.ccdexplorer_bot.bot.messages_logic import Mixin
from ccdexplorer.ccdexplorer_bot.notification_classes import (
    CCD_AccountAddress_Complete,
    CCD_Address_Complete,
    AddressType,
    EventType,
    EventTypeAccount,
    ImpactedAddress,
    NotificationEvent,
)
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockSpecialEvent_PaydayAccountReward,
)

BLOCK_HASH = "df11f7a362ec39964a2200709183c0f67a5306e18593645aefc7cd6d681e8311"
TX_HASH = "215b7203511df2c061b8ad1ac29a495ad6fe6e22e565193c4712e8aa040d77c1"


def _payday_event(tx_hash: str | None = None) -> NotificationEvent:
    """A payday account reward: the case that has neither a tx nor a limit."""
    return NotificationEvent(
        event_type=EventType(
            account=EventTypeAccount(
                payday_account_reward=CCD_BlockSpecialEvent_PaydayAccountReward(
                    account="3pGQi9V9j9YUWXZjdxwCJjnn2Jy76r16LAD9TUvirhv5Wbn6yy",
                    transaction_fees=469_648_250,
                    baker_reward=3_409_076_623,
                    finalization_reward=0,
                )
            )
        ),
        block_height=53_973_452,
        block_hash=BLOCK_HASH,
        block_slot_time=dt.datetime(2026, 11, 7, 9, 0, 21),
        tx_hash=tx_hash,
        impacted_addresses=[
            ImpactedAddress(
                label="reza",
                address=CCD_Address_Complete(
                    account=CCD_AccountAddress_Complete(
                        id="3pGQi9V9j9YUWXZjdxwCJjnn2Jy76r16LAD9TUvirhv5Wbn6yy",
                        index=72723,
                    )
                ),
                address_type=AddressType.account,
            )
        ],
    )


@pytest.fixture
def footer_of():
    """`Mixin.footer` bound to nothing but the two rules it reads."""
    stub = SimpleNamespace(line_top=Mixin.line_top, line_bottom=Mixin.line_bottom)

    def call(notification_event, **kwargs):
        return Mixin.footer(stub, notification_event, **kwargs)

    return call


# --- the rules ------------------------------------------------------------


def test_the_two_rules_are_the_same_width():
    """35 against 55 closed every notification 20 characters wider than it
    opened. The `# TODO make lines the same` this replaces had been in the
    class since the first commit."""
    assert len(Mixin.line_top) == len(Mixin.line_bottom)


def test_the_rules_span_the_longest_footer_line():
    """A rule narrower than the text it brackets looks like a wrapped line
    rather than a rule. The longest is the event type."""
    assert len(Mixin.line_top) >= len("Notification type: Validator - Payday pool reward")


def test_the_rules_are_still_made_of_the_characters_they_were():
    assert set(Mixin.line_top) == {"_"}
    assert set(Mixin.line_bottom) == {"‾"}


# --- the blank lines ------------------------------------------------------


def _visible_lines(footer: str) -> list[str]:
    """Footer lines as Telegram shows them: `<br/>` is the break, so strip the
    tags the way the rendering does and keep every line, blank ones included."""
    import re

    text = re.sub(r"<br\s*/?>", "", footer, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip("\n").split("\n")


def test_a_payday_footer_has_no_blank_lines(footer_of):
    """The fault itself. `footer_tx` and `footer_notification_limit` are both
    empty for a payday, and each used to leave its own newline behind."""
    lines = _visible_lines(footer_of(_payday_event()))

    assert "" not in lines, lines


def test_a_payday_footer_keeps_every_line_it_should(footer_of):
    lines = _visible_lines(footer_of(_payday_event()))

    assert lines[0] == Mixin.line_top
    assert lines[-1] == Mixin.line_bottom
    assert lines[1] == "Timestamp: 2026-11-07 09:00:21"
    assert lines[2] == "Block: 53,973,452"
    assert lines[3] == "Account: reza"
    assert lines[4] == "Notification type: Account - Payday account reward"
    assert len(lines) == 6


def test_dropping_empty_lines_does_not_drop_a_present_one(footer_of):
    """The sibling risk: filtering on falsiness must not swallow a field that
    is there. A transaction hash is the one a payday happens not to have."""
    lines = _visible_lines(footer_of(_payday_event(tx_hash=TX_HASH)))

    assert "" not in lines, lines
    assert f"Transaction: {TX_HASH[:8]}" in lines


def test_a_notification_limit_still_appears(footer_of):
    lines = _visible_lines(footer_of(_payday_event(), notification_limit=1_000))

    assert "" not in lines, lines
    assert "Notification limit: 1,000 CCD" in lines


def test_the_footer_still_opens_and_closes_with_a_newline(footer_of):
    """Callers concatenate it straight onto a message body, so the leading and
    trailing newline the f-string used to provide have to survive."""
    footer = footer_of(_payday_event())

    assert footer.startswith("\n")
    assert footer.endswith("\n")
