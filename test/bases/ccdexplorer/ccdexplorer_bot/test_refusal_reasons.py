"""Which refused sends say something about the recipient.

A 424 is apprise failing to deliver, for whatever reason. Over ten days to
2026-11-13 the sender logged 135 of them: 41 were apprise not reaching
Telegram at all, 21 a chat id that does not exist, and 73 a recipient who is
gone. Only the last kind is a reason to stop sending to a chat, and counting
the rest lets anything that can make a send fail -- an outage, or a message
Telegram will not parse -- switch a working subscription off.
"""

import pytest

from ccdexplorer.ccdexplorer_bot.telegram_delivery import refusal_is_a_block


def _body(reason: str) -> str:
    """apprise's answer to a refused notify, as the sender logs it."""
    return (
        '{"error":"One or more notifications could not be sent",'
        f'"details":[["WARNING","2026-11-13 14:46:30,817","{reason}"]]}}'
    )


@pytest.mark.parametrize(
    "reason",
    [
        "Failed to send Telegram notification to 777: Forbidden: bot was blocked by the user, error=403.",
        "Failed to send Telegram notification to 777: Forbidden: user is deactivated, error=403.",
    ],
)
def test_a_recipient_who_is_gone_is_a_block(reason):
    assert refusal_is_a_block(_body(reason)) is True


@pytest.mark.parametrize(
    "reason",
    [
        "A connection error occurred sending Telegram:777 notification.",
        "Failed to send Telegram notification to 777: Bad Request: can't parse entities, error=400.",
        "Failed to send Telegram notification to 777: Bad Request: message is too long, error=400.",
        "Failed to send Telegram notification to 777: Bad Request: chat not found, error=400.",
        "Failed to send Telegram notification to 777: Too Many Requests: retry after 5, error=429.",
    ],
)
def test_any_other_refusal_is_not(reason):
    assert refusal_is_a_block(_body(reason)) is False


@pytest.mark.parametrize("detail", ["", None, "(no body)", "(body unreadable: OSError())"])
def test_a_refusal_with_no_reason_is_not(detail):
    """Not knowing why is not knowing it was the recipient."""
    assert refusal_is_a_block(detail) is False
