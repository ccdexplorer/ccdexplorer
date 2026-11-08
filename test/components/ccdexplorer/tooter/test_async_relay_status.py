"""`async_relay` reports what apprise said, rather than only printing it.

A 424 means Telegram refused delivery because the recipient blocked the bot.
Printing it and returning None is what let four blocked chats be sent to for
weeks: the caller -- ms_bot_sender, the only part of the estate holding a
database -- had no way to learn of it. The status is the whole seam, so it is
worth a test of its own.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ccdexplorer.tooter.core import Tooter, TooterChannel, TooterType


class _Session:
    """The two nested `async with` blocks of aiohttp, and nothing else."""

    def __init__(self, status):
        self.status = status
        self.posted = []

    def __call__(self, *_args, **_kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def post(self, path, json):
        self.posted.append((path, json))
        return _Response(self.status)


class _Response:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return SimpleNamespace(status=self.status)

    async def __aexit__(self, *_):
        return False


async def _relay(status):
    session = _Session(status)
    with patch("ccdexplorer.tooter.core.aiohttp.ClientSession", session):
        returned = await Tooter().async_relay(
            channel=TooterChannel.BOT,
            title="T",
            body="B",
            notifier_type=TooterType.INFO,
            chat_id=7534089330,
        )
    return returned, session.posted


@pytest.mark.parametrize("status", [200, 424, 500])
async def test_the_status_comes_back(status):
    returned, _ = await _relay(status)

    assert returned == status


async def test_the_message_is_still_posted():
    """Returning the status must not change what goes out, or whether it does
    -- a 424 is Telegram's answer to a request that was made."""
    _, posted = await _relay(424)

    assert len(posted) == 1
    path, payload = posted[0]
    assert path == "/notify"
    assert payload["format"] == "html"
    assert "7534089330" in payload["urls"]
