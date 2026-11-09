"""`async_relay` reports what apprise said, rather than only printing it.

A 424 means Telegram refused delivery because the recipient blocked the bot.
Printing it and returning None is what let four blocked chats be sent to for
weeks: the caller -- ms_bot_sender, the only part of the estate holding a
database -- had no way to learn of it. The status is the whole seam, so it is
worth a test of its own.

The status alone turned out not to be enough. On 2026-11-09 one of the two
payday notifications to a working chat came back 424 while the other, 240 ms
earlier, came back 200. Nothing could say why: the status was logged and
apprise's response body -- the only place Telegram's own reason existed -- was
dropped on the floor, and apprise-api keeps no application log. So the body is
logged too now, with the bot token scrubbed out of it, because apprise echoes
the url it was given and that url carries the token.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ccdexplorer.tooter.core import Tooter, TooterChannel, TooterType

UNREADABLE = object()
"""A body that raises when read, as a dropped connection would."""


class _Session:
    """The two nested `async with` blocks of aiohttp, and nothing else."""

    def __init__(self, status, body=None):
        self.status = status
        self.body = body
        self.posted = []

    def __call__(self, *_args, **_kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def post(self, path, json, headers=None):
        self.posted.append((path, json, headers))
        return _Response(self.status, self.body)


class _Response:
    def __init__(self, status, body=None):
        self.status = status
        self.body = body

    async def __aenter__(self):
        if self.body is None:
            # No `text` attribute at all, which is how the original fake
            # behaved and how a stubbed response elsewhere might still behave.
            return SimpleNamespace(status=self.status)
        return _Reply(self.status, self.body)

    async def __aexit__(self, *_):
        return False


class _Reply:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    async def text(self):
        if self._body is UNREADABLE:
            raise OSError("connection reset")
        return self._body


async def _relay(status, body=None, token="BOT-TOKEN-12345", channel=TooterChannel.BOT):
    session = _Session(status, body)
    tooter = Tooter()
    tooter.BOT_API_TOKEN = token
    tooter.NOTIFIER_API_TOKEN = token
    with patch("ccdexplorer.tooter.core.aiohttp.ClientSession", session):
        returned = await tooter.async_relay(
            channel=channel,
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
    path, payload, _headers = posted[0]
    assert path == "/notify"
    assert payload["format"] == "html"
    assert "7534089330" in payload["urls"]


async def test_apprise_is_asked_for_its_log():
    """The reason a provider refused appears only in apprise's own captured
    log, which it renders into the response body at the level the request
    asks for. Without this header the level is the server's to choose."""
    _, posted = await _relay(200)

    _path, _payload, headers = posted[0]
    assert headers["X-Apprise-Log-Level"] == "WARNING"


# --- what apprise said ----------------------------------------------------


async def test_the_reason_apprise_gave_is_logged(capsys):
    """The point of the whole change: a 424 that is not a block has to be
    explicable after the fact, and this is the only record of why."""
    await _relay(424, body='{"error": "Bad Request: can\'t parse entities"}')

    printed = capsys.readouterr().out
    assert "424" in printed
    assert "can't parse entities" in printed


async def test_the_bot_token_is_never_printed(capsys):
    """apprise echoes the url it was handed, and that url carries the token.
    Logging the body verbatim would publish it to every container log and to
    whatever ships them."""
    await _relay(424, body="Failed to send to tgram://BOT-TOKEN-12345/7534089330")

    printed = capsys.readouterr().out
    assert "BOT-TOKEN-12345" not in printed
    assert "7534089330" in printed


async def test_a_body_that_cannot_be_read_still_reports_the_status(capsys):
    """Reading the body is a second network read and can fail on its own. The
    status is what the caller acts on, so it must survive that."""
    returned, _ = await _relay(424, body=UNREADABLE)

    assert returned == 424
    assert "424" in capsys.readouterr().out


async def test_a_response_without_a_body_to_read_still_reports_the_status():
    returned, _ = await _relay(500)

    assert returned == 500


async def test_an_empty_body_says_so_rather_than_printing_nothing(capsys):
    await _relay(424, body="   ")

    printed = capsys.readouterr().out
    assert "424" in printed
    assert "no body" in printed


async def test_a_long_body_is_truncated(capsys):
    await _relay(424, body="x" * 5_000)

    printed = capsys.readouterr().out
    assert len(printed) < 1_000


async def test_a_delivered_message_logs_nothing(capsys):
    """The overwhelming majority of sends are 200s. Reading and printing a
    body for each one would cost a network read per notification and bury the
    failures."""
    await _relay(200, body="OK")

    assert capsys.readouterr().out == ""
