"""The status `async_relay` returns carries apprise's reason with it.

The caller decides what a refusal means for the chat, and a 424 alone does
not say: the reason is only in the body apprise answers with.
"""

from unittest.mock import patch

from ccdexplorer.tooter.core import RelayStatus, Tooter, TooterChannel, TooterType

BLOCKED = (
    '{"error":"One or more notifications could not be sent","details":[["WARNING",'
    '"2026-11-13 14:46:30,817","Failed to send Telegram notification to 777: '
    'Forbidden: bot was blocked by the user, error=403."]]}'
)


class _Reply:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def text(self):
        return self._body


class _Session:
    def __init__(self, status, body):
        self.reply = _Reply(status, body)

    def __call__(self, *_args, **_kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def post(self, path, json, headers=None):
        return self.reply


async def _relay(status, body, token="BOT-TOKEN-12345"):
    tooter = Tooter()
    tooter.BOT_API_TOKEN = token
    with patch("ccdexplorer.tooter.core.aiohttp.ClientSession", _Session(status, body)):
        return await tooter.async_relay(
            channel=TooterChannel.BOT,
            title="T",
            body="B",
            notifier_type=TooterType.INFO,
            chat_id=777,
        )


async def test_a_refusal_comes_back_with_its_reason():
    returned = await _relay(424, BLOCKED)

    assert returned == 424
    assert "bot was blocked by the user" in returned.detail


async def test_a_delivery_has_no_reason():
    returned = await _relay(200, "")

    assert returned == 200
    assert returned.detail == ""


async def test_the_reason_does_not_carry_the_token():
    returned = await _relay(424, "tgram://BOT-TOKEN-12345/777 refused")

    assert "BOT-TOKEN-12345" not in returned.detail


def test_the_status_still_behaves_as_a_number():
    status = RelayStatus(424, "why")

    assert status == 424
    assert status != 200
    assert f"{status}" == "424"
