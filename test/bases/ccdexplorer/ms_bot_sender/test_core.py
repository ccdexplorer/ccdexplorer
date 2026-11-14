import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Import the module under test
import ccdexplorer.ms_bot_sender.core as bot_sender
from ccdexplorer.ccdexplorer_bot.notification_classes import MessageResponse
from ccdexplorer.tooter.core import RelayStatus


def _refused(reason: str) -> RelayStatus:
    """A 424 as `async_relay` returns it, with apprise's reason attached."""
    return RelayStatus(
        424,
        '{"error":"One or more notifications could not be sent",'
        f'"details":[["WARNING","2026-11-13 14:46:30,817","{reason}"]]}}',
    )


def _blocked() -> RelayStatus:
    return _refused(
        "Failed to send Telegram notification to 777: "
        "Forbidden: bot was blocked by the user, error=403."
    )


@pytest.mark.asyncio
async def test_send_to_telegram():
    # Prepare fake message response
    msg = MessageResponse(
        title_telegram="TG Title",
        message_telegram="TG Body",
        title_email="",
        message_email="",
    )

    # Patch the module-level Tooter instance. The relay has to answer 200:
    # anything else is a refusal, and send_to_telegram then consults the users
    # collection to decide whether to resend -- which, left unpatched, reached
    # the live database from this test.
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "users_collection", MagicMock()),
        patch.object(bot_sender, "record_telegram_success"),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=200)

        await bot_sender.send_to_telegram(
            self=None,
            telegram_chat_id=12345,
            message_response=msg,
        )

        mock_tooter.async_relay.assert_awaited_once_with(
            channel=bot_sender.TooterChannel.BOT,
            title="TG Title",
            chat_id=12345,
            body="TG Body",
            notifier_type=bot_sender.TooterType.INFO,
        )


@pytest.mark.asyncio
async def test_send_to_email():
    msg = MessageResponse(
        title_telegram="",
        message_telegram="",
        title_email="Email Title",
        message_email="Email Body",
    )

    with patch.object(bot_sender, "tooter") as mock_tooter:
        mock_tooter.email = MagicMock()

        await bot_sender.send_to_email(
            self=None,
            email_address="user@example.com",
            message_response=msg,
        )

        mock_tooter.email.assert_called_once_with(
            title="Email Title",
            body="Email Body",
            email_address="user@example.com",
        )


def test_send_message_telegram_path():
    payload = {
        "service": "telegram",
        "telegram_chat_id": 999,
        "message_response": {
            "title_telegram": "A",
            "message_telegram": "B",
            "title_email": "",
            "message_email": "",
        },
    }

    with (
        patch.object(bot_sender, "send_to_telegram", AsyncMock()) as mock_tg,
        patch.object(bot_sender, "send_to_email", AsyncMock()) as mock_mail,
    ):
        # send_service_message is a sync Celery task that drives its own event
        # loop internally (asyncio.run), so it must be called from sync code.
        bot_sender.send_service_message.run(payload)  # type: ignore

        mock_tg.assert_awaited_once()
        mock_mail.assert_not_awaited()


def test_send_message_email_path():
    payload = {
        "service": "email",
        "email_address": "test@example.com",
        "message_response": {
            "title_telegram": "",
            "message_telegram": "",
            "title_email": "E",
            "message_email": "F",
        },
    }

    with (
        patch.object(bot_sender, "send_to_email", AsyncMock()) as mock_mail,
        patch.object(bot_sender, "send_to_telegram", AsyncMock()) as mock_tg,
    ):
        bot_sender.send_service_message.run(payload)  # type: ignore

        mock_mail.assert_awaited_once()
        mock_tg.assert_not_awaited()


# --- remembering who blocked the bot --------------------------------------
#
# A 424 from apprise is Telegram refusing delivery because the recipient
# blocked the bot. This worker is the only part of the estate that sees it, so
# it is the only part that can stop the next message being built. See
# ccdexplorer_bot/telegram_delivery.py for the threshold and the queries.


@pytest.mark.asyncio
async def test_a_block_is_counted_against_the_chat():
    msg = MessageResponse(
        title_telegram="T", message_telegram="B", title_email="", message_email=""
    )

    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "record_telegram_success") as success,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=_blocked())

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=msg)

        failure.assert_called_once()
        assert failure.call_args.args[1] == 777
        success.assert_not_called()


@pytest.mark.asyncio
async def test_a_delivery_clears_the_count():
    msg = MessageResponse(
        title_telegram="T", message_telegram="B", title_email="", message_email=""
    )

    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "record_telegram_success") as success,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=200)

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=msg)

        success.assert_called_once()
        failure.assert_not_called()


def test_other_statuses_unsubscribe_nobody():
    """apprise's own bad minute is not the recipient's fault, so a 500 must
    log and leave the count exactly where it was."""
    with (
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "record_telegram_success") as success,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        for status in (500, 502, 401, 0):
            bot_sender.record_delivery(status, 777)

        failure.assert_not_called()
        success.assert_not_called()


def test_a_failed_write_does_not_raise_into_the_task():
    """The message is already sent or already refused by this point. Celery
    retrying the task over a bookkeeping failure would re-send it."""
    with (
        patch.object(bot_sender, "users_collection", side_effect=RuntimeError("no mongo")),
    ):
        bot_sender.record_delivery(424, 777)  # must not raise


# --- a refused message gets one more chance -------------------------------
#
# A 424 is returned, not raised, so the celery task ends in SUCCESS and
# `autoretry_for` -- grpc.RpcError, ConnectionError, TimeoutError -- never sees
# it. Nothing resent a refused message. On 2026-11-09 that lost a payday pool
# reward: apprise took the same chat's payday account reward with a 200 at
# 09:00:47.472 and refused the next one with a 424 at 09:00:47.710.
#
# One retry, and only for a chat that was delivering as of its last send. See
# worth_retrying in ccdexplorer_bot/telegram_delivery.py for why not more.


def _msg():
    return MessageResponse(
        title_telegram="T", message_telegram="B", title_email="", message_email=""
    )


@pytest.mark.asyncio
async def test_a_refused_message_is_sent_once_more():
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=True),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "record_telegram_success") as success,
        patch.object(bot_sender, "users_collection", MagicMock()),
        patch.object(bot_sender.asyncio, "sleep", AsyncMock()),
    ):
        mock_tooter.async_relay = AsyncMock(side_effect=[424, 200])

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        assert mock_tooter.async_relay.await_count == 2
        success.assert_called_once()
        failure.assert_not_called()


@pytest.mark.asyncio
async def test_a_transient_refusal_leaves_no_mark():
    """The whole point of the retry: a 424 between two deliveries to the same
    chat is a dropped message, not someone blocking the bot, and it must not
    push that chat a step closer to being switched off."""
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=True),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "users_collection", MagicMock()),
        patch.object(bot_sender.asyncio, "sleep", AsyncMock()),
    ):
        mock_tooter.async_relay = AsyncMock(side_effect=[424, 200])

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        failure.assert_not_called()


@pytest.mark.asyncio
async def test_a_chat_that_refuses_twice_is_counted_once():
    """Two sends, one verdict. Counting both would halve the twenty-five
    strikes a chat is meant to get."""
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=True),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "users_collection", MagicMock()),
        patch.object(bot_sender.asyncio, "sleep", AsyncMock()),
    ):
        mock_tooter.async_relay = AsyncMock(side_effect=[_blocked(), _blocked()])

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        assert mock_tooter.async_relay.await_count == 2
        failure.assert_called_once()


@pytest.mark.asyncio
async def test_a_chat_already_failing_is_not_sent_twice():
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=False),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=_blocked())

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        assert mock_tooter.async_relay.await_count == 1
        failure.assert_called_once()


@pytest.mark.asyncio
async def test_a_delivered_message_is_not_sent_twice():
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed") as allowed,
        patch.object(bot_sender, "record_telegram_success"),
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=200)

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        assert mock_tooter.async_relay.await_count == 1
        allowed.assert_not_called()


@pytest.mark.asyncio
async def test_the_retry_waits_first():
    """Back-to-back is how the message was refused in the first place. The
    module's own sleep is patched, not the clock, so this stays a test of what
    send_to_telegram does rather than of asyncio."""
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=True),
        patch.object(bot_sender, "record_telegram_success"),
        patch.object(bot_sender, "users_collection", MagicMock()),
        patch.object(bot_sender.asyncio, "sleep", AsyncMock()) as slept,
    ):
        mock_tooter.async_relay = AsyncMock(side_effect=[500, 200])

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        slept.assert_awaited_once_with(bot_sender.TELEGRAM_RETRY_DELAY_S)
        assert bot_sender.TELEGRAM_RETRY_DELAY_S > 0


@pytest.mark.asyncio
async def test_the_retry_sends_the_same_message():
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=True),
        patch.object(bot_sender, "record_telegram_failure"),
        patch.object(bot_sender, "users_collection", MagicMock()),
        patch.object(bot_sender.asyncio, "sleep", AsyncMock()),
    ):
        mock_tooter.async_relay = AsyncMock(side_effect=[424, 424])

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        first, second = mock_tooter.async_relay.await_args_list
        assert first == second
        assert first.kwargs["chat_id"] == 777
        assert first.kwargs["body"] == "B"


def test_a_counter_we_cannot_read_does_not_resend():
    """If the collection is unreachable we cannot tell a blocked chat from a
    working one, and guessing wrong resends to every blocked chat. The send
    already happened either way."""
    with patch.object(bot_sender, "users_collection", side_effect=RuntimeError("no mongo")):
        assert bot_sender.retry_allowed(777) is False


def test_a_working_chat_is_worth_a_retry():
    users = MagicMock()
    users.return_value.find_one.return_value = {"telegram_chat_id": 777}

    with patch.object(bot_sender, "users_collection", users):
        assert bot_sender.retry_allowed(777) is True


def test_a_chat_with_failures_behind_it_is_not():
    users = MagicMock()
    users.return_value.find_one.return_value = {
        "telegram_chat_id": 777,
        "telegram_consecutive_failures": 4,
    }

    with patch.object(bot_sender, "users_collection", users):
        assert bot_sender.retry_allowed(777) is False


# --- only a recipient who is gone is a strike -----------------------------
#
# A 424 is apprise failing to deliver, whatever the cause. Counting every one
# let an outage, or a message Telegram would not parse, work toward switching
# a subscription off.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason",
    [
        "A connection error occurred sending Telegram:777 notification.",
        "Failed to send Telegram notification to 777: Bad Request: can't parse entities, error=400.",
        "Failed to send Telegram notification to 777: Bad Request: message is too long, error=400.",
    ],
)
async def test_a_refusal_that_is_not_the_recipient_is_no_strike(reason):
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=False),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "record_telegram_success") as success,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=_refused(reason))

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        failure.assert_not_called()
        success.assert_not_called()


@pytest.mark.asyncio
async def test_a_424_with_no_reason_is_no_strike():
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=False),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=424)

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        failure.assert_not_called()


@pytest.mark.asyncio
async def test_a_deactivated_recipient_is_a_strike():
    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "retry_allowed", return_value=False),
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(
            return_value=_refused(
                "Failed to send Telegram notification to 777: "
                "Forbidden: user is deactivated, error=403."
            )
        )

        await bot_sender.send_to_telegram(self=None, telegram_chat_id=777, message_response=_msg())

        failure.assert_called_once()
