import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Import the module under test
import ccdexplorer.ms_bot_sender.core as bot_sender
from ccdexplorer.ccdexplorer_bot.notification_classes import MessageResponse


@pytest.mark.asyncio
async def test_send_to_telegram():
    # Prepare fake message response
    msg = MessageResponse(
        title_telegram="TG Title",
        message_telegram="TG Body",
        title_email="",
        message_email="",
    )

    # Patch the module-level Tooter instance
    with patch.object(bot_sender, "tooter") as mock_tooter:
        mock_tooter.async_relay = AsyncMock()

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
async def test_a_424_is_counted_against_the_chat():
    msg = MessageResponse(
        title_telegram="T", message_telegram="B", title_email="", message_email=""
    )

    with (
        patch.object(bot_sender, "tooter") as mock_tooter,
        patch.object(bot_sender, "record_telegram_failure") as failure,
        patch.object(bot_sender, "record_telegram_success") as success,
        patch.object(bot_sender, "users_collection", MagicMock()),
    ):
        mock_tooter.async_relay = AsyncMock(return_value=424)

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
