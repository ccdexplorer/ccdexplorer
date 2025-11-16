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


@pytest.mark.asyncio
async def test_send_message_telegram_path():
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
        # Celery task must be invoked via .run(...)
        await bot_sender.send_message.run("message", payload)  # type: ignore

        mock_tg.assert_awaited_once()
        mock_mail.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_message_email_path():
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
        await bot_sender.send_message.run("message", payload)  # type: ignore

        mock_mail.assert_awaited_once()
        mock_tg.assert_not_awaited()
