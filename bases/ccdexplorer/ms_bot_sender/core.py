#!/usr/bin/env python3
from __future__ import annotations


import warnings
from typing import Any, Dict
import asyncio
from ccdexplorer.ccdexplorer_bot.notification_classes import MessageResponse
from ccdexplorer.ccdexplorer_bot.telegram_delivery import (
    TELEGRAM_BLOCKED_STATUS,
    record_telegram_failure,
    record_telegram_success,
    users_collection_name,
)
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.mongodb import MongoDB
from ccdexplorer.tooter import Tooter
from ccdexplorer.tooter.core import TooterChannel, TooterType
from celery import shared_task
from celery.exceptions import CPendingDeprecationWarning
import grpc
from ccdexplorer.env import RUN_LOCAL_STR

warnings.simplefilter("ignore", CPendingDeprecationWarning)


# Instantiate once per worker process
tooter = Tooter()

_mongodb: MongoDB | None = None


def users_collection():
    """The users collection, connected on first use rather than at import.

    MongoDB's constructor reaches the network -- it logs the server version --
    and sends a notification if it cannot. Building it at import time would
    make importing this module depend on a reachable database and tooter,
    which the tests do not have and should not need.
    """
    global _mongodb
    if _mongodb is None:
        _mongodb = MongoDB(tooter, caller_name="bot_sender")
    return _mongodb.utilities_db[users_collection_name()]


def record_delivery(status: int, telegram_chat_id: int) -> None:
    """Remember what Telegram said about this chat.

    This worker is the only thing in the estate that learns a recipient has
    blocked the bot, so it is the only thing that can stop the next message
    being built for them. It is recorded here and not inside `async_relay`
    because the tooter component has no database and no notion of a user, and
    because `async_relay` also serves the NOTIFIER channel -- the admin chat,
    which must never be switched off by this.

    Bookkeeping must never cost a delivery: the message has already been sent
    or already been refused by the time we get here, so a failed write only
    leaves the count where it was until the next notification.
    """
    try:
        if status == TELEGRAM_BLOCKED_STATUS:
            record_telegram_failure(users_collection(), telegram_chat_id)
        elif status == 200:
            record_telegram_success(users_collection(), telegram_chat_id)
    except Exception as e:
        print(f"Could not record telegram delivery status {status} for {telegram_chat_id}: {e}")


async def send_to_telegram(
    self,
    telegram_chat_id: int,
    message_response: MessageResponse,
):
    status = await tooter.async_relay(
        channel=TooterChannel.BOT,
        title=message_response.title_telegram,
        chat_id=telegram_chat_id,
        body=message_response.message_telegram,
        notifier_type=TooterType.INFO,
    )
    record_delivery(status, telegram_chat_id)


async def send_to_email(
    self,
    email_address: str,
    message_response: MessageResponse,
):
    tooter.email(
        title=message_response.title_email,
        body=message_response.message_email,
        email_address=email_address,
    )


@shared_task(
    name="bot_message",
    bind=True,
    track_started=True,
    autoretry_for=(grpc.RpcError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=5,
    ignore_result=True,
)
def send_service_message(self, payload: Dict[str, Any]) -> dict | None:
    """
    Celery consumer for 'message'. Producer publishes task name 'bot_message'.
    """
    try:
        if payload.get("service") == "telegram":
            asyncio.run(
                send_to_telegram(
                    self,
                    telegram_chat_id=payload["telegram_chat_id"],
                    message_response=MessageResponse(**payload["message_response"]),
                )
            )
        elif payload.get("service") == "email":
            asyncio.run(
                send_to_email(
                    self,
                    email_address=payload["email_address"],
                    message_response=MessageResponse(**payload["message_response"]),
                )
            )

        return None

    except Exception as _:
        # Re-raise so Celery marks FAILURE (and triggers autoretry if configured)
        raise


def _start_worker() -> None:
    argv = [
        "worker",
        "--pool=solo",
        "-Q",
        "bot_sender",
        "-n",
        f"{RUN_LOCAL_STR}:bot_sender",
    ]
    celery_app.worker_main(argv)
