#!/usr/bin/env python3
from __future__ import annotations


import warnings
from typing import Any, Dict
import asyncio
from ccdexplorer.ccdexplorer_bot.notification_classes import MessageResponse
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.tooter import Tooter
from ccdexplorer.tooter.core import TooterChannel, TooterType
from celery import shared_task
from celery.exceptions import CPendingDeprecationWarning
import grpc
from ccdexplorer.env import RUN_LOCAL_STR

warnings.simplefilter("ignore", CPendingDeprecationWarning)


# Instantiate once per worker process
tooter = Tooter()


async def send_to_telegram(
    self,
    telegram_chat_id: int,
    message_response: MessageResponse,
):
    await tooter.async_relay(
        channel=TooterChannel.BOT,
        title=message_response.title_telegram,
        chat_id=telegram_chat_id,
        body=message_response.message_telegram,
        notifier_type=TooterType.INFO,
    )


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
