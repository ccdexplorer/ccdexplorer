# Bot Sender (Project: `ms_bot_sender`)

`ms_bot_sender` is a lightweight Celery worker dedicated to delivering outbound notifications for the Explorer bot and web alerts.
It decouples the high-volume notification producers (site, API, cron jobs) from the actual Telegram/email transport layer.

## Responsibilities
- Consume `bot_message` tasks from the `bot_sender` queue (defined in `ccdexplorer.celery_app`).
- Deserialize `MessageResponse` payloads and forward them via:
  - `TooterChannel.BOT` (Telegram) for chat-based alerts.
  - Fastmail SMTP (`Tooter.email`) for email notifications.
- Retry transient failures using Celery’s autoretry logic (configured backoff/jitter with gRPC/connection exceptions).
- Surface failures by raising the exception so Celery marks the task as `FAILURE` and records metadata via `store_result_in_mongo`.

## Message shape
```json
{
  "service": "telegram" | "email",
  "telegram_chat_id": 123456789,
  "email_address": "user@example.com",
  "message_response": {
    "title_telegram": "...",
    "message_telegram": "...",
    "title_email": "...",
    "message_email": "..."
  }
}
```

The upstream producers (Explorer bot, notification scheduler, API) are responsible for populating the `MessageResponse`.

## Deployment
- The worker entry point runs `celery_app.worker_main` with `-Q bot_sender --pool=solo`, making it safe to run alongside other services without competing for resources.
- Environment variables such as `RUN_LOCAL_STR` and the Fastmail API token are pulled from `components/env`.
- The Dockerfile in `projects/ms_bot_sender/` installs the base `ccdexplorer.ms_bot_sender` package so this worker can be deployed independently.

## Related services
- [`ccdexplorer_bot`](bot.md) produces notification intents.
- [`ms_events_and_impacts`](every_block/events_and_impacted.md) and other data services enqueue messages targeted at the bot sender when they detect an event worthy of alerting.***
