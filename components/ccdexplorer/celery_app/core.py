from __future__ import annotations

import datetime as dt
import os
import re
from typing import Literal, Optional

import sentry_sdk
from ccdexplorer.env import REDIS_URL, RUN_ON_NET, sentry_dsn
from ccdexplorer.mongodb import Collections, MongoDB
from celery import Celery
from pydantic import BaseModel, ConfigDict, Field


class TaskResult(BaseModel):
    """
    Schema for task result documents stored in MongoDB.
    """

    model_config = ConfigDict(arbitrary_types_allowed=False)

    id: str = Field(..., alias="_id", description="Celery task_id (UUID).")
    queue: str = Field(..., description="Processor/queue name, e.g. 'plt'.")
    block_height: Optional[int] = None
    token_address: Optional[str] = None
    net: str = Field(
        ..., pattern="^(mainnet|testnet|devnet)$", description="Network this task ran on."
    )
    status: Literal["STARTED", "SUCCESS", "FAILURE"] = Field(
        ..., description="Task execution status."
    )
    date_done: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    slot_time: Optional[dt.datetime] = None

    error: Optional[str] = Field(None, description="Error message if status=FAILURE.")
    traceback: Optional[str] = Field(None, description="Traceback if status=FAILURE.")

    # Convenience export method
    def to_mongo(self) -> dict:
        """
        Export as a Mongo-ready dict, using `_id` instead of `id`.
        """
        doc = self.model_dump(by_alias=True, exclude_none=True)
        return doc


#: A Dagster run id, which is what makes every failure look new.
_RUN_ID = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.IGNORECASE
)

#: The numeric ids Dagster puts beside the run id -- a storage row, not
#: anything a reader can act on, and different on every run.
_BARE_NUMBER = re.compile(r"^\d+$")


def dagster_fingerprint(message: str | None) -> list[str] | None:
    """A grouping key for a Dagster event log line, or None for anything else.

    init() below takes no `integrations`, so the default LoggingIntegration
    turns every ERROR log record into an event, and Sentry groups those by
    message. Dagster's failure messages carry the run id:

        j_spot_retrieval - 8e3cd456-...-f8a8078b222d - 1217035 -
        RUN_FAILURE - Execution of run for "j_spot_retrieval" failed.

    so each run opened its own issue. On 2026-10-11 the celery project held
    163 unresolved issues untouched for a fortnight, nearly all of them one
    run id each: receipts for runs that cannot recur, burying the few real
    faults among them.

    What identifies the fault is everything except the ids -- the job, the
    step, the event type -- so those are what is kept. Dropping the run id
    from the fingerprint does not hide it: it stays in the message and the
    tags, where it belongs, and the issue now counts occurrences instead of
    multiplying.
    """
    if not message:
        return None
    if not _RUN_ID.search(message):
        # Not a Dagster event. Sentry's own grouping is the right one.
        return None
    kept = [
        part
        for part in (p.strip() for p in message.split(" - "))
        if part and not _RUN_ID.fullmatch(part) and not _BARE_NUMBER.fullmatch(part)
    ]
    # Job and step, or job and event type -- and nothing after. The prose
    # that follows names the token, the host, the status code, and keeping
    # it was worth almost nothing: measured over the 68 Dagster lines open
    # on 2026-10-11, keeping it gave 55 groups and dropping it gives 13. One
    # of those holds 44 issues that were all "spot_retrieval could not price
    # a token", which is one problem with a list of examples, not 44
    # problems. The detail is still in every event.
    return ["dagster", *kept[:2]]


def _before_send(event, hint):
    """Regroup Dagster's per-run log events before they leave."""
    message = (event.get("logentry") or {}).get("formatted") or event.get("message")
    fingerprint = dagster_fingerprint(message)
    if fingerprint:
        event["fingerprint"] = fingerprint
    return event


sentry_sdk.init(
    dsn=sentry_dsn(
        "https://514fe6c4c0481f29c21e71f1b7ad2755@o4503924901347328.ingest.us.sentry.io/4510817932935168"
    ),
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    before_send=_before_send,
)

RESULT_MONGO_DB = {
    "mainnet": "concordium_mainnet",
    "testnet": "concordium_testnet",
    "devnet": "concordium_devnet",
}[RUN_ON_NET]

app = Celery("ccd")
app.conf.update(
    broker_url=REDIS_URL,
    task_serializer="json",
    accept_content=["json"],
    worker_redirect_stdouts=False,
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_acks_on_failure_or_timeout=False,
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": int(os.getenv("VISIBILITY_TIMEOUT", "3600")),
    },
    worker_concurrency=1,
    worker_prefetch_multiplier=1,
    worker_pool="solo",
)


def store_result_in_mongo(mongodb: MongoDB, task: TaskResult) -> None:
    native = {
        "_id": task.id,
        "queue": task.queue,
        "block_height": task.block_height,
        "status": task.status,
        "date_done": dt.datetime.now(dt.timezone.utc),
    }
    if task.slot_time:
        native["slot_time"] = task.slot_time
        print(f"Storing slot_time {task.slot_time} for block_height {task.block_height}")
    if task.error:
        native["error"] = task.error
    if task.traceback:
        native["traceback"] = task.traceback

    db_to_use = {
        "mainnet": mongodb.mainnet,
        "testnet": mongodb.testnet,
        "devnet": mongodb.devnet,
    }[task.net]
    db_to_use[Collections.celery_taskmeta].update_one(
        {"_id": task.id}, {"$set": native}, upsert=True
    )
