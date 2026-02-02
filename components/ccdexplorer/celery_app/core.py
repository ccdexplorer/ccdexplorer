from __future__ import annotations

import datetime as dt
import os
from typing import Literal, Optional

import sentry_sdk
from ccdexplorer.env import REDIS_URL, RUN_ON_NET
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
    net: str = Field(..., pattern="^(mainnet|testnet)$", description="Network this task ran on.")
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


sentry_sdk.init(
    dsn="https://514fe6c4c0481f29c21e71f1b7ad2755@o4503924901347328.ingest.us.sentry.io/4510817932935168",
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
)

RESULT_MONGO_DB = "concordium_mainnet" if RUN_ON_NET == "mainnet" else "concordium_testnet"

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

    db_to_use = mongodb.mainnet if task.net == "mainnet" else mongodb.testnet
    db_to_use[Collections.celery_taskmeta].update_one(
        {"_id": task.id}, {"$set": native}, upsert=True
    )
