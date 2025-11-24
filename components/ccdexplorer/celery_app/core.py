from __future__ import annotations
import os
from celery import Celery
from ccdexplorer.env import REDIS_URL, RUN_ON_NET
import datetime as dt
from ccdexplorer.mongodb import Collections, MongoDB

from typing import Optional, Literal
from pydantic import BaseModel, Field, ConfigDict


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
