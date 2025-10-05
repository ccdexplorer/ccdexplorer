from __future__ import annotations

import asyncio
import traceback
import warnings
from typing import Any

from ccdexplorer.celery_app import TaskResult, store_result_in_mongo
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.domain.generic import NET
from ccdexplorer.env.settings import RUN_LOCAL_STR
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter
from celery import shared_task
from celery.exceptions import CPendingDeprecationWarning
from celery.utils.log import get_task_logger

from ccdexplorer.env import RUN_ON_NET
from .subscriber import Subscriber
import grpc

warnings.simplefilter("ignore", CPendingDeprecationWarning)

logger = get_task_logger(__name__)
grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter)
subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
#################################################
processor_for_consumer = "account_creation"
#################################################


@shared_task(
    name="process_block",
    bind=True,
    track_started=True,
    autoretry_for=(grpc.RpcError, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=5,
    ignore_result=True,
)
def process_block(self, processor: str, payload: dict[str, Any]) -> dict | None:
    self.ignore_result = True
    if processor != processor_for_consumer:
        self.ignore_result = True
        return None

    logger.info(f"Handling payload: {payload}")
    block_height = payload.get("height")
    try:
        height = payload.get("height")
        assert height is not None
        asyncio.run(subscriber.process_new_address(NET(RUN_ON_NET), height))
        # success
        task_doc = TaskResult(
            _id=self.request.id,
            queue=processor,
            block_height=block_height,  # type: ignore
            net=RUN_ON_NET,  # type: ignore
            status="SUCCESS",
            error=None,
            traceback=None,
        )
        store_result_in_mongo(mongodb, task_doc)
        return None

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("plt task failed at height %s: %s", block_height, e)
        task_doc = TaskResult(
            _id=self.request.id,
            queue=processor,
            block_height=block_height,  # type: ignore
            net=RUN_ON_NET,  # type: ignore
            status="FAILURE",
            error=str(e),
            traceback=tb,
        )
        store_result_in_mongo(mongodb, task_doc)
        # Re-raise so Celery marks FAILURE (and triggers autoretry if configured)
        raise


def _start_worker() -> None:
    argv = [
        "worker",
        "-Q",
        f"{RUN_ON_NET}:queue:{processor_for_consumer}",
        "-n",
        f"{RUN_ON_NET}:{RUN_LOCAL_STR}:{processor_for_consumer}",
    ]
    celery_app.worker_main(argv)
