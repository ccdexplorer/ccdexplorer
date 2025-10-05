#!/usr/bin/env python3
from __future__ import annotations


import traceback
import warnings
from typing import Any, Dict

from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.celery_app import store_result_in_mongo, TaskResult
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter
from celery import shared_task
from celery.exceptions import CPendingDeprecationWarning
import grpc
from ccdexplorer.env import RUN_ON_NET, RUN_LOCAL_STR
from .update_plts_from_txs import update_plts

warnings.simplefilter("ignore", CPendingDeprecationWarning)


# -------------------------------------------------
# This consumer handles processor "plt"
# -------------------------------------------------
processor = "plt"

# Instantiate once per worker process
grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter)


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
def process_block(self, processor: str, payload: Dict[str, Any]) -> dict | None:
    """
    Celery consumer for 'plt'. Producer publishes task name 'process_block'
    with kwargs {processor, payload}. We ignore messages for other processors.
    """
    self.ignore_result = True
    if processor != "plt":
        self.ignore_result = True
        return None

    print(f"Handling payload: {payload}")
    block_height = payload.get("height")
    try:
        # your work
        update_plts(mongodb, grpcclient, RUN_ON_NET, block_height)  # type: ignore

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
        print(f"plt task failed at height {block_height}: {e}")
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
        f"{RUN_ON_NET}:queue:{processor}",
        "-n",
        f"{RUN_ON_NET}:{RUN_LOCAL_STR}:{processor}",
    ]
    celery_app.worker_main(argv)
