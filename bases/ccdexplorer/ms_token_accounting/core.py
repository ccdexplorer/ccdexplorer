from __future__ import annotations

import asyncio
import traceback
import warnings
from typing import Any

from ccdexplorer.celery_app import TaskResult, store_result_in_mongo
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.env.settings import RUN_LOCAL_STR
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter
from celery import shared_task
from .heartbeat import Heartbeat
from celery.exceptions import CPendingDeprecationWarning
from celery.utils.log import get_task_logger

from ccdexplorer.env import RUN_ON_NET
import grpc

warnings.simplefilter("ignore", CPendingDeprecationWarning)

logger = get_task_logger(__name__)
grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True, caller_name="ms_token_accounting")
mongodb = MongoDB(tooter, caller_name="ms_token_accounting")
heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)  # type: ignore
#################################################
processor_for_consumer = "token_accounting"
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
    try:
        block_height = payload.get("height")
        block_hash = payload.get("block_hash")
        assert block_height is not None

        # The payload hash must be the hash of `block_height`, or balanceOf is
        # evaluated against the wrong / no block. Resolve it authoritatively
        # from the blocks collection (the block is already stored by the time
        # this task runs, since the producer watches that collection).
        db = mongodb.mainnet if RUN_ON_NET == "mainnet" else mongodb.testnet
        block_doc = db[Collections.blocks].find_one({"height": block_height}, {"hash": 1})
        if block_doc is None:
            # block not yet visible to this worker → let autoretry handle it
            raise ConnectionError(f"block at height {block_height} not yet in db (transient)")
        if block_hash != block_doc["hash"]:
            logger.warning(
                "payload block_hash %r != height %s hash %s; using db hash",
                block_hash,
                block_height,
                block_doc["hash"],
            )
            block_hash = block_doc["hash"]
        asyncio.run(
            heartbeat.update_token_accounting_v2(RUN_ON_NET, block_height, block_hash)  # type: ignore
        )
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

    except grpc.RpcError as e:
        if isinstance(e, grpc.Call) and e.code() == grpc.StatusCode.NOT_FOUND:
            raise ConnectionError("block not found (transient)") from None
        tb = traceback.format_exc()
        logger.error("%s task failed at height %s: %s", processor_for_consumer, block_height, e)
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
        raise
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("%s task failed at height %s: %s", processor_for_consumer, block_height, e)
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
