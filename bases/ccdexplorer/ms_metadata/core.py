from __future__ import annotations
import os
import random
import grpc
import asyncio
import traceback
import warnings
from typing import Any
import httpx
from ccdexplorer.celery_app import TaskResult, store_result_in_mongo
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter
from celery import shared_task
from celery.exceptions import CPendingDeprecationWarning
from celery.utils.log import get_task_logger

from ccdexplorer.env import RUN_ON_NET, RUN_LOCAL_STR
from .subscriber import Subscriber

warnings.simplefilter("ignore", CPendingDeprecationWarning)

logger = get_task_logger(__name__)
grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter)
subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
#################################################
processor_for_consumer = "metadata"
#################################################

httpx_client = httpx.Client()


class GrpcLimiter:
    def __init__(self, max_concurrent: int = 5):
        self._sem = asyncio.Semaphore(max_concurrent)
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def singleflight_lock(self, key: str) -> asyncio.Lock:
        # ensure only one fetch per token_address at a time
        async with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def call_with_limits(self, key: str, coro_factory, *, max_retries=5):
        # global concurrency
        async with self._sem:
            # single-flight for same key
            lock = await self.singleflight_lock(key)
            async with lock:
                # retry with jitter on pressure
                delay = 0.2
                for attempt in range(max_retries):
                    try:
                        return await coro_factory()
                    except grpc.aio.AioRpcError as e:
                        code = e.code()
                        # backoff only on server pressure/transient issues
                        if code in (
                            grpc.StatusCode.RESOURCE_EXHAUSTED,
                            grpc.StatusCode.UNAVAILABLE,
                            grpc.StatusCode.DEADLINE_EXCEEDED,
                        ):
                            await asyncio.sleep(delay + random.random() * 0.2)
                            delay = min(delay * 2, 5.0)
                            continue
                        raise


# create one limiter (module/global)
grpc_limiter = GrpcLimiter(max_concurrent=int(os.getenv("GRPC_MAX_CONCURRENT", "5")))


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
        token_address = payload.get("token_address")
        if not token_address:
            raise ValueError("missing 'token_address'")

        # defer the actual RPC into the limiter
        # async def _do():
        #     return await subscriber.fetch_token_metadata(NET(RUN_ON_NET), token_address)

        # asyncio.run(grpc_limiter.call_with_limits(token_address, _do))

        subscriber.fetch_token_metadata(NET(RUN_ON_NET), token_address, httpx_client)

        # success
        task_doc = TaskResult(
            _id=self.request.id,
            queue=processor,
            block_height=None,  # type: ignore
            token_address=token_address,  # type: ignore
            net=RUN_ON_NET,  # type: ignore
            status="SUCCESS",
            error=None,
            traceback=None,
        )
        store_result_in_mongo(mongodb, task_doc)
        return None

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(
            f"{processor_for_consumer} task failed for token %s: %s",
            token_address,
            e,  # type: ignore
        )
        task_doc = TaskResult(
            _id=self.request.id,
            queue=processor,
            block_height=None,  # type: ignore
            token_address=token_address,  # type: ignore
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
