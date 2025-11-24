#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from contextlib import asynccontextmanager
import traceback
from typing import Any, Dict, Optional
from uuid import uuid4

from ccdexplorer.celery_app import TaskResult, store_result_in_mongo
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.domain.generic import NET
from ccdexplorer.env import REDIS_URL, RUN_ON_NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoMotor
from ccdexplorer.mongodb.core import MongoDB
from ccdexplorer.tooter import Tooter
from pydantic import BaseModel
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import PyMongoError
from redis.asyncio import Redis

grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter, nearest=True)


class Settings(BaseModel):
    grpcclient: GRPCClient = grpcclient
    motormongo: MongoMotor = motormongo
    mongodb: MongoDB = mongodb
    redis_url: str | None = REDIS_URL

    resume_token_key: str = os.getenv(
        "RESUME_TOKEN_KEY", f"{RUN_ON_NET}:change_stream:resume_token"
    )
    reconnect_backoff_seconds: float = float(os.getenv("RECONNECT_BACKOFF", "5.0"))

    model_config = {"arbitrary_types_allowed": True}


async def publish_to_celery(processor: str, payload: Dict[str, Any]) -> None:
    """
    Publish one task per processor to queue: {RUN_ON_NET}:queue:{processor}.
    Uses send_task so producer has zero dependency on worker code.
    """
    task_name = "process_block"
    qname = f"{RUN_ON_NET}:queue:{processor}"
    await asyncio.to_thread(
        celery_app.send_task,
        task_name,
        args=[],  # prefer kwargs for clarity
        kwargs={"processor": processor, "payload": payload},
        queue=qname,
    )


def extract_processors(block_hash: str) -> list[str]:
    """
    Return the list of processors that should handle this block.
    """
    # always include
    processors = ["events_and_impacted", "indexers"]

    block = grpcclient.get_block_transaction_events(block_hash, NET(RUN_ON_NET))
    transactions = block.transaction_summaries

    account_creation = any([tx.account_creation for tx in transactions])
    token_creation = any([tx.token_creation for tx in transactions])

    contract_initialized = any(
        [
            tx.account_transaction.effects.contract_initialized
            for tx in transactions
            if tx.account_transaction
        ]
    )

    token_update_effect = any(
        [
            tx.account_transaction.effects.token_update_effect
            for tx in transactions
            if tx.account_transaction
        ]
    )

    module_deployed = any(
        [
            tx.account_transaction.effects.module_deployed
            for tx in transactions
            if tx.account_transaction
        ]
    )

    contract_upgraded = any(
        [
            effect.upgraded
            for tx in transactions
            if tx.account_transaction and tx.account_transaction.effects.contract_update_issued
            for effect in tx.account_transaction.effects.contract_update_issued.effects
            if effect.upgraded
        ]
    )

    if account_creation:
        processors.append("account_creation")

    if contract_initialized or contract_upgraded:
        processors.append("contract")

    if module_deployed:
        processors.append("module_deployed")

    if token_creation or token_update_effect:
        processors.append("plt")

    return processors


def stream_name_for(processor: str) -> str:
    return f"blocks:{processor}:{RUN_ON_NET}"


# ----------------------------
# Redis helpers
# ----------------------------


@asynccontextmanager
async def redis_client(url: str):
    r = Redis.from_url(url, decode_responses=False)
    try:
        yield r
    finally:
        await r.aclose()


async def save_resume_token(r: Redis, token: Dict[str, Any], settings: Settings) -> None:
    await r.set(settings.resume_token_key, json.dumps(token).encode())


async def load_resume_token(r: Redis, settings: Settings) -> Optional[Dict[str, Any]]:
    raw = await r.get(settings.resume_token_key)
    if not raw:
        return None
    try:
        return json.loads(raw.decode())
    except Exception:
        return None


class Shutdown:
    def __init__(self) -> None:
        self._e = asyncio.Event()

    def trigger(self, *_):
        self._e.set()

    @property
    def is_set(self):
        return self._e.is_set()

    async def wait(self):
        await self._e.wait()


async def watch_loop(stop: Shutdown):
    settings = Settings()
    db: dict[Collections, AsyncCollection] = (
        settings.motormongo.mainnet if RUN_ON_NET == "mainnet" else settings.motormongo.testnet
    )
    while not stop.is_set:
        try:
            r = Redis.from_url(settings.redis_url, decode_responses=False)  # type: ignore

            resume = await load_resume_token(r, settings)
            kwargs: Dict[str, Any] = {
                "pipeline": [
                    {"$match": {"operationType": {"$in": ["insert", "replace"]}}},
                    {
                        "$project": {
                            "documentKey._id": 1,
                            "fullDocument.height": 1,
                            "fullDocument.transaction_count": 1,
                        }
                    },
                ],
            }
            if resume:
                kwargs["resume_after"] = resume

            # right before async with coll.watch(**kwargs)
            mode = "fresh"
            if "resume_after" in kwargs:
                mode = "resume_after"
            elif "startAtOperationTime" in kwargs:
                mode = "startAtOperationTime"
            print(
                f"[producer] opening change stream mode={mode} on {Collections.blocks.value} on {RUN_ON_NET}",
                file=sys.stderr,
            )

            coll: AsyncCollection = db[Collections.blocks]
            stream = await coll.watch(**kwargs)
            async with stream as change_stream:
                async for change in change_stream:
                    if stop.is_set:
                        break

                    try:
                        if "_id" in change:
                            await save_resume_token(r, change["_id"], settings)

                        block_hash = change.get("documentKey", {}).get("_id")
                        height = change.get("fullDocument", {}).get("height")
                        transaction_count = change.get("fullDocument", {}).get(
                            "transaction_count", 0
                        )
                        payload = {}
                        if transaction_count == 0:
                            print(f"{height:,.0f} - skip")
                            continue

                        slot_time = change.get("fullDocument", {}).get("slot_time")

                        payload["height"] = height
                        payload["block_hash"] = block_hash

                        processors = extract_processors(block_hash)

                        print(f"{payload['height']:,.0f} - {processors}")
                        for proc in processors:
                            await publish_to_celery(proc, payload)
                        task_doc = TaskResult(
                            _id=uuid4().hex,
                            queue="block_analyser",
                            block_height=height,  # type: ignore
                            slot_time=slot_time,
                            net=RUN_ON_NET,  # type: ignore
                            status="SUCCESS",
                            error=None,
                            traceback=None,
                        )
                        store_result_in_mongo(settings.mongodb, task_doc)
                    except Exception as e:
                        tb = traceback.format_exc()
                        print(f"[producer][event-error] {e!r}", file=sys.stderr)
                        task_doc = TaskResult(
                            _id=uuid4().hex,
                            queue="block_analyser",
                            block_height=height,  # type: ignore
                            net=RUN_ON_NET,  # type: ignore
                            status="FAILURE",
                            error=str(e),
                            traceback=tb,
                        )
                        store_result_in_mongo(settings.mongodb, task_doc)

            await r.aclose()
            await motormongo.connection.close()

        except (PyMongoError, OSError) as e:
            print(
                f"[producer][watch-loop] {e!r}; backoff {settings.reconnect_backoff_seconds}s",
                file=sys.stderr,
            )
            await asyncio.sleep(settings.reconnect_backoff_seconds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[producer][unexpected] {e!r}", file=sys.stderr)
            await asyncio.sleep(settings.reconnect_backoff_seconds)


async def main():
    stop = Shutdown()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.trigger, sig, None)
        except NotImplementedError:
            signal.signal(sig, stop.trigger)
    await watch_loop(stop)


if __name__ == "__main__":
    asyncio.run(main())
