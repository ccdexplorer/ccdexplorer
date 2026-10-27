"""One instance of a service at a time, enforced by the database.

Most `ms_*` services are Celery workers consuming a named queue, so a second
copy shares the work rather than duplicating it -- that is how you scale them.
Two services are not like that, and this component exists for them:

- `heartbeat` keeps its position in a single Mongo document: it reads the last
  processed height, requests the next run of heights, processes them and writes
  the new height back. Two copies read the same height, fetch the same blocks
  and race on the write, so the marker ends up wherever the slower one left it.
- `ms_block_analyser` tails a Mongo change stream and publishes one Celery task
  per processor per block. Two copies publish every block twice, which makes
  every worker downstream do its work twice, and they also share a single
  resume-token key in Redis.

Until now the guarantee was "only one copy is deployed", which holds right up
to the moment a host is failed over and the original comes back with
`restart: always`. That is precisely when nobody is watching.

The implementation is one document per service in the per-net `helpers`
collection, claimed with a conditional upsert. Mongo does the mutual exclusion:
`_id` is unique, so an upsert whose filter does not match a live lease fails
with DuplicateKeyError, and that failure *is* the answer "someone else holds
it". No Redis, no etcd, no extra moving part to run.

Two deliberate choices:

`expires_at_epoch` is a float, not a BSON date. Filters compare against it, and
a float compares identically in Mongo, in mongomock and in Python, with no
timezone or millisecond-truncation edge cases. `expires_at` is written beside it
as a real datetime purely so the document is readable in a shell.

The lease is time-bound rather than a flag someone clears. A container that is
SIGKILLed never runs its release path, and a lock needing manual clearing after
a crash is worse than no lock: it turns an outage into an outage plus a puzzle.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, PrivateAttr
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

DOCUMENT_PREFIX = "single_instance_lock"


def default_holder_id() -> str:
    """Who we are, specifically enough that a restart is a different holder.

    The uuid matters: without it a restarted container on the same host with a
    recycled pid would match its own dead lease and skip the expiry check.
    """
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class SingleInstanceLock(BaseModel):
    """A renewable lease on the right to be the only copy of a service.

    `collection` is typed loosely on purpose -- it takes any pymongo-style
    collection, and the tests pass a mongomock one, which is not a subclass of
    pymongo's Collection.
    """

    collection: Any
    service: str
    lease_seconds: float = 60.0
    holder: str = Field(default_factory=default_holder_id)
    now_fn: Callable[[], float] = time.time

    model_config = {"arbitrary_types_allowed": True}

    _renewer: threading.Thread | None = PrivateAttr(default=None)
    _stop: threading.Event = PrivateAttr(default_factory=threading.Event)

    @property
    def document_id(self) -> str:
        return f"{DOCUMENT_PREFIX}:{self.service}"

    def acquire(self) -> bool:
        """Claim the lease, or report that someone else holds it.

        Succeeds when the lock is free, when its lease has expired, or when we
        already hold it. Never blocks and never waits.
        """
        now = self.now_fn()
        expires = now + self.lease_seconds
        try:
            self.collection.find_one_and_update(
                {
                    "_id": self.document_id,
                    "$or": [
                        {"expires_at_epoch": {"$lte": now}},
                        {"holder": self.holder},
                    ],
                },
                {
                    "$set": {
                        "holder": self.holder,
                        "service": self.service,
                        "expires_at_epoch": expires,
                        "expires_at": datetime.fromtimestamp(expires, UTC),
                        "acquired_at": datetime.fromtimestamp(now, UTC),
                    }
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # The filter missed and the insert collided on _id: a live lease
            # held by somebody else. This is the expected losing path, not an
            # error to report.
            return False
        return True

    def renew(self) -> bool:
        """Extend our own lease. False means we no longer hold it."""
        now = self.now_fn()
        expires = now + self.lease_seconds
        result = self.collection.update_one(
            {"_id": self.document_id, "holder": self.holder},
            {
                "$set": {
                    "expires_at_epoch": expires,
                    "expires_at": datetime.fromtimestamp(expires, UTC),
                }
            },
        )
        return result.matched_count == 1

    def release(self) -> None:
        """Give up the lease, if it is ours. Safe to call when it is not."""
        self.collection.delete_one({"_id": self.document_id, "holder": self.holder})

    def start_renewing(self, on_lost: Callable[[], None] | None = None) -> None:
        """Keep the lease alive in the background until told to stop.

        Renews at a third of the lease, so two renewals can be missed -- to a
        slow database or a stalled event loop -- before the lease lapses. If it
        does lapse, `on_lost` is called; deciding what to do about it belongs to
        the service, which usually means exiting so the restart policy can try
        again later.
        """
        if self._renewer is not None:
            return

        interval = max(self.lease_seconds / 3.0, 0.01)

        def loop() -> None:
            while not self._stop.wait(interval):
                if not self.renew():
                    if on_lost is not None:
                        on_lost()
                    return

        self._stop.clear()
        self._renewer = threading.Thread(
            target=loop, name=f"{DOCUMENT_PREFIX}:{self.service}", daemon=True
        )
        self._renewer.start()

    def stop_renewing(self) -> None:
        self._stop.set()
        if self._renewer is not None:
            self._renewer.join(timeout=5)
            self._renewer = None

    @contextmanager
    def hold(self, on_lost: Callable[[], None] | None = None) -> Iterator[bool]:
        """Hold the lease for the duration of the block.

        Yields whether we got it, rather than raising, because standing down
        quietly is a normal outcome: the other copy is doing the work.
        """
        acquired = self.acquire()
        if acquired:
            self.start_renewing(on_lost=on_lost)
        try:
            yield acquired
        finally:
            if acquired:
                self.stop_renewing()
                self.release()
