"""Keep the dashboard_nodes collection warm.

A live read is far too slow for a page -- it is an HTTP fetch of every
reporting node plus a bulk upsert -- so the collection is a cache. Until
2026-10-28 a Dagster asset refreshed it on `*/1 * * * *` per net: 4,320 runs
a day, each paying a process launch and a code-location load to make one
request.

It lives in the api because the api is the only long-lived process that both
has Mongo and serves this collection. The site cannot hold it: the bot and
heartbeat read dashboard_nodes directly, so the refresh must not depend on a
site process being up.

The api runs two instances, so both would refresh. Rather than a lock, each
pass skips a net whose `heartbeat_last_timestamp_dashboard_nodes` is still
fresh -- the stamp the refresh itself writes. Cheap, and it doubles as the
staleness check.
"""

import asyncio
import datetime as dt
import logging

from ccdexplorer.mongodb import Collections, net_db
from ccdexplorer.node_dashboard import DASHBOARDS, refresh_dashboard_nodes

STAMP = "heartbeat_last_timestamp_dashboard_nodes"
STALE_AFTER_SECONDS = 45
REFRESH_SECONDS = 60

log = logging.getLogger(__name__)


def _age_seconds(mongodb, net: str, now: dt.datetime) -> float | None:
    """Seconds since the last refresh of `net`, or None if never/unreadable."""
    doc = net_db(mongodb, net)[Collections.helpers].find_one({"_id": STAMP})
    stamp = (doc or {}).get("timestamp")
    if not isinstance(stamp, dt.datetime):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    return (now - stamp).total_seconds()


def refresh_if_stale(
    mongodb,
    net: str,
    now: dt.datetime | None = None,
    stale_after: float = STALE_AFTER_SECONDS,
    logger: logging.Logger = log,
) -> bool:
    """Refresh `net` unless another instance just did. True if it refreshed."""
    now = now or dt.datetime.now(dt.timezone.utc)
    age = _age_seconds(mongodb, net, now)
    if age is not None and age < stale_after:
        return False
    refresh_dashboard_nodes(logger, mongodb, net)
    return True


async def run_refresher(
    mongodb,
    nets=tuple(DASHBOARDS),
    interval: float = REFRESH_SECONDS,
    iterations: int | None = None,
    logger: logging.Logger = log,
) -> None:
    """Loop until cancelled. `iterations` bounds it for tests.

    The fetch and the Mongo writes are synchronous, so they go to a thread --
    blocking the event loop here would stall every api request.
    """
    n = 0
    while iterations is None or n < iterations:
        for net in nets:
            try:
                await asyncio.to_thread(refresh_if_stale, mongodb, net, None,
                                        STALE_AFTER_SECONDS, logger)
            except Exception as e:
                # The dashboard is someone else's service; a bad minute must
                # not end the loop.
                logger.error("%s | dashboard refresh failed: %s", net, e)
        n += 1
        if iterations is None or n < iterations:
            await asyncio.sleep(interval)
