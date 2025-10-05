import datetime as dt
from typing import Any

from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.domain.mongo import MongoTypeTokenAddress
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo.collection import Collection
from redis import Redis


def publish_to_celery(payload: dict[str, Any]) -> None:
    """
    Publish one task per processor to queue: {RUN_ON_NET}:queue:{processor}.
    Uses send_task so producer has zero dependency on worker code.
    Synchronous version.
    """
    task_name = "process_block"
    qname = "mainnet:queue:metadata"

    # Fire-and-forget; ignore the returned AsyncResult
    celery_app.send_task(
        task_name,
        args=[],
        kwargs={"processor": "metadata", "payload": payload},
        queue=qname,
    )


def send_metadata_to_redis(r: Redis, dom: MongoTypeTokenAddress):
    token_address = f"{dom.contract}-{dom.token_id}"
    publish_to_celery({"token_address": token_address})


def update_metadata(context, mongodb: MongoDB, redis: Redis, net: str):
    dct = {}

    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    """
    This method looks into the token_addresses_v2 collection and tries to read the
    metadata_url to fetch the metdata for a token.

    As such, every time this runs, it retrieves all tokenIDs from this contract
    and loops through all tokenIDs that do not have metadata set (mostly new, could
    also be that in a previous run, there was a http issue).
    For every tokenID withou metadata, there is a call to the wallet-proxy to get
    the metadataURL, which is then stored in the collection. Finally, we read the
    metadataURL to determine the actual domainname and store this is a separate
    collection.
    """
    context.log.info(f"Finding tokens without metadata in {net}")
    # these are new tokens, as no failed attempt yet
    pipeline = [
        {"$match": {"token_metadata": {"$exists": False}}},
        {"$match": {"failed_attempt": {"$exists": False}}},
    ]

    new_tokens_result: list = (
        db[Collections.tokens_token_addresses_v2].aggregate(pipeline).to_list(1000)
    )

    if len(new_tokens_result) > 0:
        current_content = [MongoTypeTokenAddress(**x) for x in new_tokens_result]
    else:
        current_content = []

    counter_new = 0
    counter_new_error = 0
    for dom in current_content:
        counter_new += 1
        send_metadata_to_redis(redis, dom)

    # these are existing tokens, with a failed attempt
    # only retrieve if we can try again
    now = dt.datetime.now().astimezone(tz=dt.timezone.utc)

    pipeline = [
        {"$match": {"failed_attempt": {"$exists": True}}},
        {"$match": {"failed_attempt.do_not_try_before": {"$lte": now}}},
    ]

    existing_tokens_result: list = list(
        db[Collections.tokens_token_addresses_v2].aggregate(pipeline).to_list(1000)
    )
    if len(existing_tokens_result) > 0:
        current_content = [MongoTypeTokenAddress(**x) for x in existing_tokens_result]
    else:
        current_content = []

    counter_existing = 0
    counter_existing_error = 0
    for dom in current_content:
        counter_existing += 1
        send_metadata_to_redis(redis, dom)

    context.log.info(
        f"Sent metadata request for {(counter_new - counter_new_error):,.0f} / {(counter_new):,.0f} new tokens and  {(counter_existing - counter_existing_error):,.0f} / {(counter_existing):,.0f} existing tokens"
    )
    return dct
