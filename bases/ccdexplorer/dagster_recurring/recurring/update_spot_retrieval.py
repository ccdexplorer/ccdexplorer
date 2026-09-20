import datetime as dt
import time
from collections.abc import Sequence
from datetime import timezone

import dateutil
import httpx2 as httpx
from ccdexplorer.env import COIN_API_KEY
from ccdexplorer.mongodb import (
    CollectionsUtilities,
    MongoDB,
)
from pymongo import ReplaceOne

# Space the per-token calls out. Both providers rate-limit, and this delay is
# what keeps a cycle under their thresholds -- it is deliberate protection
# against 429s, not leftover overhead.
#
# It used to sit at the end of every token's own run, which is why collapsing
# those runs into one looked like it was removing four minutes of pure waiting.
# It was not: the waiting is the point. The delay now paces the tokens inside
# the single run instead. Roughly 24 tokens is roughly four minutes, which fits
# inside the ten-minute schedule, and it no longer comes on top of 24 process
# starts.
PACING_SECONDS = 10


def coinapi(token: str, client: httpx.Client) -> tuple[int, dict | None]:
    url = f"https://rest.coinapi.io/v1/exchangerate/{token}/USD/apikey-{COIN_API_KEY}/"
    response = client.get(url)

    if response.status_code == 200:
        result = response.json()
        return response.status_code, {
            "_id": f"USD/{token}",
            "token": token,
            "timestamp": dateutil.parser.parse(result["time"]),
            "rate": result["rate"],
            "source": "CoinAPI",
        }

    return response.status_code, None


def coingecko(token: str, token_translation: dict, client: httpx.Client) -> tuple[int, dict | None]:
    token_to_request = token_translation.get(token)
    if not token_to_request:
        # No CoinGecko id configured for this token, so there is nothing to ask for.
        return -2, None

    url = f"https://api.coingecko.com/api/v3/simple/price?ids={token_to_request}&vs_currencies=usd&include_last_updated_at=true"
    response = client.get(url)

    if response.status_code != 200:
        return response.status_code, None

    result = response.json()[token_to_request]
    return response.status_code, {
        "_id": f"USD/{token}",
        "token": token,
        # CoinGecko's own timestamp for the price, not the time we fetched it:
        # a thinly traded token keeps an old timestamp here while still being
        # refreshed every cycle.
        "timestamp": (
            dt.datetime.fromtimestamp(result["last_updated_at"], tz=timezone.utc)
            if "last_updated_at" in result
            else dt.datetime.now(tz=timezone.utc)
        ),
        "rate": result["usd"],
        "source": "CoinGecko",
    }


def get_token_translations_from_mongo(mongodb: MongoDB):
    result = list(
        mongodb.utilities[CollectionsUtilities.token_api_translations].find(
            {"service": "coingecko"}
        )
    )
    return {x["token"]: x["translation"] for x in list(result)}


def fetch_rate(
    context, token: str, coingecko_token_translation: dict, client: httpx.Client
) -> dict | None:
    """One token's spot rate: CoinAPI first, CoinGecko as the fallback.

    Returns None when neither source yields a rate, which is the caller's signal
    that this token produced no write. A failure of one source falls through to
    the other rather than stranding the token, so an expired CoinAPI key alone
    cannot stop prices being stored.
    """
    status_coinapi: int | str = "not called"
    status_coingecko: int | str = "not called"

    try:
        status_coinapi, result = coinapi(token, client)
        if result:
            context.log.info(f"CoinAPI result: {result['rate']} {result['_id']}")
            return result
    except Exception as e:
        status_coinapi = f"error: {e}"
        context.log.error(f"Recurring: Error in CoinAPI call for {token}. Error: {e}")

    try:
        status_coingecko, result = coingecko(token, coingecko_token_translation, client)
        if result:
            context.log.info(f"Coingecko result: {result['rate']} {result['_id']}")
            return result
    except Exception as e:
        status_coingecko = f"error: {e}"
        context.log.error(f"Recurring: Error in Coingecko call for {token}. Error: {e}")

    context.log.error(
        f"Recurring: No spot rate for {token} "
        f"(CoinAPI: {status_coinapi}, CoinGecko: {status_coingecko})."
    )
    return None


def perform_spot_retrieval_update(
    context, tokens: Sequence[str], mongodb: MongoDB
) -> tuple[list[str], list[str]]:
    """Fetch the spot rate for every token and store them all in one write.

    Takes the whole set of tokens rather than one, because this now runs once
    per cycle instead of once per token: the translation table is read once, a
    single HTTP client is reused across the calls, and the rates land in one
    bulk_write.

    Returns (written, failed) -- the tokens whose rate was stored, and those
    that yielded nothing from either source.
    """
    coingecko_token_translation = get_token_translations_from_mongo(mongodb)

    queue: list[ReplaceOne] = []
    written: list[str] = []
    failed: list[str] = []

    with httpx.Client() as client:
        for index, token in enumerate(tokens):
            if index:
                time.sleep(PACING_SECONDS)

            result = fetch_rate(context, token, coingecko_token_translation, client)
            if result is None:
                failed.append(token)
                continue

            queue.append(ReplaceOne({"_id": f"USD/{token}"}, result, upsert=True))
            written.append(token)

    if queue:
        _ = mongodb.utilities[CollectionsUtilities.exchange_rates].bulk_write(queue)

    return written, failed
