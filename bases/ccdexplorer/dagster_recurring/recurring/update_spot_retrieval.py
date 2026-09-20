import datetime as dt
import time
from collections.abc import Sequence
from datetime import timezone

import dateutil
import httpx2 as httpx
from ccdexplorer.env import COIN_API_KEY, COIN_GECKO_API_KEY
from ccdexplorer.mongodb import (
    CollectionsUtilities,
    MongoDB,
)
from pymongo import ReplaceOne

# Space out the CoinAPI calls, which are still one per token. CoinGecko is no
# longer paced because it is no longer asked more than once -- see
# coingecko_rates below.
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


def coingecko_rates(
    tokens: Sequence[str], token_translation: dict, client: httpx.Client
) -> tuple[int | str, dict[str, dict]]:
    """Every token's rate from CoinGecko, in one request.

    /simple/price takes a comma-separated list of ids, so a cycle needs one
    call rather than one per token. That is not just cheaper, it is the
    difference between working and not: paced at 10s a token, a cycle made
    about six requests a minute, and the keyless endpoint began answering 429
    after roughly ten of them. The tokens at the back of the queue -- CCD among
    them -- got nothing, every single run.

    Returns (status, {token: rate document}). A token missing from the mapping
    was either not asked for (no id) or not answered for.
    """
    wanted = {token: token_translation[token] for token in tokens if token_translation.get(token)}
    if not wanted:
        return "not called", {}

    ids = ",".join(sorted(set(wanted.values())))
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=usd&include_last_updated_at=true"
    )
    # COIN_GECKO_API_KEY has been set in the recurring stack all along without
    # anything reading it, so every call so far went out as an anonymous one
    # against the shared-IP rate limit. A demo key is sent as this header
    # against the public host; a Pro key needs pro-api.coingecko.com and
    # x-cg-pro-api-key instead, so if one is ever issued this has to change.
    headers = {"x-cg-demo-api-key": COIN_GECKO_API_KEY} if COIN_GECKO_API_KEY else None
    response = client.get(url, headers=headers)
    if response.status_code != 200:
        return response.status_code, {}

    payload = response.json()
    rates: dict[str, dict] = {}
    for token, coingecko_id in wanted.items():
        quote = payload.get(coingecko_id)
        if not quote or "usd" not in quote:
            # Asked for, not answered for: CoinGecko knows the id but has no
            # USD price right now. CoinAPI still gets its turn below.
            continue
        rates[token] = {
            "_id": f"USD/{token}",
            "token": token,
            # CoinGecko's own timestamp for the price, not the time we fetched
            # it: a thinly traded token keeps an old timestamp here while still
            # being refreshed every cycle.
            "timestamp": (
                dt.datetime.fromtimestamp(quote["last_updated_at"], tz=timezone.utc)
                if "last_updated_at" in quote
                else dt.datetime.now(tz=timezone.utc)
            ),
            "rate": quote["usd"],
            "source": "CoinGecko",
        }
    return response.status_code, rates


def get_token_translations_from_mongo(mongodb: MongoDB):
    result = list(
        mongodb.utilities[CollectionsUtilities.token_api_translations].find(
            {"service": "coingecko"}
        )
    )
    return {x["token"]: x["translation"] for x in list(result)}


def fetch_rate_from_coinapi(context, token: str, client: httpx.Client) -> dict | None:
    """One token's spot rate from CoinAPI, for what CoinGecko did not cover.

    CoinGecko goes first now, because it answers for every token in one call.
    CoinAPI is what is left for tokens it has no id for, and it is still one
    request each, so it stays paced.
    """
    try:
        status, result = coinapi(token, client)
        if result:
            context.log.info(f"CoinAPI result: {result['rate']} {result['_id']}")
            return result
    except Exception as e:
        context.log.error(f"Recurring: Error in CoinAPI call for {token}. Error: {e}")
        return None

    context.log.error(f"Recurring: No spot rate for {token} from CoinAPI (status {status}).")
    return None


def perform_spot_retrieval_update(
    context, tokens: Sequence[str], mongodb: MongoDB
) -> tuple[list[str], list[str]]:
    """Fetch the spot rate for every token and store them all in one write.

    Takes the whole set of tokens rather than one, because this now runs once
    per cycle instead of once per token: the translation table is read once, a
    single HTTP client is reused across the calls, and the rates land in one
    bulk_write.

    Returns (written, failed, unpriceable):

    * written      -- the tokens whose rate was stored
    * failed       -- tokens that have a CoinGecko id configured and still came
                      back with nothing. Something is wrong: the provider is
                      down, rate-limiting, or no longer lists the token.
    * unpriceable  -- tokens with no CoinGecko id at all. CoinAPI was their only
                      route to a price and it did not answer, so there is
                      nothing left to ask. That is a gap in configuration, not a
                      failure of this run, and it is the same every cycle.

    The split exists because failing the run is an alert, and an alert that
    fires every ten minutes for a condition nobody is going to fix this morning
    stops being read.
    """
    coingecko_token_translation = get_token_translations_from_mongo(mongodb)

    queue: list[ReplaceOne] = []
    written: list[str] = []
    failed: list[str] = []
    unpriceable: list[str] = []

    with httpx.Client() as client:
        # One request covers every token CoinGecko has an id for.
        status, from_coingecko = coingecko_rates(tokens, coingecko_token_translation, client)
        if from_coingecko:
            context.log.info(
                f"CoinGecko returned {len(from_coingecko)} rate(s) in one request: "
                f"{', '.join(sorted(from_coingecko))}"
            )
        elif status != "not called":
            context.log.error(f"Recurring: the CoinGecko batch request returned {status}.")

        coinapi_calls = 0
        for token in tokens:
            result = from_coingecko.get(token)

            if result is None:
                # Anything CoinGecko did not answer for still gets its turn at
                # CoinAPI, which can price a token that has no CoinGecko id.
                # These are one request each, so they stay paced.
                if coinapi_calls:
                    time.sleep(PACING_SECONDS)
                coinapi_calls += 1
                result = fetch_rate_from_coinapi(context, token, client)

            if result is None:
                # Whether this is worth an alert depends on whether the token
                # had anywhere left to go.
                if token in coingecko_token_translation:
                    failed.append(token)
                else:
                    unpriceable.append(token)
                continue

            queue.append(ReplaceOne({"_id": f"USD/{token}"}, result, upsert=True))
            written.append(token)

    if queue:
        _ = mongodb.utilities[CollectionsUtilities.exchange_rates].bulk_write(queue)

    return written, failed, unpriceable
