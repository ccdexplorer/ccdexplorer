import datetime as dt
import time
from datetime import timezone

import dateutil
import httpx
from ccdexplorer.env import COIN_API_KEY
from ccdexplorer.mongodb import (
    CollectionsUtilities,
    MongoDB,
)
from pymongo import ReplaceOne


def coinapi(token: str):
    status = -1

    url = f"https://rest.coinapi.io/v1/exchangerate/{token}/USD/apikey-{COIN_API_KEY}/"
    with httpx.Client() as client:
        response = client.get(url)

        if response.status_code == 200:
            result = response.json()
            return_dict = {
                "_id": f"USD/{token}",
                "token": token,
                "timestamp": dateutil.parser.parse(result["time"]),
                "rate": result["rate"],
                "source": "CoinAPI",
            }
        else:
            return_dict = None

    return status, return_dict


def coingecko(token: str, token_translation: dict):
    token_to_request = token_translation.get(token)
    status = -2
    if token_to_request:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={token_to_request}&vs_currencies=usd&include_last_updated_at=true"
        with httpx.Client() as client:
            response = client.get(url)

            if response.status_code == 200:
                result = response.json()
                result = result[token_to_request]
                return_dict = {
                    "_id": f"USD/{token}",
                    "token": token,
                    "timestamp": (
                        dt.datetime.fromtimestamp(result["last_updated_at"], tz=timezone.utc)
                        if "last_updated_at" in result
                        else dt.datetime.now(tz=timezone.utc)
                    ),
                    "rate": result["usd"],
                    "source": "CoinGecko",
                }
            else:
                status = response.status_code
                return_dict = None
    else:
        return_dict = None
    return status, return_dict


def get_token_translations_from_mongo(mongodb: MongoDB):
    result = list(
        mongodb.utilities[CollectionsUtilities.token_api_translations].find(
            {"service": "coingecko"}
        )
    )
    return {x["token"]: x["translation"] for x in list(result)}


def perform_spot_retrieval_update(
    context, token: str, mongodb: MongoDB
) -> tuple[bool, dict | None]:
    coingecko_token_translation = get_token_translations_from_mongo(mongodb)
    queue = []

    status_coinapi = 0
    status_coingecko = 0
    result: dict | None = None
    try:
        status_coinapi, result = coinapi(token)
        if result:
            context.log.info(f"CoinAPI result: {result['rate']} {result['_id']}")
            queue.append(
                ReplaceOne(
                    {"_id": f"USD/{token}"},
                    result,
                    upsert=True,
                )
            )
    except Exception as e:
        context.log.error(
            f"Recurring: Error in CoinAPI call for {token} with status {status_coinapi}. Error: {e}"
        )
        return False, None

    if not result:
        try:
            status_coingecko, result = coingecko(token, coingecko_token_translation)
            if result:
                context.log.info(f"Coingecko result: {result['rate']} {result['_id']}")
                queue.append(
                    ReplaceOne(
                        {"_id": f"USD/{token}"},
                        result,
                        upsert=True,
                    )
                )

        except Exception as e:
            context.log.error(
                f"Recurring: Error in Coingecko call for {token} with status {status_coingecko}. Error: {e}"
            )
            return False, None

    if len(queue) > 0:
        _ = mongodb.utilities[CollectionsUtilities.exchange_rates].bulk_write(queue)

    time.sleep(10)
    return len(queue) > 0, result
