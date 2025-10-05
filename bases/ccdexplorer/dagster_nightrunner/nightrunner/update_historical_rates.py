import asyncio
import datetime as dt
import time
import aiohttp
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
)
from ccdexplorer.tooter import Tooter
from pymongo import ReplaceOne

tooter: Tooter = Tooter()
mongodb: MongoDB = MongoDB(tooter, nearest=True)


async def get_token_translations_from_mongo() -> dict[str, str]:
    result = mongodb.utilities[CollectionsUtilities.token_api_translations].find(
        {"service": "coingecko"}
    )
    coingecko_token_translation = {x["token"]: x["translation"] for x in list(result)}
    return coingecko_token_translation


async def coingecko_historical(context, token: str):
    """
    This is the implementation of the CoinGecko historical API.
    """
    coingecko_token_translation = await get_token_translations_from_mongo()
    token_to_request = coingecko_token_translation.get(token)

    return_list_for_token = []
    # only for tokens that we know we can request
    if token_to_request:
        session = aiohttp.ClientSession()
        url = f"https://api.coingecko.com/api/v3/coins/{token_to_request}/market_chart?vs_currency=usd&days=364&interval=daily&precision=full"
        async with session.get(url) as resp:
            if resp.ok:
                result = await resp.json()
                result = result["prices"]
                context.log.info(
                    f"Historic: {token} | {token_to_request} | {resp.status} | {len(result)} days"
                )
                for timestamp, price in result:
                    formatted_date = (
                        f"{dt.datetime.fromtimestamp(timestamp / 1000, tz=dt.UTC):%Y-%m-%d}"
                    )
                    return_dict = {
                        "_id": f"USD/{token}-{formatted_date}",
                        "token": token,
                        "timestamp": timestamp,
                        "date": formatted_date,
                        "rate": price,
                        "source": "CoinGecko",
                    }
                    return_list_for_token.append(
                        ReplaceOne(
                            {"_id": f"USD/{token}-{formatted_date}"},
                            return_dict,
                            upsert=True,
                        )
                    )
                # sleep to prevent from being rate-limited.
                await asyncio.sleep(61)
            else:
                pass
    return return_list_for_token


async def perform_historical_rates_update(context, token: str, mongodb: MongoDB) -> bool:
    if token not in [
        "ETH",
        "USDT",
        "USDC",
        "BTC",
        # "MANA",
        "DAI",
        # "BUSD",
        "VNXAU",
        "DOGE",
        # "SHIB",
        "CCD",
        "EURR",
        "USDR",
    ]:
        return True

    queue = []

    queue = await coingecko_historical(context, token)

    if len(queue) > 0:
        _ = mongodb.utilities[CollectionsUtilities.exchange_rates_historical].bulk_write(queue)

        # update exchange rates retrieval
        query = {"_id": "heartbeat_last_timestamp_exchange_rates_historical"}
        mongodb.mainnet[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_last_timestamp_exchange_rates_historical",
                "timestamp": dt.datetime.now().astimezone(dt.timezone.utc),
            },
            upsert=True,
        )
    time.sleep(61)
    return len(queue) > 0
