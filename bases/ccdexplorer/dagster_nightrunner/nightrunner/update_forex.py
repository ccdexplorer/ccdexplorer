import asyncio
import datetime as dt

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


async def retrieve_forex_data(context, d_date: str) -> list[ReplaceOne]:
    """
    This is the implementation of the https://github.com/fawazahmed0/exchange-api API.
    """

    return_list_for_date = []

    session = aiohttp.ClientSession()
    url = f"https://{d_date}.currency-api.pages.dev/v1/currencies/usd.json"
    async with session.get(url) as resp:
        result: dict | None = None
        if not resp.ok:
            async with session.get(
                f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{d_date}/v1/currencies/usd.json"
            ) as resp:
                if resp.ok:
                    result = await resp.json()
        else:
            result = await resp.json()

        if result is not None:
            result = result["usd"]

            for token, price in result.items():  # pyright: ignore[reportOptionalMemberAccess]
                token: str = token.upper()
                return_dict = {
                    "_id": f"USD/{token}-{d_date}",
                    "token": token,
                    # "timestamp": timestamp,
                    "date": d_date,
                    "rate": 1 / price if price > 0 else 0,
                    "source": "exchange-api",
                }
                return_list_for_date.append(
                    ReplaceOne(
                        {"_id": f"USD/{token}-{d_date}"},
                        return_dict,
                        upsert=True,
                    )
                )
        # sleep to prevent from being rate-limited.

        else:
            return_list_for_date = []
    return return_list_for_date


async def perform_forex_update(context, d_date: str, mongodb: MongoDB) -> bool:
    queue = []

    queue = await retrieve_forex_data(context, d_date)

    if len(queue) > 0:
        _ = mongodb.utilities[CollectionsUtilities.exchange_rates_historical].bulk_write(queue)

        # update exchange rates retrieval
        query = {"_id": "heartbeat_last_timestamp_forex"}
        mongodb.mainnet[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_last_timestamp_forex",
                "timestamp": dt.datetime.now().astimezone(dt.timezone.utc),
            },
            upsert=True,
        )

    return len(queue) > 0


# if __name__ == "__main__":
#     d_date = "2025-12-05"  # (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
#     asyncio.run(perform_forex_update(None, d_date, mongodb))
