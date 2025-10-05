import httpx
from ccdexplorer.mongodb import Collections, MongoDB, CollectionsUtilities

from pymongo import ReplaceOne
from pymongo.collection import Collection
from ccdexplorer.env import COIN_MARKET_CAP_API_KEY


def perform_market_position_update(context, mongodb: MongoDB) -> dict:
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id=18031"
    context.log.info(f"Fetching data from {url}")
    # Your asset logic here
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": COIN_MARKET_CAP_API_KEY,
    }
    context.log.info(f"Headers: {headers}")
    result = {}
    with httpx.Client(headers=headers) as client:
        response = client.get(url)
        dct = {}
        if response.status_code == 200:
            result = response.json()
            result = result["data"]["18031"]

            context.log.info(f"Fetched data: {result}")
            dct: dict = result
            dct.update({"_id": "coinmarketcap_data"})
            if "id" in dct:
                dct["cmc_id"] = dct["id"]
                del dct["id"]

            db: dict[Collections, Collection] = mongodb.mainnet
            local_queue = []

            local_queue.append(
                ReplaceOne(
                    {"_id": "coinmarketcap_data"},
                    replacement=dct,
                    upsert=True,
                )
            )

            if len(local_queue) > 0:
                _ = db[Collections.helpers].bulk_write(local_queue)

            if "quote" in dct and "USD" in dct["quote"] and "price" in dct["quote"]["USD"]:
                dct = {
                    "_id": "USD/CCD",
                    "token": "CCD",
                    "timestamp": dct["last_updated"],
                    "rate": dct["quote"]["USD"]["price"],
                    "source": "CoinMarketCap",
                }
                _ = mongodb.utilities[CollectionsUtilities.exchange_rates].replace_one(
                    {"_id": "USD/CCD"},
                    dct,
                    upsert=True,
                )

        else:
            context.log.info(f"{response.status_code} | {response.text}")
        return dct
