from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
import requests
from dateutil import parser
from ..nightrunner.utils import AnalysisType, write_queue_to_collection

# tooter: Tooter = Tooter()
# mongodb: MongoDB = MongoDB(tooter, nearest=True)


def perform_data_for_exchange_volume(context, d_date: str, mongodb: MongoDB) -> dict:
    """Fetch CCD exchange volume from CoinGecko and store it in MongoDB."""

    queue = []
    analysis = AnalysisType.statistics_ccd_volume

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)

    date_in_datetime_format = parser.parse(d_date)
    date_in_coingecko_format = f"{date_in_datetime_format:%d-%m-%Y}"

    a = requests.get(
        f"https://api.coingecko.com/api/v3/coins/concordium/history?date={date_in_coingecko_format}&localization=false"
    )
    if a.status_code == 200:
        ccd_usd = a.json()["market_data"]["current_price"]["usd"]
        vol_usd = a.json()["market_data"]["total_volume"]["usd"]
        ccd_eur = a.json()["market_data"]["current_price"]["eur"]
        vol_eur = a.json()["market_data"]["total_volume"]["eur"]
        vol_ccd = vol_usd / ccd_usd

        # fix for coingecko not having the right data.
        if d_date == "2022-02-11":
            vol_ccd = 16_827_000

        dct = {
            "_id": _id,
            "type": analysis.value,
            "date": d_date,
            "ccd_usd": f"{ccd_usd:.6f}",
            "ccd_eur": f"{ccd_eur:.6f}",
            "vol_usd": f"{vol_usd:.0f}",
            "vol_eur": f"{vol_eur:.0f}",
            "vol_ccd": f"{vol_ccd:.0f}",
            "label": "Total Volume",
        }

        queue.append(
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        )
        if len(queue) > 0:
            _ = mongodb.mainnet[Collections.statistics].bulk_write(queue)
            queue = []

        context.log.info(f"ccd_volume: {dct}")
        write_queue_to_collection(mongodb, queue, analysis)
    else:
        dct = {}
        context.log.info(f"Failed with status code {a.status_code} for {d_date}")
        raise ValueError(
            f"Failed to fetch data from CoinGecko for {d_date}. Status code: {a.status_code}"
        )
    return {"message": f"ccd_volume: {dct}"}
