import math

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_TokenEvent,
    CCD_TokenInfo,
)
from ccdexplorer.mongodb import Collections, CollectionsUtilities, MongoDB
from pymongo import ReplaceOne
from ccdexplorer.tooter import Tooter

try:
    from ..nightrunner.utils import (
        AnalysisType,
        get_start_end_block_from_date,
        write_queue_to_collection,
    )
except ImportError:
    from ccdexplorer.dagster_nightrunner.nightrunner.utils import (
        AnalysisType,
        get_start_end_block_from_date,
        write_queue_to_collection,
    )


def get_historical_fx_rate_for(token: str, plts_dict: dict, date: str, mongodb: MongoDB) -> float:
    plt_tracks = plts_dict.get(token, {}).get("stablecoin_tracks", None)
    fx_rate = mongodb.utilities[CollectionsUtilities.exchange_rates_historical].find_one(
        {"token": plt_tracks, "date": date}
    )
    if fx_rate and "rate" in fx_rate:
        return fx_rate["rate"]
    return 1.0


def get_plt_txs_for_day(
    start_block: int,
    end_block: int,
    mongodb: MongoDB,
):
    pipeline = [
        {"$match": {"block_info.height": {"$gte": start_block, "$lte": end_block}}},
        {
            "$match": {
                "$or": [
                    {"account_transaction.effects.token_update_effect": {"$exists": True}},
                    {"token_creation": {"$exists": True}},
                ]
            }
        },
    ]
    txs = [
        CCD_BlockItemSummary(**x)
        for x in mongodb.mainnet[Collections.transactions].aggregate(pipeline)
    ]
    return txs


def get_movements_for_token(
    token: str,
    plt_info_eod: CCD_TokenInfo,
    fx_rate: float,
    txs: list[CCD_BlockItemSummary],
) -> tuple[dict, int]:
    movements: dict = {"mint": 0, "burn": 0, "transfer": 0}
    tx_count = 0
    for tx in txs:
        events: list[CCD_TokenEvent] = []
        if tx.token_creation:
            token_id = tx.token_creation.create_plt.token_id
            if token_id != token:
                continue

            tx_count += 1
            events = tx.token_creation.events

        elif tx.account_transaction:
            if tx.account_transaction.effects.token_update_effect:
                events = tx.account_transaction.effects.token_update_effect.events

        for event in events:
            if event.token_id != token:
                continue

            tx_count += 1
            if event.module_event:
                pass
            elif event.transfer_event:
                movements["transfer"] += int(event.transfer_event.amount.value)

            elif event.mint_event:
                movements["mint"] += int(event.mint_event.amount.value)

            elif event.burn_event:
                movements["burn"] += int(event.burn_event.amount.value)

    movements_to_return = {}
    movements_to_return["local_currency"] = {
        "mint": movements["mint"] * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals)),
        "burn": movements["burn"] * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals)),
        "transfer": movements["transfer"]
        * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals)),
    }
    movements_to_return["USD"] = {
        "mint": movements["mint"]
        * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals))
        * fx_rate,
        "burn": movements["burn"]
        * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals))
        * fx_rate,
        "transfer": movements["transfer"]
        * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals))
        * fx_rate,
    }
    return movements_to_return, tx_count


def perform_plt_statistics_update(
    context, d_date: str, mongodb: MongoDB, grpcclient: GRPCClient
) -> dict:
    analysis = AnalysisType.statistics_plt

    plts_dict = {x["_id"]: x for x in mongodb.mainnet[Collections.plts_tags].find({})}

    _id = f"{d_date}-{analysis.value}"
    queue = []
    height_for_first_block, height_for_last_block = get_start_end_block_from_date(mongodb, d_date)
    hash_for_last_block = grpcclient.get_block_info(height_for_last_block)
    contents = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
    }
    txs = get_plt_txs_for_day(height_for_first_block, height_for_last_block, mongodb)
    plts_to_store = {}
    for plt, plt_info in plts_dict.items():
        fx_rate = get_historical_fx_rate_for(plt, plts_dict, d_date, mongodb)
        try:
            plt_info_eod = grpcclient.get_token_info(hash_for_last_block.hash, plt)
        except Exception as e:
            context.log.error(f"Error fetching token info for {plt}: {e}")
            continue

        movements, count_txs = get_movements_for_token(plt, plt_info_eod, fx_rate, txs)
        plts_to_store[plt] = {
            "symbol": plt,
            "fx_rate": fx_rate,
            "stablecoin_tracks": plt_info.get("stablecoin_tracks", None),
            "count_txs": count_txs,
        }
        plts_to_store[plt].update(movements)
        plts_to_store[plt]["local_currency"].update(
            {
                "total_supply": int(plt_info_eod.token_state.total_supply.value)
                * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals))
            }
        )
        plts_to_store[plt]["USD"].update(
            {
                "total_supply": int(plt_info_eod.token_state.total_supply.value)
                * (math.pow(10, -plt_info_eod.token_state.total_supply.decimals))
                * fx_rate,
            }
        )

    contents.update({"tokens": plts_to_store})  # type: ignore
    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=contents,
            upsert=True,
        )
    )

    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(f"info: {contents}")
    return {"dct": contents}


# tooter: Tooter = Tooter()
# mongodb: MongoDB = MongoDB(tooter, nearest=True)
# grpcclient: GRPCClient = GRPCClient()


# if __name__ == "__main__":
#     d_date = "2025-12-10"  # (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
#     perform_plt_statistics_update(None, d_date, mongodb, grpcclient)
