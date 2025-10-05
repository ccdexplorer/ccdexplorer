import io
from datetime import timedelta
from enum import Enum
from io import StringIO

import pandas as pd
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
from ccdexplorer.domain.mongo import MongoTypeInstance
from ccdexplorer.mongodb import Collections, CollectionsUtilities, MongoDB
from dateutil import parser
from git import Commit
from pandas import DataFrame
from pymongo import ReplaceOne
import polars as pl


class AnalysisType(Enum):
    """
    Analyses are performed on a nightly schedule and are displayed
    on the statistics page or used elsewhere.
    """

    statistics_daily_holders = "statistics_daily_holders"
    statistics_daily_limits = "statistics_daily_limits"
    statistics_network_summary = "statistics_network_summary"
    statistics_classified_pools = "statistics_classified_pools"
    statistics_microccd = "statistics_microccd"
    statistics_ccd_classified = "statistics_ccd_classified"
    statistics_exchange_wallets = "statistics_exchange_wallets"
    statistics_ccd_volume = "statistics_ccd_volume"
    statistics_release_amounts = "statistics_release_amounts"
    statistics_mongo_transactions = "statistics_mongo_transactions"
    statistics_network_activity = "statistics_network_activity"
    statistics_transaction_fees = "statistics_transaction_fees"
    statistics_bridges_and_dexes = "statistics_bridges_and_dexes"
    statistics_historical_exchange_rates = "statistics_historical_exchange_rates"
    statistics_transaction_types = "statistics_transaction_types"
    statistics_transaction_count = "statistics_transaction_count"
    statistics_tvl_for_tokens = "statistics_tvl_for_tokens"
    statistics_unique_addresses_daily = "statistics_unique_addresses_daily"
    statistics_unique_addresses_weekly = "statistics_unique_addresses_weekly"
    statistics_unique_addresses_monthly = "statistics_unique_addresses_monthly"

    statistics_unique_addresses_v2_daily = "statistics_unique_addresses_v2_daily"
    statistics_unique_addresses_v2_weekly = "statistics_unique_addresses_v2_weekly"
    statistics_unique_addresses_v2_monthly = "statistics_unique_addresses_v2_monthly"

    statistics_transaction_types_by_project = "statistics_transaction_types_by_project"
    # used as indicator whether that date is already processed or needs to be processed again
    statistics_transaction_types_by_project_dates = "statistics_transaction_types_by_project_dates"
    account_graph = "account_graph"
    # used as indicator whether that date is already processed or needs to be processed again
    account_graph_dates = "account_graph_dates"
    statistics_plt = "statistics_plt"
    statistics_realized_prices = "statistics_realized_prices"


def write_queue_to_collection(mongodb: MongoDB, queue: list[ReplaceOne], analysis: AnalysisType):
    if len(queue) > 0:
        _ = mongodb.mainnet[Collections.statistics].bulk_write(queue)

    result = mongodb.mainnet[Collections.helpers].find_one({"_id": "statistics_rerun"})
    # set rerun to False
    if not result:
        result = {}
    result[analysis.value] = False
    _ = mongodb.mainnet[Collections.helpers].bulk_write(
        [
            ReplaceOne(
                {"_id": "statistics_rerun"},
                replacement=result,
                upsert=True,
            )
        ]
    )


def create_module_dict_from_instances(mongodb: MongoDB):
    result_instances = [
        MongoTypeInstance(**x) for x in mongodb.mainnet[Collections.instances].find({})
    ]
    modules_dict = {}
    for instance in result_instances:
        if instance.v0:
            module_ref = instance.v0.source_module
        elif instance.v1:
            module_ref = instance.v1.source_module
        else:
            module_ref = None
        if module_ref in modules_dict:
            modules_dict[module_ref].extend([instance.id])
        else:
            modules_dict[module_ref] = [instance.id]

    return modules_dict


def find_new_instances_from_project_modules(mongodb: MongoDB):
    modules_dict = create_module_dict_from_instances(mongodb)
    instances_to_write = []
    result = mongodb.mainnet[Collections.projects].find({"type": "module"})
    for module in list(result):
        project_id = module["project_id"]

        instances = modules_dict.get(module["module_ref"], [])
        for contract_address in instances:
            contract_as_class = CCD_ContractAddress.from_str(contract_address)
            _id = f"{project_id}-address-{contract_address}"

            # we need to check if this address is already in the collection
            # and has a display_name. In that case, do NOT overwrite.
            contract_in_collection = mongodb.mainnet[Collections.projects].find_one({"_id": _id})
            if contract_in_collection:
                if "display_name" in contract_in_collection:
                    continue

            d_address = {"project_id": project_id}
            d_address.update(
                {
                    "_id": _id,
                    "type": "contract_address",
                    "contract_index": contract_as_class.index,
                    "contract_address": contract_address,
                }
            )

            instances_to_write.append(
                ReplaceOne(
                    {"_id": _id},
                    replacement=d_address,
                    upsert=True,
                )
            )
    if len(instances_to_write) > 0:
        pass
        _ = mongodb.mainnet[Collections.projects].bulk_write(instances_to_write)


def get_start_end_block_from_date(mongodb: MongoDB, date: str) -> tuple[int, int]:
    result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": date})
    if result:
        return result["height_for_first_block"], result["height_for_last_block"]
    else:
        # if it's today or in the future

        # find yesterday's date and find the last block there
        yesterday_date = f"{(parser.parse(date) - timedelta(days=1)):%Y-%m-%d}"
        result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": yesterday_date})
        end_height = 1_000_000_000
        if not result:
            # if we cannot find yesterday's date, we are requesting a further date
            start_height = 0
        else:
            # we have found yesterday, so we can set the start height
            start_height = result["height_for_last_block"] + 1

        return start_height, end_height


# def get_start_end_block_from_date(mongodb: MongoDB, date: str) -> tuple[int, int]:
#     result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": date})
#     if result:
#         return result["height_for_first_block"], result["height_for_last_block"]
#     else:
#         # if it's today...
#         # set last block to the last block we can find
#         height_result = mongodb.mainnet[Collections.blocks].find_one(
#             {}, sort=[("height", -1)]
#         )
#         if height_result:
#             end_height = height_result["height"]
#         else:
#             end_height = 1_000_000_000

#         # find yesterday's date and find the last block there
#         yesterday_date = f"{(parser.parse(date) - timedelta(days=1)):%Y-%m-%d}"
#         result = mongodb.mainnet[Collections.blocks_per_day].find_one(
#             {"date": yesterday_date}
#         )
#         if not result:
#             # if we cannot find yesterday's date, we assume the start height is 1
#             start_height = 0
#             end_height = 0
#         else:
#             start_height = result["height_for_last_block"] + 1

#         return start_height, end_height


def get_df_from_git(commit: Commit | None) -> DataFrame | None:
    if not commit:
        return None
    targetfile = commit.tree / "accounts.csv"
    with io.BytesIO(targetfile.data_stream.read()) as f:
        my_file = f.read().decode("utf-8")
    data = StringIO(my_file)
    df = pd.read_csv(data, low_memory=False)

    return df


def get_polars_df_from_git(commit: Commit | None) -> pl.DataFrame | None:
    if not commit:
        return None
    targetfile = commit.tree / "accounts.csv"
    with io.BytesIO(targetfile.data_stream.read()) as f:
        my_file = f.read().decode("utf-8")
    data = StringIO(my_file)
    df = pl.read_csv(data, low_memory=False, ignore_errors=True)

    return df


def get_date_from_git(commit: Commit) -> str:
    timestamp = parser.parse(commit.message)
    d_date = f"{timestamp:%Y-%m-%d}"
    return d_date


def get_exchanges(mongodb: MongoDB) -> dict[str, list[str]]:
    result = mongodb.utilities[CollectionsUtilities.labeled_accounts].find(
        {"label_group": "exchanges"}
    )

    exchanges = {}
    for r in result:
        key = r["label"].lower().split(" ")[0]
        current_list: list[str] = exchanges.get(key, [])
        current_list.append(r["_id"][:29])
        exchanges[key] = current_list
    return exchanges


def get_hash_from_date(date: str, mongodb: MongoDB) -> str:
    result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": date})
    if not result:
        return "0" * 64
    return result["hash_for_last_block"]


def get_total_amount_from_summary_for_date(date: str, mongodb: MongoDB) -> int:
    result: dict | None = mongodb.mainnet[Collections.statistics].find_one(
        {"$and": [{"type": "statistics_network_summary"}, {"date": date}]}
    )
    if not result:
        exit(f"No statistics_network_summary for {date}")
    else:
        return int(result["total_amount"])


def get_all_dates_with_info(mongodb: MongoDB) -> dict:
    return {x["date"]: x for x in mongodb.mainnet[Collections.blocks_per_day].find({})}
