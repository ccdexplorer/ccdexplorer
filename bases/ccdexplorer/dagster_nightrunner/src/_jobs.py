import dagster as dg
from dagster import AssetSelection, define_asset_job

from ._partitions import (
    partitions_def_from_genesis,
    partitions_def_from_staking,
    partitions_def_from_trading,
    partitions_def_grouping_from_genesis,
    partitions_def_tokens,
    partitions_def_from_plts,
)

job_from_staking = define_asset_job(
    name="j_from_staking",
    partitions_def=partitions_def_from_staking,
    selection=AssetSelection.assets("classified_pools"),
    executor_def=dg.in_process_executor,
)
job_from_genesis = define_asset_job(
    name="j_from_genesis",
    partitions_def=partitions_def_from_genesis,
    selection=AssetSelection.assets(
        "account_graph",
        "daily_limits",
        "daily_holders",
        "exchange_wallets",
        "tx_fees",
        "tx_types",
        "microccd",
        "release_amounts",
        "ccd_classified",
        "network_summary",
        "network_activity",
        "tx_by_project",
        "mongo_transactions",
    ),
    executor_def=dg.in_process_executor,
)

job_updated_sender_ids = define_asset_job(
    name="j_updated_sender_ids",
    partitions_def=partitions_def_from_genesis,
    selection=AssetSelection.assets("updated_sender_ids"),
    executor_def=dg.in_process_executor,
)

job_from_trading = define_asset_job(
    name="j_from_trading",
    partitions_def=partitions_def_from_trading,
    selection=AssetSelection.assets("exchange_volume"),
)

job_historical_rates = define_asset_job(
    name="j_historical_rates",
    partitions_def=partitions_def_tokens,
    selection=AssetSelection.assets("historical_rates"),
)

job_forex = define_asset_job(
    name="j_forex",
    partitions_def=partitions_def_from_plts,
    selection=AssetSelection.assets("forex"),
)

job_unique_addresses = define_asset_job(
    name="j_unique_addresses",
    partitions_def=partitions_def_grouping_from_genesis,
    selection=AssetSelection.assets("unique_addresses"),
)

# job_update_days_to_rerun = define_asset_job(
#     name="j_days_to_rerun",
#     partitions_def=partitions_def_grouping_from_genesis,
#     selection=AssetSelection.assets("days_to_rerun"),
# )


defs = dg.Definitions(
    jobs=[
        job_from_staking,
        job_from_genesis,
        job_from_trading,
        job_historical_rates,
        job_unique_addresses,
        job_updated_sender_ids,
        job_forex,
    ]
)
