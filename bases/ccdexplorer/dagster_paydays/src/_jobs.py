import dagster as dg
from dagster import AssetSelection, define_asset_job

from ._partitions import (
    partitions_def_from_staking,
    partitions_def_daily_apy_averages,
)

job_payday_daily = define_asset_job(
    name="j_payday_daily",
    partitions_def=partitions_def_from_staking,
    selection=AssetSelection.assets(
        "paydays_calculation_daily",
    ),
    # executor_def=dg.in_process_executor,
)


job_payday_averages = define_asset_job(
    name="j_payday_averages",
    partitions_def=partitions_def_daily_apy_averages,
    selection=AssetSelection.assets("paydays_calculation_averages"),
)

defs = dg.Definitions(jobs=[job_payday_daily, job_payday_averages])
