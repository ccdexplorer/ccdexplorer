import datetime as dt

import dagster as dg
from dagster import AssetKey

from ..paydays.update_payday_apy_calc_for import perform_payday_apy_calc_for
from ..paydays.update_payday_baker_performance import (
    perform_payday_performance_for_bakers,
)
from ..paydays.update_payday_daily_apy_accounts import (
    fill_daily_apy_for_accounts_for_date,
)
from ..paydays.update_payday_daily_apy_validators import (
    fill_daily_apy_for_validators_for_date,
)
from ..paydays.update_payday_init import perform_payday_init
from ..paydays.update_payday_rewards import perform_payday_rewards
from ..paydays.update_payday_save_statistics import save_statistics_for_date
from ..paydays.update_payday_state_information import (
    perform_payday_state_information_for_current_payday,
)
from ._jobs import job_payday_averages
from ._partitions import (
    partitions_def_apy_averages,
    partitions_def_from_staking,
)
from ._resources import (
    GRPCResource,
    MongoDBResource,
    grpc_resource_instance,
    mongodb_resource_instance,
)
from .paydays_day_info import paydays_day_info

asset_name: str = "paydays_calculation_daily"


@dg.asset(
    deps=[paydays_day_info],
    name=asset_name,
    partitions_def=partitions_def_from_staking,
    group_name="source_mongo",
)
def paydays_calculation_daily(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> dg.MaterializeResult:
    """Asset that updates the paydays collection (rewards, performance, current day and daily apy)
    from the partitioned payday.
    Metadata:\n
        - total_rewards: Total rewards for the payday (sum of total_rewards_pools and total_rewards_passive_delegators)
        - total_rewards_pools: Total rewards for pools (sum of total_rewards_validators and total_rewards_pool_delegators)
        - total_rewards_validators: Total rewards for validators
        - total_rewards_pool_delegators: Total rewards for pool delegators
        - total_rewards_passive_delegators: Total rewards for passive delegators
        - daily_apy_all: Daily APY for all accounts
        - daily_apy_validators: Average daily APY for validators
        - daily_apy_pool_delegators: Daily APY for pool delegators
        - daily_apy_passive_delegators: Daily APY for passive delegators
        - validator_staked_amount: Staked amount by validators
        - delegator_staked_amount: Staked amount by delegators
        - passive_staked_amount: Staked amount by passive delegators
        - count_validators: Count of validators
        - count_pool_delegators: Count of pool delegators
        - count_passive_delegators: Count of passive delegators
    """
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    partition_key = context.partition_key
    latest_event = context.instance.get_latest_materialization_event(asset_key=paydays_day_info.key)
    if latest_event is None:
        raise RuntimeError("No materialization found for paydays_day_info")

    if not latest_event.asset_materialization:
        raise RuntimeError("No materialization found for paydays_day_info")

    context.log.info(f"Processing partition key: {partition_key}")
    # metadata = latest_event.asset_materialization.metadata

    # if "date" not in metadata or "hash" not in metadata:
    #     raise RuntimeError(
    #         "Metadata for paydays_day_info does not contain 'date' or 'hash'."
    #     )

    # if metadata["date"].value != partition_key:
    #     hash = None
    # else:
    #     hash = metadata["hash"].value

    hash = None
    payday_info = perform_payday_init(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_date_string=partition_key,
        payday_block_hash=hash,  # type: ignore
    )
    payday_info = perform_payday_state_information_for_current_payday(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )

    perform_payday_performance_for_bakers(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )
    payday_info = perform_payday_apy_calc_for(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )

    payday_info = perform_payday_rewards(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )
    payday_info = fill_daily_apy_for_accounts_for_date(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )
    payday_info = fill_daily_apy_for_validators_for_date(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )
    payday_info = save_statistics_for_date(
        context=context,
        mongodb=mongodb,
        grpcclient=grpcclient,
        payday_info=payday_info,
    )

    return dg.MaterializeResult(
        metadata={
            "total_rewards": dg.MetadataValue.float(payday_info.metadata["total_rewards"]),
            "total_rewards_pools": dg.MetadataValue.float(
                payday_info.metadata["total_rewards_pools"]
            ),
            "total_rewards_validators": dg.MetadataValue.float(
                payday_info.metadata["total_rewards_validators"]
            ),
            "total_rewards_pool_delegators": dg.MetadataValue.float(
                payday_info.metadata["total_rewards_pool_delegators"]
            ),
            "total_rewards_passive_delegators": dg.MetadataValue.float(
                payday_info.metadata["total_rewards_passive"]
            ),
            "restaked_rewards": dg.MetadataValue.float(payday_info.metadata["restaked_rewards"]),
            "paidout_rewards": dg.MetadataValue.float(payday_info.metadata["paidout_rewards"]),
            "restaked_rewards_perc": dg.MetadataValue.float(
                payday_info.metadata["restaked_rewards_perc"]
            ),
            "daily_apy_all": dg.MetadataValue.float(payday_info.metadata["daily_apy_all"]),
            "daily_apy_validators": dg.MetadataValue.float(
                payday_info.metadata["daily_apy_validators"]
            ),
            "daily_apy_pool_delegators": dg.MetadataValue.float(
                payday_info.metadata["average_daily_apy_pool_delegators"]
            ),
            "daily_apy_passive_delegators": dg.MetadataValue.float(
                payday_info.metadata["daily_apy_passive"]
            ),
            "validator_staked_amount": dg.MetadataValue.float(
                payday_info.metadata["validator_staked_amount"]
            ),
            "delegator_staked_amount": dg.MetadataValue.float(
                payday_info.metadata["delegator_staked_amount"]
            ),
            "passive_staked_amount": dg.MetadataValue.float(
                payday_info.metadata["passive_staked_amount"]
            ),
            "count_validators": dg.MetadataValue.int(payday_info.metadata["count_validators"]),
            "count_pool_delegators": dg.MetadataValue.int(
                payday_info.metadata["count_pool_delegators"]
            ),
            "count_passive_delegators": dg.MetadataValue.int(
                payday_info.metadata["count_passive_delegators"]
            ),
        }
    )


@dg.asset_sensor(
    asset_key=AssetKey(asset_name),
    default_status=dg.DefaultSensorStatus.RUNNING,
    jobs=[job_payday_averages],
)
def trigger_payday_averages(context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry):
    today = dt.datetime.now(dt.UTC).date()

    # Get the partition key from the dagster event
    dagster_event = asset_event.dagster_event
    if not dagster_event:
        return dg.SensorResult(skip_reason=f"No dagster_event found for asset {asset_name}")
    triggered_partition_key = dagster_event.partition

    if today.strftime("%Y-%m-%d") != triggered_partition_key:
        return dg.SensorResult(
            skip_reason=f"Asset partition key {triggered_partition_key} does not match today's date {today.strftime('%Y-%m-%d')}"
        )

    multi_partitioned_jobs = []
    for job in [job_payday_averages]:
        for grouping in partitions_def_apy_averages.get_partition_keys():
            multi_partitioned_jobs.append(
                dg.RunRequest(
                    run_key=f"{context.cursor}_{job.name}_{grouping}",
                    job_name=job.name,
                    partition_key=f"{triggered_partition_key}|{grouping}",
                )
            )

    return multi_partitioned_jobs


defs = dg.Definitions(
    assets=[paydays_calculation_daily],
    sensors=[trigger_payday_averages],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
