import dagster as dg

from ..recurring.update_top_impacted_addresses import update_top_impacted_addresses
from ._partitions import net_partition
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name = "top_impacted_addresses"


@dg.asset(
    partitions_def=net_partition,
    group_name="source_mongo",
    tags={"reserved": "critical_job"},
)
def top_impacted_addresses(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    A collection that is used to store the top impacted addresses, used for tx lists."""
    mongodb = mongo_resource.get_client()
    partition_key = context.partition_key
    result = update_top_impacted_addresses(context, mongodb, partition_key)
    return result


job = dg.define_asset_job(f"j_{asset_name}", selection=[top_impacted_addresses])


@dg.schedule(
    job=job,
    cron_schedule="*/5 * * * *",
    name=f"s_{asset_name}",
)
def schedule(context):
    # Skip if there's already a run in progress for this job
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=job.name,
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
            ],
        )
    )
    if len(run_records) > 0:
        return dg.SkipReason(
            f"Skipping this run because another run of job '{job.name}' is already running"
        )

    # Get all partition keys and create a run request for each
    partition_keys = net_partition.get_partition_keys()

    return [
        dg.RunRequest(
            run_key=f"{context.scheduled_execution_time.isoformat()}_{partition_key}",
            partition_key=partition_key,
        )
        for partition_key in partition_keys
    ]


defs = dg.Definitions(
    assets=[top_impacted_addresses],
    jobs=[job],
    schedules=[schedule],
    resources={"mongo_resource": mongodb_resource_instance},
)
