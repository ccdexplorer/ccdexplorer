import dagster as dg

from ..recurring.update_spot_retrieval import perform_spot_retrieval_update
from ._partitions import partitions_def_tokens
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name = "spot_retrieval"


######### spot_retrieval #########
@dg.asset(
    group_name="source_coinmarketcap",
    partitions_def=partitions_def_tokens,
    tags={"reserved": "critical_job"},
)
def spot_retrieval(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict | None:
    """
    Retrieving spot price data for selected CIS-2 tokens and all PLTs."""
    mongodb = mongo_resource.get_client()
    partition_key = context.partition_key
    retrieval_success, dct = perform_spot_retrieval_update(context, partition_key, mongodb)
    return dct


job = dg.define_asset_job(f"j_{asset_name}", selection=[asset_name])


@dg.schedule(
    job=job,
    cron_schedule="*/10 * * * *",
    name=f"s_{asset_name}",
)
def schedule(context):
    # Find runs of the same job that are currently running
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=f"j_{asset_name}",
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
            ],
        )
    )

    # Skip a schedule run if another run of the same job is already running
    if len(run_records) > 0:
        return dg.SkipReason(
            "Skipping this run because another run of the same job is already running"
        )
    partition_keys = partitions_def_tokens.get_partition_keys()

    return [
        dg.RunRequest(
            run_key=f"{context.scheduled_execution_time.isoformat()}_{partition_key}",
            partition_key=partition_key,
        )
        for partition_key in partition_keys
    ]


defs = dg.Definitions(
    assets=[spot_retrieval],
    jobs=[job],
    schedules=[schedule],
    resources={"mongo_resource": mongodb_resource_instance},
)
