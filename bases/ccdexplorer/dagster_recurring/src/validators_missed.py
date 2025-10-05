import dagster as dg

from ..recurring.update_validators_missed import perform_validators_missed_update
from ._resources import (
    GRPCResource,
    grpc_resource_instance,
    MongoDBResource,
    mongodb_resource_instance,
)

asset_name = "validators_missed"


@dg.asset(
    group_name="source_grpc",
)
def validators_missed(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> dict:
    """
    Missing validators information
    """
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    result = perform_validators_missed_update(context, grpcclient, mongodb)
    return result


job = dg.define_asset_job(f"j_{asset_name}", selection=[validators_missed])


@dg.schedule(
    job=job,
    cron_schedule="2 * * * *",
    name=f"s_{asset_name}",
)
def schedule(context):
    """Schedule that runs all partitions every 15 minutes"""
    # Get all partition keys and create a run request for each
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

    return dg.RunRequest(run_key=f"{context.scheduled_execution_time.isoformat()}")


defs = dg.Definitions(
    assets=[validators_missed],
    jobs=[job],
    schedules=[schedule],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
