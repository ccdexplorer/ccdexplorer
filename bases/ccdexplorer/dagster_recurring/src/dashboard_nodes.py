import dagster as dg

from ..recurring.update_nodes_from_dashboard import perform_update_nodes_from_dashboard
from ._partitions import net_partition
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name = "dashboard_nodes"


######### Dashboard nodes #########
@dg.asset(
    partitions_def=net_partition,
    group_name="source_concordium",
    tags={"reserved": "critical_job"},
)
def dashboard_nodes(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> int:
    """This is the MongoDB collection dashboard_nodes that stores information from
    nodes reporting to the dashboard. Used in Nodes and Validators overview and
    on validator detail pages."""
    mongodb = mongo_resource.get_client()
    partition_key = context.partition_key
    result = perform_update_nodes_from_dashboard(context, mongodb, partition_key)
    return result


job = dg.define_asset_job("j_dashboard_nodes", selection=["dashboard_nodes"])


@dg.schedule(
    job=job,
    cron_schedule="*/1 * * * *",
    name=f"s_{asset_name}",
)
def dashboard_nodes_schedule(context):
    """Schedule that runs all partitions every 1 min"""
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
    assets=[dashboard_nodes],
    jobs=[job],
    schedules=[dashboard_nodes_schedule],
    resources={"mongo_resource": mongodb_resource_instance},
)
