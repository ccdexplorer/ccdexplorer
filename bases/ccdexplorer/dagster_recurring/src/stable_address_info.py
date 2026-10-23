import dagster as dg

from ..recurring.update_stable_address_info import reconcile_stable_address_info
from ._partitions import net_partition
from ._resources import (
    GRPCResource,
    MongoDBResource,
    grpc_resource_instance,
    mongodb_resource_instance,
)

asset_name = "stable_address_info"


@dg.asset(
    partitions_def=net_partition,
    group_name="source_node",
    tags={"reserved": "critical_job"},
)
def stable_address_info(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> dict:
    """Every account the chain has, in the collection that maps address to index.

    ms_accounts writes this collection one account at a time as it sees an
    account_creation transaction, so an account that never produced one --
    a genesis account, or anything from before the indexer's window -- was
    missing, and every link to it rendered as an address instead of an index.
    This reconciles against the node's own account list and adds whatever is
    absent.

    One partition per net. A devnet is replaced periodically and each
    replacement arrives with a fresh genesis set, which is why this has to
    keep running rather than being a one-off backfill.
    """
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    return reconcile_stable_address_info(context, mongodb, grpcclient, context.partition_key)


job = dg.define_asset_job(f"j_{asset_name}", selection=[stable_address_info])


@dg.schedule(
    job=job,
    # Daily, not minutely. The steady state is one get_account_list per net
    # and no writes; what it is here to catch -- a replaced devnet, or
    # account_creation events missed while an indexer was down -- shows up on
    # the scale of days, and the run can be launched by hand from the Dagster
    # UI for one net when it cannot wait.
    cron_schedule="30 4 * * *",
    name=f"s_{asset_name}",
)
def schedule(context):
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

    return [
        dg.RunRequest(
            run_key=f"{context.scheduled_execution_time.isoformat()}_{partition_key}",
            partition_key=partition_key,
        )
        for partition_key in net_partition.get_partition_keys()
    ]


defs = dg.Definitions(
    assets=[stable_address_info],
    jobs=[job],
    schedules=[schedule],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
