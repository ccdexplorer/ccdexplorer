import dagster as dg

from ..recurring.update_ccd_market_position import perform_market_position_update
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name = "market_position"


######### Market Position #########
@dg.asset(group_name="source_coinmarketcap", tags={"reserved": "critical_job"})
def market_position(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    A collection that is used to store the market position of Concordium (CCD) as retrieved
     from CoinMarketCap."""
    mongodb = mongo_resource.get_client()
    dct = perform_market_position_update(context, mongodb)
    if dct is None:
        raise Exception("perform_market_position_update() returned None")

    context.add_asset_metadata({"dct": dct})
    return dct


@dg.asset_check(asset=market_position)
def price_is_available(dct: dict) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=dct["rate"] > 0,
        metadata={"price": dct["rate"]},
    )


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
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[market_position],
    asset_checks=[price_is_available],
    jobs=[job],
    schedules=[schedule],
    resources={"mongo_resource": mongodb_resource_instance},
)
