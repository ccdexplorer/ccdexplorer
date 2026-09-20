from collections.abc import Sequence

import dagster as dg

from ..recurring.update_spot_retrieval import perform_spot_retrieval_update
from ._partitions import current_token_keys, partitions_def_tokens
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name = "spot_retrieval"

# Dagster reads a run's partition range from these two tags. There is no public
# constant for them -- dagster._core.storage.tags is private -- so they are
# spelled out here rather than imported from under the private namespace.
PARTITION_RANGE_START_TAG = "dagster/asset_partition_range_start"
PARTITION_RANGE_END_TAG = "dagster/asset_partition_range_end"


def tokens_still_configured(requested: Sequence[str]) -> tuple[list[str], list[str]]:
    """Split a run's partition range into the tokens to fetch and those to skip.

    Partition keys are only ever added -- deleting one would detach that token's
    history -- so the range a scheduled run covers spans every token that has
    ever had a price source, including any whose `get_price_from` has since been
    cleared. Those must not be fetched: there is no source left to ask, so they
    would yield nothing, and a token that yields nothing fails the run. One
    cleared setting would otherwise fail this job every ten minutes, and spend
    rate-limited requests doing it.

    Returns (to_fetch, skipped), both in the order they were requested.
    """
    configured = set(current_token_keys())
    to_fetch = [token for token in requested if token in configured]
    skipped = [token for token in requested if token not in configured]
    return to_fetch, skipped


######### spot_retrieval #########
@dg.asset(
    group_name="source_coinmarketcap",
    partitions_def=partitions_def_tokens,
    backfill_policy=dg.BackfillPolicy.single_run(),
    tags={"reserved": "critical_job"},
)
def spot_retrieval(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> None:
    """
    Retrieving spot price data for selected CIS-2 tokens and all PLTs.

    Every token in the run's partition range that still has a price source is
    handled by this one process. The partitions themselves are unchanged, so a
    single token can still be re-run on its own from the UI; what changed is
    that the scheduled cycle no longer pays a process start, a definitions
    import and a Mongo client per token.

    Returns None deliberately: an IO manager cannot persist one output across
    several partitions, and nothing consumes the value -- the rates are the
    write to Mongo.
    """
    tokens, skipped = tokens_still_configured(context.partition_keys)
    if skipped:
        context.log.info(
            f"Skipping {len(skipped)} token(s) with no price source configured: "
            f"{', '.join(skipped)}"
        )
    if not tokens:
        context.log.info("No token in this run's range has a price source configured.")
        return

    mongodb = mongo_resource.get_client()
    written, failed = perform_spot_retrieval_update(context, tokens, mongodb)
    context.log.info(f"Stored {len(written)} of {len(tokens)} spot rates.")

    if failed:
        # The rates that did come back are already written; failing the run is
        # what makes a token that yielded nothing visible at all. Re-run just
        # that token on its own partition to retry it.
        raise dg.Failure(
            description=(
                f"No spot rate for {len(failed)} of {len(tokens)} tokens: {', '.join(failed)}"
            )
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
    # Sync the partitions here rather than listing them at import. Schedules
    # evaluate in the long-lived code server, so this query reuses one client
    # for the life of the server and run workers open nothing just by loading
    # the definitions. Keys are only ever added: deleting one would detach that
    # token's history. The range below therefore spans tokens that are no longer
    # configured too, and the asset drops those before fetching anything.
    partition_keys = current_token_keys()
    if not partition_keys:
        return dg.SkipReason("No tokens have a price source configured.")
    context.instance.add_dynamic_partitions(partitions_def_tokens.name, partition_keys)

    # One run covering every token, rather than one run per token. Twenty-odd
    # runs a cycle each cost more in process start than in work, and four at a
    # time is the whole queue -- they crowded out every other recurring job.
    # The range is expressed over the stored partition order, so it spans all
    # of them however many there are.
    stored_keys = context.instance.get_dynamic_partitions(partitions_def_tokens.name)
    return dg.RunRequest(
        run_key=context.scheduled_execution_time.isoformat(),
        tags={
            PARTITION_RANGE_START_TAG: stored_keys[0],
            PARTITION_RANGE_END_TAG: stored_keys[-1],
        },
    )


defs = dg.Definitions(
    assets=[spot_retrieval],
    jobs=[job],
    schedules=[schedule],
    resources={"mongo_resource": mongodb_resource_instance},
)
