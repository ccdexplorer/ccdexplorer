from collections import Counter
from datetime import UTC, datetime, timedelta
from html import escape

import dagster as dg
from ccdexplorer.tooter import TooterChannel, TooterType

from ._resources import TooterResource, tooter_resource_instance

LOOKBACK_HOURS = 24
RETENTION_DAYS = 7
SUMMARY_CRON = "0 6 * * *"
CLEANUP_CRON = "15 6 * * *"
TARGET_LOCATIONS = ("nightrunner", "paydays")
RECURRING_LOCATION = "recurring"
REPOSITORY_LABEL_TAG = ".dagster/repository"
RUNNING_STATUSES = {
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.MANAGED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
}


def _canonical_location_name(raw_name: str) -> str:
    normalized = raw_name.strip().lower().replace("-", "_")
    if normalized.startswith("dagster_"):
        return normalized.removeprefix("dagster_")
    return normalized


def _get_location_candidates(run_record: dg.RunRecord) -> set[str]:
    dagster_run = run_record.dagster_run
    candidates = set()

    if dagster_run.remote_job_origin and dagster_run.remote_job_origin.location_name:
        candidates.add(dagster_run.remote_job_origin.location_name)

    repository_label = dagster_run.tags.get(REPOSITORY_LABEL_TAG)
    if repository_label:
        if "@" in repository_label:
            _, location_name = repository_label.split("@", maxsplit=1)
            candidates.add(location_name)
        else:
            candidates.add(repository_label)

    return candidates


def _run_matches_location(run_record: dg.RunRecord, location_name: str) -> bool:
    target = _canonical_location_name(location_name)
    return any(
        _canonical_location_name(candidate) == target
        for candidate in _get_location_candidates(run_record)
    )


def _failed_jobs_by_location(run_records: list[dg.RunRecord]) -> dict[str, Counter[str]]:
    failed_by_location: dict[str, Counter[str]] = {location: Counter() for location in TARGET_LOCATIONS}
    for run_record in run_records:
        for location in TARGET_LOCATIONS:
            if _run_matches_location(run_record, location):
                failed_by_location[location][run_record.dagster_run.job_name] += 1
                break

    return failed_by_location


def _format_daily_summary(
    window_start: datetime, window_end: datetime, failed_by_location: dict[str, Counter[str]]
) -> str:
    has_failures = any(counter for counter in failed_by_location.values())

    lines = [
        "<b>Dagster Daily Summary</b>",
        f"Window: {window_start:%Y-%m-%d %H:%M} UTC to {window_end:%Y-%m-%d %H:%M} UTC",
    ]

    if not has_failures:
        lines.append("Status: all nightrunner and paydays tasks are OK.")
        return "<br/>".join(lines)

    lines.append("Status: failures detected.")
    for location in TARGET_LOCATIONS:
        failed_jobs = failed_by_location[location]
        if not failed_jobs:
            lines.append(f"{location}: OK")
            continue

        lines.append(f"{location}:")
        for job_name, failure_count in sorted(failed_jobs.items()):
            lines.append(f"&nbsp;&nbsp;- {escape(job_name)} ({failure_count} failed run(s))")

    return "<br/>".join(lines)


@dg.asset(group_name="maintenance")
def daily_execution_summary(
    context: dg.AssetExecutionContext,
    tooter_resource: dg.ResourceParam[TooterResource],
) -> dict:
    now = datetime.now(UTC)
    window_start = now - timedelta(hours=LOOKBACK_HOURS)

    failed_run_records = context.instance.get_run_records(
        filters=dg.RunsFilter(
            created_after=window_start,
            statuses=[dg.DagsterRunStatus.FAILURE],
        )
    )

    failed_by_location = _failed_jobs_by_location(list(failed_run_records))
    message = _format_daily_summary(window_start, now, failed_by_location)

    tooter = tooter_resource.get_client()
    tooter.send(
        channel=TooterChannel.NOTIFIER,
        message=message,
        notifier_type=TooterType.INFO,
    )

    total_failures = sum(sum(counter.values()) for counter in failed_by_location.values())
    return {
        "total_failures": total_failures,
        "nightrunner_failed_jobs": len(failed_by_location["nightrunner"]),
        "paydays_failed_jobs": len(failed_by_location["paydays"]),
        "window_start": window_start.isoformat(),
        "window_end": now.isoformat(),
    }


@dg.asset(group_name="maintenance")
def cleanup_recurring_run_history(
    context: dg.AssetExecutionContext,
) -> dict:
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=RETENTION_DAYS)
    deleted_runs = 0
    skipped_running_runs = 0

    old_run_records = context.instance.get_run_records(
        filters=dg.RunsFilter(created_before=cutoff),
    )

    for run_record in old_run_records:
        if not _run_matches_location(run_record, RECURRING_LOCATION):
            continue

        if run_record.dagster_run.status in RUNNING_STATUSES:
            skipped_running_runs += 1
            continue

        context.instance.delete_run(run_record.dagster_run.run_id)
        deleted_runs += 1

    return {
        "deleted_runs": deleted_runs,
        "skipped_running_runs": skipped_running_runs,
        "cutoff": cutoff.isoformat(),
    }


daily_execution_summary_job = dg.define_asset_job(
    name="j_daily_execution_summary",
    selection=[daily_execution_summary],
)

cleanup_recurring_run_history_job = dg.define_asset_job(
    name="j_cleanup_recurring_run_history",
    selection=[cleanup_recurring_run_history],
)


def _skip_if_job_is_running(
    context: dg.ScheduleEvaluationContext, job_name: str
) -> dg.SkipReason | None:
    running_records = context.instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=job_name,
            statuses=list(RUNNING_STATUSES),
        )
    )
    if running_records:
        return dg.SkipReason(
            f"Skipping schedule because another run of job '{job_name}' is still active."
        )

    return None


@dg.schedule(
    job=daily_execution_summary_job,
    cron_schedule=SUMMARY_CRON,
    execution_timezone="UTC",
    name="s_daily_execution_summary",
)
def summary_schedule(context: dg.ScheduleEvaluationContext):
    maybe_skip = _skip_if_job_is_running(context, daily_execution_summary_job.name)
    if maybe_skip:
        return maybe_skip
    return dg.RunRequest()


@dg.schedule(
    job=cleanup_recurring_run_history_job,
    cron_schedule=CLEANUP_CRON,
    execution_timezone="UTC",
    name="s_cleanup_recurring_run_history",
)
def recurring_cleanup_schedule(context: dg.ScheduleEvaluationContext):
    maybe_skip = _skip_if_job_is_running(context, cleanup_recurring_run_history_job.name)
    if maybe_skip:
        return maybe_skip
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[daily_execution_summary, cleanup_recurring_run_history],
    jobs=[daily_execution_summary_job, cleanup_recurring_run_history_job],
    schedules=[summary_schedule, recurring_cleanup_schedule],
    resources={"tooter_resource": tooter_resource_instance},
)
