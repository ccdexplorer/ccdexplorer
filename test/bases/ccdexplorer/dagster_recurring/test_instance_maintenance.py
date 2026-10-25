from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg

from ccdexplorer.dagster_recurring.src.instance_maintenance import (
    _failed_jobs_by_location,
    _failed_runs_filter,
    _format_daily_summary,
    _run_matches_location,
)


def make_run_record(
    *,
    job_name: str,
    location_name: str | None = None,
    repository_label: str | None = None,
):
    remote_job_origin = (
        None if location_name is None else SimpleNamespace(location_name=location_name)
    )
    tags = {}
    if repository_label is not None:
        tags[".dagster/repository"] = repository_label

    dagster_run = SimpleNamespace(
        job_name=job_name,
        remote_job_origin=remote_job_origin,
        tags=tags,
    )
    return SimpleNamespace(dagster_run=dagster_run)


def test_run_matches_location_with_remote_origin():
    run_record = make_run_record(job_name="j_example", location_name="NIGHTRUNNER")
    assert _run_matches_location(run_record, "nightrunner")
    assert not _run_matches_location(run_record, "paydays")


def test_run_matches_location_with_repository_label():
    run_record = make_run_record(
        job_name="j_example",
        repository_label="defs@dagster_paydays",
    )
    assert _run_matches_location(run_record, "paydays")
    assert not _run_matches_location(run_record, "recurring")


def test_failed_jobs_by_location_groups_failures():
    records = [
        make_run_record(job_name="j_a", location_name="NIGHTRUNNER"),
        make_run_record(job_name="j_a", location_name="NIGHTRUNNER"),
        make_run_record(job_name="j_b", location_name="PAYDAYS"),
        make_run_record(job_name="j_ignore", location_name="RECURRING"),
    ]
    grouped = _failed_jobs_by_location(records)
    assert grouped["nightrunner"] == Counter({"j_a": 2})
    assert grouped["paydays"] == Counter({"j_b": 1})


def test_format_daily_summary_all_ok():
    start = datetime(2026, 2, 15, 6, 0, tzinfo=UTC)
    end = datetime(2026, 2, 16, 6, 0, tzinfo=UTC)
    summary = _format_daily_summary(
        start,
        end,
        {"nightrunner": Counter(), "paydays": Counter()},
    )
    assert "all nightrunner and paydays tasks are OK" in summary
    assert "failures detected" not in summary


def test_format_daily_summary_with_failures():
    start = datetime(2026, 2, 15, 6, 0, tzinfo=UTC)
    end = datetime(2026, 2, 16, 6, 0, tzinfo=UTC)
    summary = _format_daily_summary(
        start,
        end,
        {
            "nightrunner": Counter({"j_from_genesis": 2}),
            "paydays": Counter({"j_payday_daily": 1}),
        },
    )
    assert "failures detected" in summary
    assert "j_from_genesis (2 failed run(s))" in summary
    assert "j_payday_daily (1 failed run(s))" in summary


# --- which runs the summary is allowed to count ---------------------------
#
# The helpers above were tested; the query that feeds them was not, which is
# how this got out. The summary asked for every FAILURE created in the window
# and got failed *backfill* runs among them -- historical re-runs of
# j_from_genesis, a ~1950-partition job nothing schedules and that exists to
# be backfilled. Dagster files those under Backfills rather than Runs, so the
# summary reported a failure for a night in which the Runs page showed none.


def test_the_filter_excludes_backfill_runs():
    """exclude_subruns is WHERE backfill_id IS NULL. Without it a backfill
    failure is indistinguishable from last night's job failing."""
    assert _failed_runs_filter(datetime(2026, 10, 20, 6, 0, tzinfo=UTC)).exclude_subruns is True


def test_the_filter_still_bounds_the_window():
    """Dropping backfills must not quietly widen the window to all time."""
    start = datetime(2026, 10, 20, 6, 0, tzinfo=UTC)

    assert _failed_runs_filter(start).created_after == start


def test_the_filter_still_asks_only_for_failures():
    assert list(_failed_runs_filter(datetime(2026, 10, 20, 6, 0, tzinfo=UTC)).statuses) == [
        dg.DagsterRunStatus.FAILURE
    ]


def test_the_filter_does_not_pin_a_job_name():
    """Every nightrunner and paydays job is in scope, not one of them."""
    assert _failed_runs_filter(datetime(2026, 10, 20, 6, 0, tzinfo=UTC)).job_name is None


def _invoke_summary(returned_records):
    """Drive the asset itself, capturing the filter it queries with.

    Through the real asset rather than the helper alone: a correct helper the
    asset does not use would leave the bug exactly where it was.
    """
    from ccdexplorer.dagster_recurring.src import instance_maintenance

    captured = {}

    class _Instance:
        def get_run_records(self, filters):
            captured["filters"] = filters
            return returned_records

    class _Tooter:
        def send(self, channel=None, message=None, notifier_type=None):
            captured["message"] = message

    class _TooterResource:
        def get_client(self):
            return _Tooter()

    fn = instance_maintenance.daily_execution_summary.op.compute_fn.decorated_fn
    captured["returned"] = fn(SimpleNamespace(instance=_Instance()), _TooterResource())
    return captured


def test_the_asset_queries_with_backfills_excluded():
    """The helper being right is worth nothing if the asset builds its own."""
    captured = _invoke_summary([])

    assert captured["filters"].exclude_subruns is True


def test_a_night_with_nothing_failed_is_reported_as_ok():
    """What the backfill-only night should have said all along."""
    captured = _invoke_summary([])

    assert "all nightrunner and paydays tasks are OK" in captured["message"]
    assert "failures detected" not in captured["message"]
    assert captured["returned"]["total_failures"] == 0


def test_a_real_nightrunner_failure_is_still_reported():
    """The fix narrows what counts; it must not mute what should."""
    captured = _invoke_summary(
        [make_run_record(job_name="j_from_genesis", location_name="NIGHTRUNNER")]
    )

    assert "failures detected" in captured["message"]
    assert "j_from_genesis (1 failed run(s))" in captured["message"]
    assert captured["returned"]["total_failures"] == 1
