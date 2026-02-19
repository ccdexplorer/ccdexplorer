from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace

from ccdexplorer.dagster_recurring.src.instance_maintenance import (
    _failed_jobs_by_location,
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
        None
        if location_name is None
        else SimpleNamespace(location_name=location_name)
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
