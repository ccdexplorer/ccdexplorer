"""One Dagster failure should be one Sentry issue, not one per run.

sentry_sdk.init here takes no `integrations`, so the default
LoggingIntegration captures every ERROR log record as an event. Dagster
logs its run and step failures as ERROR, and the message carries the run
id:

    j_spot_retrieval - 8e3cd456-ac52-42b8-95c8-f8a8078b222d - 1217035 -
    RUN_FAILURE - Execution of run for "j_spot_retrieval" failed.

Sentry groups log events by message, so every run mints a new issue. On
2026-10-11 the celery project held 163 unresolved issues that had not been
seen in a fortnight, almost all of them one run id each -- receipts for
runs that can never happen again, burying the handful of real faults.
"""

from ccdexplorer.celery_app.core import dagster_fingerprint

RUN = "8e3cd456-ac52-42b8-95c8-f8a8078b222d"
OTHER_RUN = "e3604f52-111c-4548-9832-d84e06a9aae6"


def test_two_runs_of_the_same_failure_group_together():
    first = dagster_fingerprint(
        f'j_spot_retrieval - {RUN} - 1217035 - RUN_FAILURE - Execution of run for "x" failed.'
    )
    second = dagster_fingerprint(
        f'j_spot_retrieval - {OTHER_RUN} - 994431 - RUN_FAILURE - Execution of run for "x" failed.'
    )

    assert first is not None
    assert first == second


def test_the_job_still_separates_issues():
    """A spot retrieval failure and a market position failure are two
    different problems and must not collapse into one."""
    spot = dagster_fingerprint(f"j_spot_retrieval - {RUN} - 1 - RUN_FAILURE - failed.")
    market = dagster_fingerprint(f"j_market_position - {RUN} - 1 - RUN_FAILURE - failed.")

    assert spot != market


def test_a_step_failure_is_not_a_run_failure():
    """Dagster reports both for one fault; they are still distinguishable."""
    run = dagster_fingerprint(f"j_spot_retrieval - {RUN} - 1 - RUN_FAILURE - failed.")
    step = dagster_fingerprint(
        f'j_spot_retrieval - {RUN} - 1 - spot_retrieval - STEP_FAILURE - step "x" failed.'
    )

    assert run != step


def test_two_steps_of_one_job_stay_apart():
    a = dagster_fingerprint(f"j_x - {RUN} - 1 - step_one - STEP_FAILURE - failed.")
    b = dagster_fingerprint(f"j_x - {RUN} - 1 - step_two - STEP_FAILURE - failed.")

    assert a != b


def test_a_message_with_no_run_id_is_left_alone():
    """Not a Dagster event, so Sentry's own grouping is the right one."""
    assert dagster_fingerprint("ConnectionError: cannot reach tooter.ccdexplorer.io") is None


def test_an_empty_message_is_left_alone():
    assert dagster_fingerprint("") is None
    assert dagster_fingerprint(None) is None


def test_the_run_id_is_gone_from_the_key():
    """If it survives anywhere in the fingerprint the grouping is unchanged."""
    key = dagster_fingerprint(f"j_spot_retrieval - {RUN} - 1217035 - RUN_FAILURE - failed.")

    assert RUN not in " ".join(key)
    assert "1217035" not in " ".join(key)


def test_the_key_says_it_is_dagster():
    """So a human reading the fingerprint in Sentry knows why it is grouped
    the way it is."""
    key = dagster_fingerprint(f"j_x - {RUN} - 1 - RUN_FAILURE - failed.")

    assert key[0] == "dagster"


def test_the_prose_after_the_step_does_not_split_the_group():
    """The whole point. These two are "spot_retrieval could not price a
    token" twice over -- one problem with two examples. Keeping the message
    in the key gave 55 groups for the 68 Dagster lines open on 2026-10-11;
    dropping it gives 13."""
    umb = dagster_fingerprint(
        f"j_spot_retrieval - {RUN} - spot_retrieval - Recurring: No spot rate for UMB (403)."
    )
    bnb = dagster_fingerprint(
        f"j_spot_retrieval - {OTHER_RUN} - spot_retrieval - Recurring: No spot rate for BNB (403)."
    )

    assert umb == bnb == ["dagster", "j_spot_retrieval", "spot_retrieval"]


def test_the_key_stays_short_enough_to_read():
    """A fingerprint is shown in the Sentry UI; a paragraph is not a key."""
    key = dagster_fingerprint(
        f"j_x - {RUN} - 1 - step - STEP_FAILURE - a very long explanation that goes on and on"
    )

    assert len(key) == 3
