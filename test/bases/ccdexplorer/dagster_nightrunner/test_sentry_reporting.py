"""The nightrunner's failure reports name the asset, and group by it.

Before this the nightrunner sent Sentry nothing. A failure arrived as an alert
naming only the job, and `j_from_genesis` selects fourteen assets -- so working
out which number had stopped being written meant comparing statistics freshness
in Mongo by hand.

The grouping half matters as much as the naming half. Dagster puts a fresh run
id in every failure message and Sentry groups by message, so reporting these
without a fingerprint opens one issue per run: the celery project held 163
unresolved issues on 2026-10-11, nearly all of them a single run id each.
"""

from ccdexplorer.dagster_nightrunner.src._sentry import FailureReport, assets_for


class _Key:
    """Stands in for dagster.AssetKey, which is all `assets_for` uses of it."""

    def __init__(self, name: str):
        self._name = name

    def to_user_string(self) -> str:
        return self._name


def _report(**overrides) -> FailureReport:
    fields = {
        "job_name": "j_from_genesis",
        "run_id": "8e3cd456-0000-0000-0000-f8a8078b222d",
        "assets": ["tx_fees"],
        "steps": ["tx_fees"],
        "partition": "2026-10-30",
        "error": "boom",
    }
    fields.update(overrides)
    return FailureReport(**fields)


# --- which asset failed ----------------------------------------------------


def test_the_failed_steps_name_the_assets():
    """The point of the change. One of fourteen selected assets failed, and the
    report has to say which one rather than naming the job."""
    got = assets_for([_Key("tx_fees"), _Key("daily_holders")], ["tx_fees"])

    assert got == ["tx_fees"]


def test_the_selection_is_the_fallback_when_no_step_failed():
    """A run can die before any step reports -- a resource that cannot connect
    fails the run with no step failure at all. Naming the whole selection is
    worse than naming one asset and much better than naming nothing."""
    got = assets_for([_Key("tx_fees"), _Key("daily_holders")], [])

    assert got == ["daily_holders", "tx_fees"]


def test_no_assets_at_all_is_not_an_error():
    """An op job, or a run whose selection never resolved."""
    assert assets_for(None, []) == []


def test_the_asset_order_is_stable():
    """The fingerprint is built from this list, so an unstable order would
    split one fault across several Sentry issues."""
    first = assets_for(None, ["tx_types", "tx_fees", "microccd"])
    second = assets_for(None, ["microccd", "tx_fees", "tx_types"])

    assert first == second == ["microccd", "tx_fees", "tx_types"]


# --- grouping --------------------------------------------------------------


def test_the_run_id_is_not_in_the_fingerprint():
    """The whole reason the celery project accumulated 163 issues."""
    fingerprint = _report().fingerprint

    assert not any("8e3cd456" in part for part in fingerprint)


def test_two_runs_of_the_same_broken_asset_group_together():
    one = _report(run_id="aaaaaaaa-0000-0000-0000-000000000001").fingerprint
    two = _report(run_id="bbbbbbbb-0000-0000-0000-000000000002").fingerprint

    assert one == two


def test_the_partition_is_not_in_the_fingerprint():
    """These jobs are partitioned by date, so a partition in the key would open
    a fresh issue every night an asset stayed broken."""
    monday = _report(partition="2026-10-30").fingerprint
    tuesday = _report(partition="2026-10-31").fingerprint

    assert monday == tuesday


def test_two_different_broken_assets_do_not_group_together():
    """The other half: one issue per fault, not one issue for the job."""
    fees = _report(assets=["tx_fees"]).fingerprint
    holders = _report(assets=["daily_holders"]).fingerprint

    assert fees != holders


# --- what the issue is called ----------------------------------------------


def test_the_title_leads_with_the_asset():
    assert _report().title.startswith("nightrunner: tx_fees")


def test_the_title_carries_the_partition():
    """Which date did not get written is the next thing anyone asks."""
    assert "[2026-10-30]" in _report().title


def test_the_title_falls_back_to_the_job_when_no_asset_is_known():
    title = _report(assets=[], partition=None).title

    assert title == "nightrunner: j_from_genesis failed"


# --- the report is complete -------------------------------------------------


def test_the_run_id_is_still_reported_somewhere():
    """Excluded from the grouping, not from the event -- it is how you find the
    run in Dagster."""
    assert _report().run_id == "8e3cd456-0000-0000-0000-f8a8078b222d"


def test_sentry_stays_off_without_a_dsn():
    """init_sentry must be safe to call in any environment, including a test
    run and a container with no DSN configured."""
    import sentry_sdk

    from ccdexplorer.dagster_nightrunner.src._sentry import init_sentry

    assert init_sentry() is False
    assert sentry_sdk.get_client().transport is None
