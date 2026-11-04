"""Sentry reporting for the nightrunner, keyed on the asset that failed.

Until this existed the nightrunner reported nothing to Sentry at all. A failure
surfaced as a Dagster alert naming only the job -- and the jobs here are wide:
`j_from_genesis` alone selects fourteen assets. So "j_from_genesis failed" told
you a nightly batch had broken somewhere, and finding out where meant opening
Mongo and comparing the freshness of fourteen statistics by hand, after the
fact, to work out which one had stopped being written.

What makes that worth fixing is that these assets are computed once per date
and then read forever. A failure nobody localises is a permanently missing
number, and the only evidence is a gap.

Two decisions worth keeping:

* **The report names assets, and the issue is grouped by them.** The
  fingerprint is the job plus the failed assets, never the run id. Dagster puts
  a fresh run id in every failure message, and Sentry groups by message, so
  reporting these without a fingerprint opens a new issue per run. That is not
  a guess: the celery project held 163 unresolved issues on 2026-10-11, nearly
  all of them one run id each -- receipts for runs that cannot recur, burying
  the few real faults. The run id stays in the tags and the context, where it
  is useful for looking the run up and useless for grouping.

* **Log records do not become events.** `LoggingIntegration(event_level=None)`
  switches off the SDK's default "every ERROR record is an event" behaviour, so
  the only things sent from this process are the failures this sensor reports.
  celery_app/core.py takes the other route -- it lets log capture stand and
  repairs the grouping in `before_send` with a regex over the message. One
  deliberate event per failed run needs no regex and cannot drift out of step
  with Dagster's log formatting.
"""

from __future__ import annotations

import dagster as dg
import sentry_sdk
from ccdexplorer.env import environment, sentry_dsn
from pydantic import BaseModel
from sentry_sdk.integrations.logging import LoggingIntegration

#: Set once per process. sentry_sdk.init is global and re-initialising it
#: replaces the client, so the sensor must not do it on every tick.
_initialised = False


class FailureReport(BaseModel):
    """Everything sent to Sentry about one failed run.

    A model rather than a pile of locals so the decisions above can be tested
    without building a Dagster run, an instance and an event log.
    """

    job_name: str
    run_id: str
    assets: list[str]
    steps: list[str]
    partition: str | None
    error: str

    @property
    def title(self) -> str:
        """What the issue is called in Sentry.

        The asset leads, because that is the thing that did not get written.
        The job is context -- one job covers up to fourteen assets, so on its
        own it does not identify anything.
        """
        where = ", ".join(self.assets) or self.job_name
        suffix = f" [{self.partition}]" if self.partition else ""
        return f"nightrunner: {where}{suffix} failed"

    @property
    def fingerprint(self) -> list[str]:
        """The grouping key: job and assets, never the run id or the partition.

        The partition is excluded deliberately. `j_from_genesis` is partitioned
        by date, so including it would open a new issue every night an asset
        stays broken -- the same explosion the run id causes, one day at a time.
        One issue per broken asset, counting occurrences, is the thing worth
        being paged about.
        """
        return ["nightrunner", self.job_name, *self.assets]


def assets_for(asset_selection, step_keys: list[str]) -> list[str]:
    """The asset names a failed run was producing.

    Step keys first: they name the steps that actually failed, so on a job of
    fourteen assets they are the answer. `asset_selection` is the whole
    selection and is the fallback for a run that died before any step reported
    -- a resource that cannot connect, say, which fails the run with no step
    failure at all. Sorted, because the fingerprint is built from this and a
    set's iteration order would split one fault across several issues.
    """
    if step_keys:
        return sorted(set(step_keys))
    return sorted(key.to_user_string() for key in (asset_selection or ()))


def init_sentry() -> bool:
    """Arm Sentry for this process. Returns whether it has anywhere to send.

    Called from the definitions module, so both the code server and every run
    worker it spawns are covered -- a run worker is a separate process and
    inherits no client.
    """
    global _initialised
    if _initialised:
        return sentry_sdk.get_client().transport is not None

    # environment["SENTRY_DSN"] rather than a DSN written into this source: the
    # api reads it the same way, and a DSN in the source is what made the
    # celery project unswitchable-off during a test run. Unset means Sentry
    # stays off and this returns False -- the sensor still logs.
    sentry_sdk.init(
        dsn=sentry_dsn(environment["SENTRY_DSN"]),
        environment=environment["SENTRY_ENVIRONMENT"],
        # See the module docstring: the sensor is the only source of events.
        integrations=[LoggingIntegration(event_level=None)],
        # No send_default_pii. Nothing here handles a request or a user.
    )
    _initialised = True
    return sentry_sdk.get_client().transport is not None


def report(failure: FailureReport) -> None:
    """Send one failure, grouped by asset.

    An isolation scope rather than the current one: the sensor runs in the
    long-lived daemon, and tags set on the global scope would leak onto every
    later event from the same process.
    """
    with sentry_sdk.isolation_scope() as scope:
        scope.set_fingerprint(failure.fingerprint)
        scope.set_tag("dagster.job", failure.job_name)
        scope.set_tag("dagster.run_id", failure.run_id)
        if failure.partition:
            scope.set_tag("dagster.partition", failure.partition)
        if failure.assets:
            scope.set_tag("dagster.asset", failure.assets[0])
        scope.set_context(
            "dagster",
            {
                "job": failure.job_name,
                "run_id": failure.run_id,
                "assets": failure.assets,
                "steps": failure.steps,
                "partition": failure.partition,
            },
        )
        scope.set_extra("error", failure.error)
        sentry_sdk.capture_message(failure.title, level="error")


@dg.run_failure_sensor(
    name="report_failures_to_sentry",
    # Only this code location. The nightrunner, paydays and recurring each run
    # their own daemon; monitoring all of them from here would report another
    # location's failures against the nightrunner's assets.
    monitor_all_code_locations=False,
)
def report_failures_to_sentry(context: dg.RunFailureSensorContext) -> None:
    """Report every failed run, naming the assets that did not get written."""
    step_events = context.get_step_failure_events()
    failure = FailureReport(
        job_name=context.dagster_run.job_name,
        run_id=context.dagster_run.run_id,
        assets=assets_for(
            context.dagster_run.asset_selection,
            [event.step_key for event in step_events if event.step_key],
        ),
        steps=sorted({event.step_key for event in step_events if event.step_key}),
        partition=context.partition_key,
        error=context.failure_event.message or "no message on the failure event",
    )

    # Logged as well as sent. Dagster's own event log is where someone looking
    # at the failed run will be, and it is the only trail when no DSN is
    # configured -- init_sentry() then leaves the client with no transport and
    # capture_message is silently a no-op.
    context.log.error(f"{failure.title}: {failure.error}")
    report(failure)
