"""A test run must never reach Sentry.

On 2026-09-27 between 20:58 and 21:55 a test run in a container created 77 new
issues on the celery-ccdexplorer-io project. They are recognisable by their
url tag -- `http://testserver`, which is what conftest sets API_URL to and what
Starlette's TestClient sends -- and by a pytest collection failure,
`ModuleNotFoundError: No module named 'test.bases'`, reported through the
global excepthook.

conftest sets SENTRY_DSN="" before importing anything, but that only reaches the
two call sites that read `environment["SENTRY_DSN"]`. celery_app/core.py called
sentry_sdk.init() at import time with the DSN written into the source, so
nothing in the environment could switch it off, and one import of anything
touching ccdexplorer.celery_app turned Sentry on for the whole process --
excepthook included, which is how a collection error became a production issue.

These tests are the guard. The first one is the whole point: whatever this suite
imports, no client may be live.
"""

import ast
import os
import sys
from pathlib import Path

import pytest
import sentry_sdk

REPO = Path(__file__).resolve().parent.parent


def test_no_sentry_client_is_active_in_this_process():
    """The assertion that actually protects production Sentry.

    conftest has already imported the app factories, the bot and the mongo
    layer by the time this runs, so if any of them switched Sentry on, this
    fails.
    """
    client = sentry_sdk.get_client()

    # Not is_active(): sentry_sdk.init() always yields a _Client, and that
    # reports active whether or not it has anywhere to send. A client with no
    # transport is the one that cannot reach Sentry, which is what matters here.
    assert client.transport is None, f"Sentry is live during tests, sending to {client.dsn}"


def test_importing_the_celery_app_does_not_switch_sentry_on():
    """The specific import that did it."""
    import ccdexplorer.celery_app  # noqa: F401

    client = sentry_sdk.get_client()

    assert client.transport is None, f"importing ccdexplorer.celery_app armed Sentry ({client.dsn})"


REAL_LOOKING = "https://key@o0.ingest.us.sentry.io/0"


@pytest.fixture
def sentry_left_off():
    """Re-disable the global client after a test that initialises one.

    sentry_sdk.init mutates process-wide state, so a test that arms a client
    would otherwise hand a live one to every test after it -- which is the very
    thing this file exists to prevent.
    """
    yield
    sentry_sdk.init(dsn="")


def test_the_guard_says_off_while_pytest_is_loaded():
    from ccdexplorer.env import sentry_dsn

    assert sentry_dsn(REAL_LOOKING) == ""


def test_the_guard_says_off_when_explicitly_disabled(monkeypatch):
    """The env var, not just sys.modules -- a subprocess inherits one, not the other.

    The collection error was reported from `<string>:11` through excepthook,
    which is a separate interpreter; `pytest in sys.modules` cannot see it.
    """
    from ccdexplorer.env import sentry_dsn

    monkeypatch.delitem(sys.modules, "pytest", raising=False)
    monkeypatch.setenv("SENTRY_DISABLED", "1")

    assert sentry_dsn(REAL_LOOKING) == ""


def test_a_dsn_in_the_environment_cannot_defeat_the_guard(monkeypatch, sentry_left_off):
    """Why the guard returns "" and not None.

    sentry_sdk.init(dsn=None) reads SENTRY_DSN from the environment itself and
    comes up with a working transport. A container that has a real DSN set --
    which a deployed image plausibly does -- would therefore report a whole test
    run, even though every call site had been told not to. An empty string is
    what blocks that fallback.
    """
    from ccdexplorer.env import sentry_dsn

    monkeypatch.setenv("SENTRY_DSN", REAL_LOOKING)

    sentry_sdk.init(dsn=sentry_dsn(REAL_LOOKING))

    assert sentry_sdk.get_client().transport is None


def test_none_would_not_have_been_enough(monkeypatch, sentry_left_off):
    """Pins the SDK behaviour the guard is written against, so a change is noticed."""
    monkeypatch.setenv("SENTRY_DSN", REAL_LOOKING)

    sentry_sdk.init(dsn=None)

    assert sentry_sdk.get_client().transport is not None, (
        "sentry_sdk no longer falls back to the SENTRY_DSN env var; "
        "the guard could now return None instead of an empty string"
    )


def test_the_guard_hands_back_the_dsn_in_production(monkeypatch):
    """It must not disable Sentry where Sentry is the point."""
    from ccdexplorer.env import sentry_dsn

    monkeypatch.delenv("SENTRY_DISABLED", raising=False)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    assert sentry_dsn(REAL_LOOKING) == REAL_LOOKING


def _sentry_init_calls():
    """Every real sentry_sdk.init(...) call, found by parsing rather than grepping.

    Prose matters here: this file and settings.py both discuss
    `sentry_sdk.init(dsn=None)` in docstrings, and a text scan counts those as
    call sites.
    """
    found = []
    for directory in ("bases", "components"):
        for path in sorted((REPO / directory).rglob("*.py")):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                if not (isinstance(func, ast.Attribute) and func.attr == "init"):
                    continue
                if not (isinstance(func.value, ast.Name) and func.value.id == "sentry_sdk"):
                    continue
                found.append((path, node))
    return found


def test_every_init_site_routes_its_dsn_through_the_guard():
    """So a new hardcoded DSN cannot quietly reintroduce this.

    celery_app/core.py passed a DSN written into its own source straight to
    init, which is why nothing in the environment could switch it off.
    """
    calls = _sentry_init_calls()
    assert calls, "found no sentry_sdk.init call sites -- the search is wrong"

    offenders = []
    for path, call in calls:
        dsn = next((kw.value for kw in call.keywords if kw.arg == "dsn"), None)
        guarded = (
            isinstance(dsn, ast.Call)
            and isinstance(dsn.func, ast.Name)
            and dsn.func.id == "sentry_dsn"
        )
        if not guarded:
            offenders.append(
                f"{path.relative_to(REPO)}:{call.lineno} dsn={ast.unparse(dsn) if dsn else '<missing>'}"[
                    :100
                ]
            )

    assert not offenders, "sentry_sdk.init not routed through sentry_dsn():\n  " + "\n  ".join(
        offenders
    )


@pytest.mark.parametrize("var", ["SENTRY_DISABLED"])
def test_conftest_sets_the_subprocess_guard(var):
    """conftest must export it, so anything it spawns is covered too."""
    assert os.environ.get(var) == "1"
