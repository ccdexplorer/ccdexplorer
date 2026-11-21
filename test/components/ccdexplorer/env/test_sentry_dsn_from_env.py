"""A Sentry DSN comes from the environment of the process, never from source.

Two DSNs were written into the source of a public repository: the celery one
in celery_app/core.py and the bot's. Anyone who ran that code anywhere reported
into the project -- on 2026-11-21 two issues arrived from machines called
`trial-...-terminal-...`, running a copy from /tmp/gt-independent-audit, tagged
`production` because the code said so. `sentry_dsn()` could not stop them: it
only switches off for pytest and SENTRY_DISABLED, neither of which a stranger
sets.

A copy of the code with no DSN in its environment now reports nowhere.
"""

import re
import sys
from pathlib import Path

import pytest

from ccdexplorer.env import sentry_dsn_from_env

DSN = "https://0123456789abcdef0123456789abcdef@o1.ingest.us.sentry.io/42"
REPO = Path(__file__).resolve().parents[4]


@pytest.fixture
def outside_pytest(monkeypatch):
    """sentry_dsn() answers "" whenever pytest is loaded; take that out of play."""
    monkeypatch.delitem(sys.modules, "pytest")
    monkeypatch.delenv("SENTRY_DISABLED", raising=False)


def test_a_dsn_in_the_environment_is_used(outside_pytest, monkeypatch):
    monkeypatch.setenv("SENTRY_DSN_X", DSN)

    assert sentry_dsn_from_env("SENTRY_DSN_X") == DSN


def test_no_dsn_in_the_environment_means_off_not_none(outside_pytest, monkeypatch):
    """None would not switch anything off: sentry_sdk.init(dsn=None) reads
    SENTRY_DSN itself, and a container that carries another service's DSN
    would report to that project."""
    monkeypatch.delenv("SENTRY_DSN_X", raising=False)
    monkeypatch.setenv("SENTRY_DSN", DSN)

    assert sentry_dsn_from_env("SENTRY_DSN_X") == ""


def test_an_empty_dsn_is_off(outside_pytest, monkeypatch):
    monkeypatch.setenv("SENTRY_DSN_X", "")

    assert sentry_dsn_from_env("SENTRY_DSN_X") == ""


def test_a_dsn_in_the_environment_is_still_switched_off_by_sentry_disabled(
    outside_pytest, monkeypatch
):
    monkeypatch.setenv("SENTRY_DSN_X", DSN)
    monkeypatch.setenv("SENTRY_DISABLED", "1")

    assert sentry_dsn_from_env("SENTRY_DSN_X") == ""


def test_a_dsn_in_the_environment_is_switched_off_under_pytest(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN_X", DSN)

    assert sentry_dsn_from_env("SENTRY_DSN_X") == ""


def test_a_missing_dsn_says_so_once_it_matters(outside_pytest, monkeypatch, capsys):
    """Silence from Sentry looks like a healthy service. A process that comes
    up without a DSN says so, where the container log will show it."""
    monkeypatch.delenv("SENTRY_DSN_X", raising=False)

    sentry_dsn_from_env("SENTRY_DSN_X")

    assert "SENTRY_DSN_X" in capsys.readouterr().out


def test_no_dsn_is_written_into_the_source():
    pattern = re.compile(r"https://[0-9a-f]{32}@[\w.-]*sentry\.io")
    found = [
        str(path.relative_to(REPO))
        for root in ("bases", "components")
        for path in (REPO / root).rglob("*.py")
        if pattern.search(path.read_text(errors="replace"))
    ]

    assert found == []


@pytest.mark.parametrize(
    "path,variable",
    [
        ("components/ccdexplorer/celery_app/core.py", "SENTRY_DSN_CELERY"),
        ("bases/ccdexplorer/ccdexplorer_bot/__main__.py", "SENTRY_DSN"),
    ],
)
def test_the_two_that_were_hardcoded_read_their_environment(path, variable):
    assert f'sentry_dsn_from_env("{variable}")' in (REPO / path).read_text()
