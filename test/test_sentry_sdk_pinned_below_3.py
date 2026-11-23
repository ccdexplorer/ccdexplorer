"""sentry-sdk stays below 3.

3.0.0 dropped `send_default_pii`, which the celery core, the bot and the
nightrunner pass to init(). The lock file holds 2.x, but the Dockerfiles run
`uv pip install -e ./projects/...` after the frozen sync, and that resolves
against the project's own specifier: with `>=2.41.0` it took 3.0.0 on the
next image build, and the rebuilt ms_modules container died on import with
"Unknown option 'send_default_pii'".
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PYPROJECTS = [ROOT / "pyproject.toml", *sorted((ROOT / "projects").glob("*/pyproject.toml"))]
SPEC = re.compile(r'"sentry-sdk(?:\[[^\]]*\])?([^"]*)"')


def _specifiers(path: Path) -> list[str]:
    return SPEC.findall(path.read_text())


@pytest.mark.parametrize("path", PYPROJECTS, ids=lambda p: str(p.relative_to(ROOT)))
def test_every_project_that_uses_sentry_stays_below_3(path):
    specs = _specifiers(path)
    if not specs:
        pytest.skip("does not depend on sentry-sdk")
    assert all("<3" in s.replace(" ", "") for s in specs), specs
