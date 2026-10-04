"""A project must declare every component its code imports.

A polylith project's pyproject lists the bricks copied into its build. A
component that is imported but not listed works in the monorepo -- where
everything is on the path -- and is simply absent from the deployed image,
so the failure is an ImportError at container start rather than anything a
test run would show.

`uv run poly check` catches this, but it is not in the pre-commit hook, so
the one that matters is pinned here.
"""

import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

#: Which project each base is deployed in.
PROJECT_FOR_BASE = {
    "ccdexplorer_chart_bot": "ccdexplorer_chart_bot",
    "ccdexplorer_site": "ccdexplorer_site",
    "ccdexplorer_api": "ccdexplorer_api",
}

IMPORT = re.compile(r"^\s*(?:from|import)\s+ccdexplorer\.([a-z_]+)", re.M)


def _declared(project: str) -> set[str]:
    data = tomllib.loads((ROOT / "projects" / project / "pyproject.toml").read_text())
    bricks = data.get("tool", {}).get("polylith", {}).get("bricks", {})
    return {Path(k).name for k in bricks}


def _imported(base: str) -> set[str]:
    components = {p.name for p in (ROOT / "components" / "ccdexplorer").iterdir() if p.is_dir()}
    found = set()
    for path in (ROOT / "bases" / "ccdexplorer" / base).rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        found |= set(IMPORT.findall(path.read_text())) & components
    return found


@pytest.mark.parametrize("base,project", sorted(PROJECT_FOR_BASE.items()))
def test_every_imported_component_is_declared(base, project):
    missing = _imported(base) - _declared(project)
    assert not missing, (
        f"{base} imports {sorted(missing)} but projects/{project} does not "
        f"declare them -- they would be absent from the built image"
    )
