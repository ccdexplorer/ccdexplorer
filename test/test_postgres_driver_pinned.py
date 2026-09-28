"""Dagster's Postgres storage must keep resolving to psycopg2.

ko-recurring failed to start on 2026-09-28 with

    dagster_postgres/run_storage/run_storage.py -> create_pg_engine
    sqlalchemy/dialects/postgresql/psycopg.py:497 in import_dbapi
    ModuleNotFoundError: No module named 'psycopg'

SQLAlchemy 2.1 changed which DBAPI a bare ``postgresql://`` url means: it was
psycopg2, it is now psycopg (v3). dagster-postgres still ships only
psycopg2-binary -- every version up to 0.29.24 -- and its retry logic catches
psycopg2 exception classes by name, so psycopg2 is the driver it actually
supports. Nothing in this repo asks for psycopg3; SQLAlchemy 2.1 picked it.

uv.lock pins sqlalchemy 2.0.44, which is why this never failed locally. The
image does not get that pin: the Dockerfiles run `uv pip install -e
./projects/<name>` after `uv sync`, and that re-resolves the project's own
dependencies from the index, so a fresh build took 2.1.1.

Two tests, because the constraint and the environment can drift apart: one reads
what we declare, the other checks what is actually installed.
"""

import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

REPO = Path(__file__).resolve().parent.parent


def _pyprojects_needing_postgres() -> list[Path]:
    found = []
    for path in [REPO / "pyproject.toml", *sorted((REPO / "projects").glob("*/pyproject.toml"))]:
        deps = tomllib.loads(path.read_text()).get("project", {}).get("dependencies", [])
        if any(Requirement(d).name == "dagster-postgres" for d in deps):
            found.append(path)
    return found


def test_the_installed_postgresql_dialect_is_psycopg2():
    """What the container actually got wrong, asserted directly."""
    from sqlalchemy.engine.url import make_url

    assert make_url("postgresql://u:p@h:5432/db").get_dialect().driver == "psycopg2"


def test_the_installed_sqlalchemy_predates_the_dialect_change():
    import sqlalchemy

    assert Version(sqlalchemy.__version__) < Version("2.1"), (
        f"sqlalchemy {sqlalchemy.__version__} defaults postgresql:// to psycopg (v3), "
        "which dagster-postgres does not install"
    )


def test_there_is_something_to_check():
    assert _pyprojects_needing_postgres(), "no pyproject depends on dagster-postgres"


@pytest.mark.parametrize(
    "path", _pyprojects_needing_postgres(), ids=lambda p: str(p.parent.name)
)
def test_every_postgres_project_constrains_sqlalchemy(path):
    """A declared bound, so an image build that re-resolves still gets psycopg2.

    The pin belongs next to dagster-postgres in every project that ships it,
    not only in uv.lock, because the build step that broke this ignores the lock.
    """
    deps = tomllib.loads(path.read_text())["project"]["dependencies"]
    reqs = {Requirement(d).name: Requirement(d) for d in deps}

    assert "sqlalchemy" in reqs, (
        f"{path.relative_to(REPO)} depends on dagster-postgres but does not bound sqlalchemy; "
        "a fresh resolve takes 2.1 and loses psycopg2"
    )
    assert reqs["sqlalchemy"].specifier.contains("2.0.44"), "2.0.x must stay allowed"
    assert not reqs["sqlalchemy"].specifier.contains("2.1.1"), "2.1 must be excluded"
