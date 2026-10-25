"""Serving generated CSVs without serving everything else on the box.

GET /tmp/{filename} handed back any file in /tmp whose name a caller could
name, with no authentication. Traversal was blocked -- a path parameter
matches one segment, so ../etc/passwd never reached it -- but /tmp is shared
with every other process on the host, and the route could not tell this
application's exports from anyone else's temporary files.

Authentication is the wrong fix: these downloads are for all visitors, not
just signed-in ones. What was missing is that the url be the capability. One
consumer exists -- the track-and-trace link on the smart contract page -- and
its names already carried a uuid4, so the pattern was right and the directory
was wrong.

So exports move to a directory this application owns, created 0700, and the
serving route will not resolve outside it. A caller who guesses a name gets
404; a caller given a link gets their file; nobody gets /tmp.
"""

import os
import pathlib
import time

import pytest

from ccdexplorer.ccdexplorer_site.app import exports


# --- where exports live ---------------------------------------------------


def test_the_directory_is_not_shared_tmp():
    """The whole point: /tmp holds other processes' files."""
    assert exports.EXPORT_DIR != pathlib.Path("/tmp")
    assert pathlib.Path("/tmp") in exports.EXPORT_DIR.parents


def test_the_directory_is_created_private():
    exports.ensure_export_dir()

    assert exports.EXPORT_DIR.is_dir()
    assert oct(exports.EXPORT_DIR.stat().st_mode)[-3:] == "700"


def test_ensuring_it_twice_is_fine():
    exports.ensure_export_dir()
    exports.ensure_export_dir()

    assert exports.EXPORT_DIR.is_dir()


# --- the names ------------------------------------------------------------


def test_a_new_name_is_inside_the_export_directory():
    path = exports.new_export_path("staking_rewards - 72723")

    assert path.parent == exports.EXPORT_DIR


def test_two_names_for_the_same_thing_differ():
    """A guessable name is the whole vulnerability. The old
    staking_rewards name was account index plus month and nothing else."""
    a = exports.new_export_path("staking_rewards - 72723")
    b = exports.new_export_path("staking_rewards - 72723")

    assert a != b


def test_a_name_carries_enough_entropy_to_be_unguessable():
    """A uuid4, not range(1000,10000)|random -- which is what the templates
    use for browser save-as names and would be 9,000 guesses here."""
    name = exports.new_export_path("x").name

    assert len(name) > 36, name


def test_the_prefix_survives_so_a_download_is_recognisable():
    path = exports.new_export_path("staking_rewards - 72723")

    assert path.name.startswith("staking_rewards - 72723")
    assert path.suffix == ".csv"


def test_a_prefix_cannot_escape_the_directory():
    """Prefixes are built from route parameters, so one carrying a slash
    must not place the file somewhere else."""
    path = exports.new_export_path("../../etc/passwd")

    assert path.parent == exports.EXPORT_DIR


# --- the url the client is given -----------------------------------------


def test_the_url_points_at_the_export_route_not_tmp():
    url = exports.export_url(exports.new_export_path("abc"))

    assert url.startswith("/exports/")
    assert "/tmp/" not in url


def test_the_url_survives_a_round_trip():
    path = exports.new_export_path("round trip")
    path.write_text("a,b\n1,2\n")

    name = exports.export_url(path).removeprefix("/exports/")
    import urllib.parse

    assert exports.resolve_export(urllib.parse.unquote(name)) == path


# --- resolving a requested name ------------------------------------------


def _an_export(body="a,b\n1,2\n"):
    path = exports.new_export_path("fixture")
    path.write_text(body)
    return path


def test_a_real_export_resolves():
    path = _an_export()

    assert exports.resolve_export(path.name) == path


def test_a_name_that_does_not_exist_is_refused():
    assert exports.resolve_export("staking_rewards - 72723 - 2026-10.csv") is None


@pytest.mark.parametrize(
    "name",
    [
        "../etc/passwd",
        "../../etc/passwd",
        "/etc/passwd",
        "..",
        ".",
        "",
        "subdir/thing.csv",
        "thing.csv/../../etc/passwd",
    ],
)
def test_nothing_outside_the_directory_resolves(name):
    assert exports.resolve_export(name) is None


def test_a_file_in_shared_tmp_is_not_reachable():
    """The old behaviour, asserted as gone: a sibling of the export
    directory must not resolve."""
    canary = pathlib.Path("/tmp/exports_test_canary.csv")
    canary.write_text("x\n")
    try:
        assert exports.resolve_export("../exports_test_canary.csv") is None
    finally:
        canary.unlink(missing_ok=True)


def test_a_symlink_out_of_the_directory_is_refused():
    """resolve() follows symlinks, so confinement has to be checked after
    resolving, not before."""
    target = pathlib.Path("/tmp/exports_test_symlink_target.csv")
    target.write_text("secret\n")
    link = exports.EXPORT_DIR / "innocent.csv"
    link.unlink(missing_ok=True)
    os.symlink(target, link)
    try:
        assert exports.resolve_export("innocent.csv") is None
    finally:
        link.unlink(missing_ok=True)
        target.unlink(missing_ok=True)


# --- expiry ---------------------------------------------------------------


def test_old_exports_are_swept_and_fresh_ones_kept():
    """Hygiene, not the security fix: nothing deleted these before, so they
    accumulated for the life of the host."""
    old = _an_export()
    fresh = _an_export()
    long_ago = time.time() - 7200
    os.utime(old, (long_ago, long_ago))

    exports.sweep_old_exports(max_age_seconds=3600)

    assert not old.exists()
    assert fresh.exists()
    fresh.unlink(missing_ok=True)


def test_sweeping_leaves_other_directories_alone():
    canary = pathlib.Path("/tmp/exports_test_sweep_canary.csv")
    canary.write_text("x\n")
    long_ago = time.time() - 7200
    os.utime(canary, (long_ago, long_ago))
    try:
        exports.sweep_old_exports(max_age_seconds=3600)
        assert canary.exists(), "the sweep reached outside the export directory"
    finally:
        canary.unlink(missing_ok=True)


# --- the route ------------------------------------------------------------


def test_the_old_tmp_route_is_gone():
    from ccdexplorer.ccdexplorer_site.app.routers import home

    assert "/tmp/{filename}" not in {getattr(r, "path", None) for r in home.router.routes}


def test_there_is_an_exports_route():
    from ccdexplorer.ccdexplorer_site.app.routers import home

    assert "/exports/{filename}" in {getattr(r, "path", None) for r in home.router.routes}


def test_the_one_consumer_was_updated():
    """smart_contract.html links the track-and-trace csv with href="{{filename}}",
    so the value handed to it has to be the new url, not a /tmp path."""
    import pathlib as pl

    here = pl.Path(__file__).resolve()
    root = next(p for p in here.parents if (p / "justfile").is_file())
    source = (
        root / "bases/ccdexplorer/ccdexplorer_site/app/routers/smart_contracts.py"
    ).read_text()

    assert "/tmp/track_and_trace" not in source
    assert "export" in source
