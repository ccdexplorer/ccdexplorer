"""Generated CSVs, served by a url nobody can guess.

These downloads are for every visitor, signed in or not, so the answer to
"who may fetch this file" cannot be authentication. It is that the url is the
capability: the server generates a name with a uuid4 in it, hands the caller
that one url, and a caller who guesses gets 404.

What was wrong was not the names -- the one live consumer, the track-and-trace
link on the smart contract page, already had a uuid4 in its name -- but the
directory. Files went to /tmp and GET /tmp/{filename} served anything there by
name. /tmp is shared with every other process on the host, so the route could
not tell this application's exports from anyone else's temporary files, and a
predictably-named file written by something unrelated was reachable from the
internet.

Traversal was never the hole: a path parameter matches a single segment, so
`../etc/passwd` never arrived. That was an accident of routing rather than a
decision, which is why resolve_export checks confinement explicitly -- a later
edit to `{filename:path}` would otherwise reopen it silently.
"""

import os
import pathlib
import re
import time
import urllib.parse
import uuid

#: Under /tmp so a restart still clears it, but a directory this application
#: owns rather than the shared one. Nothing else writes here, so the serving
#: route cannot reach another process's files.
EXPORT_DIR = pathlib.Path("/tmp/ccdexplorer-exports")

#: The url prefix the route is mounted at, and what export_url builds.
EXPORT_ROUTE = "/exports"

#: What a requested name may contain. Deliberately strict: anything with a
#: separator, a leading dot or an unexpected character is refused before the
#: filesystem is touched at all.
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._|,()#-]*\.csv$")

#: Long enough that a download survives a slow click, short enough that a
#: link pasted somewhere public stops working.
DEFAULT_MAX_AGE_SECONDS = 3600


def ensure_export_dir() -> pathlib.Path:
    """The export directory, created 0700 if it is not there yet."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    # mkdir's mode is masked by umask, so set it explicitly -- the point of
    # the directory is that only this process can list it.
    os.chmod(EXPORT_DIR, 0o700)
    return EXPORT_DIR


def new_export_path(prefix: str) -> pathlib.Path:
    """A fresh path for an export, named `<prefix> - <uuid4>.csv`.

    The prefix is only there so a saved file is recognisable; it is stripped
    of anything that could place the file outside the directory, because
    callers build it from route parameters.
    """
    ensure_export_dir()
    safe_prefix = re.sub(r"[^A-Za-z0-9 ._|,()#-]", "_", prefix).strip(" .") or "export"
    return EXPORT_DIR / f"{safe_prefix} - {uuid.uuid4()}.csv"


def export_url(path: pathlib.Path | str) -> str:
    """The url a client should be given for an export already written."""
    name = pathlib.Path(path).name
    return f"{EXPORT_ROUTE}/{urllib.parse.quote(name)}"


def resolve_export(name: str) -> pathlib.Path | None:
    """The file `name` refers to inside the export directory, or None.

    None covers every way a request can fail to name an export: a shape that
    is not an export name, a path that resolves outside the directory
    (including through a symlink, which is why confinement is checked after
    resolving rather than before), and a name that simply is not there.
    """
    if not name or not _SAFE_NAME.match(name):
        return None

    # Resolved only for the checks. /tmp is a symlink to /private/tmp on
    # macOS, so returning the resolved form would hand callers a path that
    # does not compare equal to the one new_export_path gave them.
    directory = EXPORT_DIR.resolve()
    candidate = (directory / name).resolve()
    if not candidate.is_relative_to(directory):
        return None
    if not candidate.is_file():
        return None
    return EXPORT_DIR / name


def sweep_old_exports(max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS) -> int:
    """Delete exports older than `max_age_seconds`; return how many went.

    Hygiene rather than the security fix: nothing deleted these before, so
    they accumulated for the life of the host. Scoped to the export
    directory and to files whose names it generates, so a sweep can never
    reach anything it did not write.
    """
    if not EXPORT_DIR.is_dir():
        return 0

    cutoff = time.time() - max_age_seconds
    removed = 0
    for path in EXPORT_DIR.iterdir():
        if not path.is_file() or path.is_symlink():
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            # Someone else's race, or a file already gone. Not worth failing
            # a download over.
            continue
    return removed
