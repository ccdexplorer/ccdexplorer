"""/node: what the pages load from node_modules, and nothing else.

The whole tree was mounted, so every package's package.json, README, license
and source were one request away -- a list of exactly what is installed and at
which version. Pages load scripts, styles, fonts and images from here; only
those are served.
"""

from pathlib import PurePosixPath

from fastapi import HTTPException
from fastapi.staticfiles import StaticFiles

#: What a page can load. `.map` is here because browsers ask for it when
#: developer tools are open; it is as public as the script it describes.
SERVED_SUFFIXES = frozenset(
    {
        ".js", ".mjs", ".css", ".map",
        ".woff", ".woff2", ".ttf", ".otf", ".eot",
        ".svg", ".png", ".gif", ".jpg", ".jpeg", ".ico",
    }
)  # fmt: skip


class NodeModulesFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        if PurePosixPath(path).suffix.lower() not in SERVED_SUFFIXES:
            raise HTTPException(status_code=404)
        return await super().get_response(path, scope)
