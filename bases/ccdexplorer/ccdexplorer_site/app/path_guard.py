"""One check on every route's path parameters, before any handler.

`{net}` goes straight into urls the site builds for the api, and `..` as a
path parameter is a dot-segment that httpx collapses -- so /v2/../x reaches a
top-level api path, with the site's own unrestricted key. Most handlers
checked `net` and redirected, which is why it never showed; the ones that did
not are the exposure, and a check each handler has to remember is a check
someone will forget.
"""

from fastapi import HTTPException, Request

from ccdexplorer.domain.generic import NET

VALID_NETS = frozenset(n.value for n in NET)


async def guard_path_params(request: Request) -> None:
    params = request.path_params
    if any(isinstance(v, str) and v in (".", "..") for v in params.values()):
        raise HTTPException(status_code=404)
    net = params.get("net")
    if net is not None and net not in VALID_NETS:
        # What the handlers that did check have always done with it.
        raise HTTPException(status_code=302, headers={"Location": "/mainnet"})
