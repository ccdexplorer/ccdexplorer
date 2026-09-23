"""Serves the image a ccdexplorer link shows when it is shared.

The statistics charts already do this: /plots/<net>/<name>/image.png
renders a Plotly figure through kaleido, and base/plots_og.html points
og:image at it. This is the same idea for the entity pages -- same
<path>/image.png shape, same tag set -- with Pillow instead of a headless
Chromium, because there is no figure here to rasterise.

The route is what ``og:image`` in the page's meta tags points at, so its
callers are Telegram, Slack, X, Discord and anything else that unfurls a link
-- never a browser someone is looking at. That shapes three decisions:

  * **Always 200.** An unknown entity gets a plain branded card rather than a
    404, because a preview consumer that receives an error shows the bare
    link instead, and a mistyped or deleted link is not a reason to make
    somebody's whole message look broken.
  * **Never on the event loop.** A card is ~12 ms of CPU and the site is a
    single uvicorn worker, so the drawing happens in a thread.
  * **Junk is refused before it costs anything.** The route is public and
    unauthenticated, and each miss would otherwise be an API call plus a
    render. Identifiers are checked against the shape they must have first.
"""

import asyncio

import httpx2 as httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response

from ccdexplorer.ccdexplorer_site.app import og_cards
from ccdexplorer.ccdexplorer_site.app.state import get_httpx_client
from ccdexplorer.ccdexplorer_site.app.utils import get_url_from_api

router = APIRouter()

#: Process-local on purpose. ccdexplorer.io is served by openresty rather than
#: through a CDN, so nothing else absorbs a burst of crawlers arriving for the
#: same link, and the site runs one uvicorn worker so one cache sees them all.
_CARDS = og_cards.PngCache()


def _png(payload: bytes, max_age: int) -> Response:
    return Response(
        content=payload,
        media_type="image/png",
        headers={
            "Cache-Control": f"public, max-age={max_age}",
            # Crawlers are not browsers and some follow whatever a sniffed type
            # suggests; this one is exactly what it says it is.
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/og/{net}/{kind}/{ident}/image.png", include_in_schema=False)
async def og_card(
    request: Request,
    net: str,
    kind: str,
    ident: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
) -> Response:
    """Render, or serve from cache, the social card for one entity."""
    key = f"{net}/{kind}/{ident}"
    cached = _CARDS.get(key)
    if cached is not None:
        payload, max_age = cached
        return _png(payload, max_age)

    if og_cards.accepts(net, kind, ident):
        api_result = await get_url_from_api(
            f"{request.app.api_url}{og_cards.api_path(net, kind, ident)}", httpx_client
        )
        payload = api_result.return_value if api_result.ok else None
    else:
        # Not a plausible height, hash or address. No API call, no lookup, and
        # the resulting fallback is cached against this key so a flood of
        # nonsense costs one render between them.
        payload = None

    max_age = og_cards.ttl_for(kind) if payload else og_cards.TTL_FALLBACK
    card = await asyncio.to_thread(og_cards.build_png, net, kind, ident, payload)
    _CARDS.put(key, card, max_age)
    return _png(card, max_age)
