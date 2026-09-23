"""Serves the image a ccdexplorer link shows when it is shared.

The callers are Telegram, Slack, X, Discord and anything else that unfurls a
link -- never a browser someone is looking at. That shapes three decisions:

  * **Always 200.** An unknown entity gets a plain branded card rather than a
    404, because a preview consumer that receives an error shows the bare link
    instead, and a mistyped or deleted link is not a reason to make somebody's
    whole message look broken.
  * **Never on the event loop.** A card is ~12 ms of CPU and the site is a
    single uvicorn worker, so the drawing happens in a thread.
  * **Junk is refused before it costs anything.** The route is public and
    unauthenticated, and a miss would otherwise be an API call plus a render.
    ``og_cards.resolve`` validates the whole path first.

The URL is the page's own address with ``og`` after the net and
``/image.png`` on the end, so /mainnet/instance/9337/0 is previewed by
/mainnet/og/instance/9337/0/image.png. The statistics charts already use the
same <path>/image.png ending, through kaleido rather than Pillow.
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
            # Crawlers are not browsers, and some follow whatever a sniffed
            # type suggests; this one is exactly what it says it is.
            "X-Content-Type-Options": "nosniff",
        },
    )


async def render(request: Request, entity: str, httpx_client: httpx.AsyncClient) -> Response:
    """Render, or serve from cache, the card for one page path."""
    entity = entity.strip("/")
    cached = _CARDS.get(entity)
    if cached is not None:
        payload, max_age = cached
        return _png(payload, max_age)

    resolved = og_cards.resolve(entity)
    if resolved is None:
        # Not a page we draw. No API call, no lookup, and the fallback is
        # cached against this key so a flood of nonsense costs one render
        # between all of it.
        net = entity.partition("/")[0]
        net, kind, parts, payload = (net if net in og_cards.NETS else "mainnet"), None, (), None
    else:
        net, kind, parts = resolved
        payload = None
        # Most kinds have one endpoint. /<net>/tokens/<tag> is the exception:
        # the tag may name a protocol-level token or a CIS-2 one and the path
        # does not say which, so it is tried in the same order the site's own
        # handler tries it, and the card is drawn by whichever answered.
        candidates = (kind, kind.alt) if kind.alt else (kind,)
        for candidate in candidates:
            api_result = await get_url_from_api(
                f"{request.app.api_url}{candidate.api(net, parts)}", httpx_client
            )
            if api_result.ok and api_result.return_value:
                kind, payload = candidate, api_result.return_value
                break

    max_age = kind.ttl if (kind and payload) else og_cards.TTL_FALLBACK
    card = await asyncio.to_thread(og_cards.build_png, net, kind, parts, payload)
    _CARDS.put(entity, card, max_age)
    return _png(card, max_age)


@router.get("/{net}/og/{rest:path}/image.png", include_in_schema=False)
async def og_card(
    request: Request,
    net: str,
    rest: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
) -> Response:
    return await render(request, f"{net}/{rest}", httpx_client)


@router.get("/og/{entity:path}/image.png", include_in_schema=False)
async def og_card_legacy(
    request: Request,
    entity: str,
    httpx_client: httpx.AsyncClient = Depends(get_httpx_client),
) -> Response:
    """The shape this shipped with for a few hours.

    Kept because a preview that was already unfurled has the old URL baked
    into it for as long as the consumer caches it. Safe to delete once nothing
    out there still points here.
    """
    return await render(request, entity, httpx_client)
