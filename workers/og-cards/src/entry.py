"""Social preview cards for ccdexplorer links, drawn at the edge.

Paste a ccdexplorer link into Telegram, Slack or X today and the preview shows
a title and nothing else -- the templates set og:title and og:url and stop
there. This Worker fills the gap the cheap way: one PNG per entity, generated
on demand from the same API the site uses, so a shared block link previews as
that block's height, hash, time and transaction count.

Why a Worker rather than the site:

  * The site is a single-worker uvicorn process. Rasterising a 1200x630 PNG in
    it would block the event loop for every other request, and crawler traffic
    arrives in bursts when a link spreads.
  * Cards are the definition of cacheable. A finalized block's card is true
    forever, so the edge can serve it without ever asking us twice.
  * It fails harmlessly. If this Worker is down the preview is plain again,
    which is exactly where we are now.

Note what this deliberately does *not* do. Cloudflare's own OpenGraph example
puts a Worker in front of the whole site and rewrites the HTML on the way
past. That would place a Worker in the critical path of every page load to
gain some meta tags. Here the site keeps serving itself and only the image URL
points at the edge.
"""

import datetime as dt
import json
from io import BytesIO
from urllib.parse import quote, urlsplit

from PIL import Image, ImageDraw, ImageFont
from pyodide.ffi import to_js
from workers import Response, WorkerEntrypoint, fetch

#: The size every preview consumer crops to. 1.91:1, which is what Telegram,
#: X and Slack all want; get it wrong and they letterbox or refuse.
CARD_WIDTH = 1200
CARD_HEIGHT = 630

BACKGROUND_TOP = (26, 26, 26)
BACKGROUND_BOTTOM = (16, 16, 16)
TEXT = (242, 242, 242)
MUTED = (138, 143, 152)
RULE = (48, 48, 48)

#: Net is the single most important thing on the card -- a testnet link that
#: previews like a mainnet one is actively misleading -- so it gets a colour
#: rather than just a word.
NET_COLOURS = {
    "mainnet": (0, 149, 15),
    "testnet": (227, 160, 8),
    "devnet": (139, 92, 246),
}
NETS = tuple(NET_COLOURS)

#: A finalized block or transaction never changes, so its card is true for as
#: long as anyone cares. An account's balance moves, so its card is not.
TTL_IMMUTABLE = 86_400
TTL_MUTABLE = 300
TTL_FALLBACK = 60

#: Identifiers arrive from a crawlable URL, so they are attacker-controlled.
#: Block heights, account addresses (50 chars) and transaction hashes (64) all
#: fit well inside this; anything longer is not a real entity.
MAX_IDENT = 80


def _font(size):
    """A scalable font, without shipping a font file.

    Pillow 10.1 gained a size argument to ``load_default``, which returns the
    bundled Aileron TrueType instead of the 10px bitmap font that came before.
    The fallback keeps the card rendering -- badly, but rendering -- on an
    older Pillow rather than 500ing on a crawler.
    """
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _fit(draw, text, font, max_width):
    """Truncate ``text`` with an ellipsis until it fits ``max_width``."""
    text = str(text)
    if draw.textlength(text, font=font) <= max_width:
        return text
    while text and draw.textlength(text + "…", font=font) > max_width:
        text = text[:-1]
    return text + "…"


def shorten_hash(value, head=10, tail=8):
    """``b7fc3dd35e…0393c1`` -- recognisable, and it leaves room for a stat row."""
    value = str(value or "")
    if len(value) <= head + tail + 1:
        return value
    return f"{value[:head]}…{value[-tail:]}"


def ccd(micro):
    """microCCD to a readable CCD amount, or None if it was not a number."""
    try:
        return f"{int(micro) / 1_000_000:,.2f} CCD"
    except (TypeError, ValueError):
        return None


def when(value):
    """An ISO timestamp as ``23 Sep 2026 06:05:50 UTC``, or None."""
    if not value:
        return None
    try:
        moment = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment.strftime("%d %b %Y %H:%M:%S UTC")


def render_card(net, kicker, headline, subline=None, stats=(), footer=None):
    """Draw one card.

    The shape is fixed on purpose: every entity gets the same frame, so a
    reader recognises a ccdexplorer preview before reading a word of it.
    """
    accent = NET_COLOURS.get(net, MUTED)
    image = Image.new("RGB", (CARD_WIDTH, CARD_HEIGHT), BACKGROUND_TOP)
    draw = ImageDraw.Draw(image)

    # A slight vertical gradient, so the card does not read as a flat box in a
    # dark chat window.
    for y in range(CARD_HEIGHT):
        factor = y / CARD_HEIGHT
        draw.line(
            [(0, y), (CARD_WIDTH, y)],
            fill=tuple(
                int(top + (bottom - top) * factor)
                for top, bottom in zip(BACKGROUND_TOP, BACKGROUND_BOTTOM)
            ),
        )

    draw.rectangle([(0, 0), (CARD_WIDTH, 10)], fill=accent)

    margin = 72
    inner = CARD_WIDTH - (margin * 2)

    brand_font = _font(30)
    draw.text((margin, 58), "ccdexplorer.io", font=brand_font, fill=TEXT)
    net_label = (net or "unknown").upper()
    draw.text(
        (CARD_WIDTH - margin - draw.textlength(net_label, font=brand_font), 58),
        net_label,
        font=brand_font,
        fill=accent,
    )

    kicker_font = _font(26)
    draw.text((margin, 160), str(kicker).upper(), font=kicker_font, fill=MUTED)

    headline_font = _font(78)
    draw.text(
        (margin, 202), _fit(draw, headline, headline_font, inner), font=headline_font, fill=TEXT
    )

    if subline:
        subline_font = _font(34)
        draw.text(
            (margin, 306), _fit(draw, subline, subline_font, inner), font=subline_font, fill=MUTED
        )

    stats = [(label, value) for label, value in stats if value not in (None, "")]
    if stats:
        draw.line([(margin, 420), (CARD_WIDTH - margin, 420)], fill=RULE, width=2)
        label_font = _font(24)
        value_font = _font(38)
        column = inner // max(len(stats), 1)
        for index, (label, value) in enumerate(stats[:3]):
            x = margin + (index * column)
            draw.text((x, 456), str(label).upper(), font=label_font, fill=MUTED)
            draw.text(
                (x, 490), _fit(draw, value, value_font, column - 24), font=value_font, fill=TEXT
            )

    if footer:
        footer_font = _font(26)
        draw.text(
            (margin, 566), _fit(draw, footer, footer_font, inner), font=footer_font, fill=MUTED
        )

    return image


def png_response(image, max_age):
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return Response(
        to_js(buffer.getvalue()).buffer,
        headers={
            "content-type": "image/png",
            "cache-control": f"public, max-age={max_age}",
        },
    )


class Explorer:
    """Reads the ccdexplorer API, with a KV cache in front of it.

    The cache exists to protect the API, not to make the card fast. A link that
    spreads brings a burst of crawlers for the same entity, and without this
    each one would become a Mongo query -- or, for accounts, a gRPC call to a
    node.
    """

    def __init__(self, base_url, api_key, cache):
        self.base_url = str(base_url).rstrip("/")
        self.api_key = api_key
        self.cache = cache

    async def get(self, path, cache_key, ttl):
        if self.cache is not None:
            try:
                hit = await self.cache.get(cache_key)
            except Exception as error:  # a cache that errors must not break the card
                print(f"og-cards: cache read failed ({error})")
                hit = None
            if hit is not None:
                try:
                    return json.loads(hit)
                except ValueError:
                    pass

        options = {}
        if self.api_key:
            options["headers"] = {"x-ccdexplorer-key": self.api_key}
        try:
            response = await fetch(f"{self.base_url}{path}", **options)
        except Exception as error:
            print(f"og-cards: {path} unreachable ({error})")
            return None
        if not 200 <= int(response.status) < 300:
            print(f"og-cards: {path} returned HTTP {response.status}")
            return None

        body = await response.text()
        if self.cache is not None:
            try:
                await self.cache.put(cache_key, body, expirationTtl=ttl)
            except Exception as error:
                print(f"og-cards: cache write failed ({error})")
        try:
            return json.loads(body)
        except ValueError:
            return None


async def block_card(explorer, net, ident):
    block = await explorer.get(
        f"/v2/{net}/block/{quote(ident, safe='')}", f"{net}:block:{ident}", TTL_IMMUTABLE
    )
    if not block:
        return None, TTL_FALLBACK

    height = block.get("height")
    validator = block.get("baker")
    return (
        render_card(
            net,
            "Block",
            f"{height:,}" if isinstance(height, int) else str(height or ident),
            shorten_hash(block.get("hash")),
            [
                ("Transactions", f"{block.get('transaction_count', 0):,}"),
                ("Validator", f"#{validator}" if validator is not None else None),
                ("Finalized", "Yes" if block.get("finalized") else "No"),
            ],
            when(block.get("slot_time")),
        ),
        TTL_IMMUTABLE,
    )


async def account_card(explorer, net, ident):
    account = await explorer.get(
        f"/v2/{net}/account/{quote(ident, safe='')}/info", f"{net}:account:{ident}", TTL_MUTABLE
    )
    if not account:
        return None, TTL_FALLBACK

    index = account.get("index")
    stake = account.get("stake") or {}
    validator = stake.get("baker") or {}
    delegator = stake.get("delegator") or {}

    role = None
    staked = None
    if validator:
        baker_id = (validator.get("baker_info") or {}).get("baker_id")
        role = f"Validator #{baker_id}" if baker_id is not None else "Validator"
        staked = ccd(validator.get("staked_amount"))
    elif delegator:
        role = "Delegator"
        staked = ccd(delegator.get("staked_amount"))

    return (
        render_card(
            net,
            "Account",
            f"#{index:,}" if isinstance(index, int) else str(index or ident),
            shorten_hash(account.get("address"), head=12, tail=10),
            [
                ("Balance", ccd(account.get("amount"))),
                ("Available", ccd(account.get("available_balance"))),
                ("Staked", staked),
            ],
            role,
        ),
        TTL_MUTABLE,
    )


async def transaction_card(explorer, net, ident):
    transaction = await explorer.get(
        f"/v2/{net}/transaction/{quote(ident, safe='')}", f"{net}:tx:{ident}", TTL_IMMUTABLE
    )
    if not transaction:
        return None, TTL_FALLBACK

    details = transaction.get("account_transaction") or {}
    block = transaction.get("block_info") or {}
    kind = transaction.get("type") or {}
    contents = kind.get("contents") or kind.get("type") or "Transaction"
    height = block.get("height")

    return (
        render_card(
            net,
            str(contents).replace("_", " "),
            shorten_hash(transaction.get("hash"), head=12, tail=10),
            f"Sender {shorten_hash(details.get('sender'), head=10, tail=8)}"
            if details.get("sender")
            else None,
            [
                ("Cost", ccd(details.get("cost"))),
                ("Block", f"{height:,}" if isinstance(height, int) else None),
                (
                    "Energy",
                    f"{transaction['energy_cost']:,}" if transaction.get("energy_cost") else None,
                ),
            ],
            when(block.get("slot_time")),
        ),
        TTL_IMMUTABLE,
    )


CARD_BUILDERS = {
    "block": block_card,
    "account": account_card,
    "transaction": transaction_card,
    "tx": transaction_card,
}


def fallback_card(net="mainnet"):
    """What a crawler gets when the entity is unknown or the API is unreachable.

    Served with 200, not 404. A preview consumer that gets an error shows the
    bare link, which is worse than a plain branded card -- and a stale or
    mistyped link is not a reason to make the whole message look broken.
    """
    return render_card(
        net if net in NETS else "mainnet",
        "Concordium",
        "ccdexplorer.io",
        "The most comprehensive explorer for the Concordium blockchain",
    )


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        path = urlsplit(request.url).path
        segments = [segment for segment in path.split("/") if segment]

        if not segments:
            return Response.json(
                {
                    "service": "ccdexplorer og-cards",
                    "usage": "/<net>/<block|account|transaction>/<identifier>.png",
                    "nets": list(NETS),
                }
            )

        if len(segments) < 3 or segments[0] not in NETS:
            return png_response(fallback_card(segments[0] if segments else "mainnet"), TTL_FALLBACK)

        net, kind = segments[0], segments[1]
        ident = "/".join(segments[2:])
        if ident.endswith(".png"):
            ident = ident[: -len(".png")]

        builder = CARD_BUILDERS.get(kind)
        if builder is None or not ident or len(ident) > MAX_IDENT:
            return png_response(fallback_card(net), TTL_FALLBACK)

        explorer = Explorer(
            getattr(self.env, "API_URL", None) or "https://api.ccdexplorer.io",
            getattr(self.env, "CCDEXPLORER_API_KEY", None) or "",
            # Optional: without it every crawler hit reaches the API, which
            # works but is exactly what the cache is here to avoid.
            getattr(self.env, "OG_CACHE", None),
        )

        try:
            image, max_age = await builder(explorer, net, ident)
        except Exception as error:  # a broken card must still be a card
            print(f"og-cards: failed to build {net}/{kind}/{ident} ({error})")
            image, max_age = None, TTL_FALLBACK

        if image is None:
            return png_response(fallback_card(net), TTL_FALLBACK)
        return png_response(image, max_age)
