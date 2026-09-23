"""The picture a ccdexplorer link shows when someone shares it.

Paste a link into Telegram, Slack or X and the crawler behind it reads the
page's meta tags and fetches whatever ``og:image`` points at. Until now the
site set no ``og:image`` at all, so a shared block was a line of text.

This renders that image: one 1200x630 PNG per block, account and transaction,
drawn from the same API the page itself uses.

Three things shape the implementation, and all three are about not hurting the
site that hosts it:

  * **It must not block the event loop.** A card costs about 12 ms of CPU,
    almost all of it glyph rasterisation and PNG encoding, and the site is a
    single uvicorn worker. The router runs ``build_png`` through
    ``asyncio.to_thread`` for exactly this reason.

  * **It must render each card once.** ccdexplorer.io is served by openresty,
    not through a CDN that would absorb repeat crawls, so the cache here is
    the only cache. Crawler traffic for one link arrives as a burst, which a
    small in-process store handles almost perfectly.

  * **It must refuse junk before doing any work.** The route is public,
    unauthenticated, and each request would otherwise cost an API call plus
    12 ms. An identifier that is not a plausible height, hash or address is
    rejected on its shape, before anything is fetched or drawn, and the
    resulting fallback is cached against that key so a flood of nonsense costs
    one render in total.

Rendering functions here take plain dictionaries and return images. Nothing in
this module touches a request, a client or a database, so a card can be
exercised from a REPL with a fixture.
"""

import datetime as dt
import re
from collections import OrderedDict
from functools import lru_cache
from io import BytesIO
from time import monotonic

from PIL import Image, ImageDraw, ImageFont

#: The size every preview consumer crops to. 1.91:1, which is what Telegram, X
#: and Slack all want; get it wrong and they letterbox it or refuse it.
CARD_WIDTH = 1200
CARD_HEIGHT = 630

BACKGROUND_TOP = (26, 26, 26)
BACKGROUND_BOTTOM = (16, 16, 16)
TEXT = (242, 242, 242)
MUTED = (138, 143, 152)
RULE = (48, 48, 48)

#: Net is the single most important thing on the card -- a testnet link that
#: previews like a mainnet one is actively misleading -- so it gets a colour
#: rather than only a word.
NET_COLOURS = {
    "mainnet": (0, 149, 15),
    "testnet": (227, 160, 8),
    "devnet": (139, 92, 246),
}
NETS = tuple(NET_COLOURS)

#: A finalized block or transaction never changes, so its card stays true for
#: as long as anyone cares. An account's balance moves, so its card does not.
TTL_IMMUTABLE = 86_400
TTL_MUTABLE = 300
TTL_FALLBACK = 600

#: ~55 KB a card, so this is about 14 MB at worst. Well within what the site
#: already carries, and far more than one link's burst of crawlers needs.
CACHE_MAX_ENTRIES = 256

#: Identifiers arrive from a public URL, so they are attacker-controlled and
#: are matched against the shape they must have before any work is done.
#: Heights and account indices are decimal, block and transaction hashes are
#: 32 bytes of hex, and account addresses are base58 without the ambiguous
#: characters.
_DECIMAL = re.compile(r"\A[0-9]{1,15}\Z")
_HASH = re.compile(r"\A[0-9a-fA-F]{64}\Z")
_ADDRESS = re.compile(r"\A[1-9A-HJ-NP-Za-km-z]{40,60}\Z")

#: kind -> (accepted identifier shapes, API path, cache TTL)
KINDS = {
    "block": ((_DECIMAL, _HASH), "/v2/{net}/block/{ident}", TTL_IMMUTABLE),
    "account": ((_DECIMAL, _ADDRESS), "/v2/{net}/account/{ident}/info", TTL_MUTABLE),
    "transaction": ((_HASH,), "/v2/{net}/transaction/{ident}", TTL_IMMUTABLE),
}


def accepts(net: str, kind: str, ident: str) -> bool:
    """Is this a request worth spending an API call and a render on?"""
    if net not in NETS or kind not in KINDS:
        return False
    shapes, _, _ = KINDS[kind]
    return any(shape.match(ident) for shape in shapes)


def api_path(net: str, kind: str, ident: str) -> str:
    return KINDS[kind][1].format(net=net, ident=ident)


def ttl_for(kind: str) -> int:
    return KINDS[kind][2]


@lru_cache(maxsize=32)
def _font(size: int):
    """A scalable font, without shipping a font file.

    Pillow 10.1 gave ``load_default`` a size argument, which returns the
    bundled Aileron TrueType rather than the 10px bitmap that came before. The
    fallback keeps a card rendering -- badly, but rendering -- on anything
    older, instead of turning a crawler into a 500.
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
    """microCCD as a readable CCD amount, or None if it was not a number."""
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

    The frame is the same for every entity on purpose, so a reader recognises
    a ccdexplorer preview before reading a word of it.
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


def block_card(net, ident, block):
    height = block.get("height")
    validator = block.get("baker")
    return render_card(
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
    )


def account_card(net, ident, account):
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

    return render_card(
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
    )


def transaction_card(net, ident, transaction):
    details = transaction.get("account_transaction") or {}
    block = transaction.get("block_info") or {}
    kind = transaction.get("type") or {}
    contents = kind.get("contents") or kind.get("type") or "Transaction"
    height = block.get("height")
    energy = transaction.get("energy_cost")

    return render_card(
        net,
        str(contents).replace("_", " "),
        shorten_hash(transaction.get("hash"), head=12, tail=10),
        f"Sender {shorten_hash(details.get('sender'), head=10, tail=8)}"
        if details.get("sender")
        else None,
        [
            ("Cost", ccd(details.get("cost"))),
            ("Block", f"{height:,}" if isinstance(height, int) else None),
            ("Energy", f"{energy:,}" if energy else None),
        ],
        when(block.get("slot_time")),
    )


BUILDERS = {"block": block_card, "account": account_card, "transaction": transaction_card}


def fallback_card(net="mainnet"):
    """What a crawler gets when the entity is unknown or the API said no.

    Served with 200, not 404. A preview consumer that gets an error falls back
    to showing the bare link, which looks worse than a plain branded card --
    and a mistyped or deleted link is not a reason to make somebody's whole
    message look broken.
    """
    return render_card(
        net if net in NETS else "mainnet",
        "Concordium",
        "ccdexplorer.io",
        "The most comprehensive explorer for the Concordium blockchain",
    )


def to_png(image) -> bytes:
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def build_png(net: str, kind: str, ident: str, payload) -> bytes:
    """Render a card to PNG bytes. Blocking; call it off the event loop."""
    builder = BUILDERS.get(kind)
    if builder is None or not payload:
        return to_png(fallback_card(net))
    try:
        return to_png(builder(net, ident, payload))
    except Exception:  # noqa: BLE001 - a card that cannot be drawn is still a card
        return to_png(fallback_card(net))


class PngCache:
    """A bounded, time-limited store of finished cards.

    The site has no CDN in front of it, so this is the only thing standing
    between a link going around a Telegram group and one render per crawler.
    Eviction is least-recently-used; entries also expire, because an account's
    balance moves and a card claiming otherwise is worse than no card.
    """

    def __init__(self, max_entries: int = CACHE_MAX_ENTRIES):
        # The value is (png, max_age): the age is cached with the image because
        # a card built from a 404 is only good for minutes even when the kind
        # it was asked for is one we would otherwise cache for a day.
        self._entries: OrderedDict[str, tuple[float, tuple[bytes, int]]] = OrderedDict()
        self._max = max_entries

    def get(self, key: str):
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at < monotonic():
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return value

    def put(self, key: str, payload: bytes, ttl: int) -> None:
        self._entries[key] = (monotonic() + ttl, (payload, ttl))
        self._entries.move_to_end(key)
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)
