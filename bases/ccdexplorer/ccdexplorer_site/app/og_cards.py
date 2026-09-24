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
from collections.abc import Callable
from dataclasses import dataclass
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

#: Three stats fit across a 1200px card at a readable size. A builder may
#: offer more, in priority order, and the extras are dropped.
MAX_STATS = 3

#: Identifiers arrive from a public URL, so they are attacker-controlled and
#: are matched against the shape they must have before any work is done.
#: Heights, account indices and contract indices are decimal; block and
#: transaction hashes are 32 bytes of hex; account addresses and lock ids are
#: base58 without the ambiguous characters.
_DECIMAL = re.compile(r"\A[0-9]{1,15}\Z")
_HASH = re.compile(r"\A[0-9a-fA-F]{64}\Z")
_ADDRESS = re.compile(r"\A[1-9A-HJ-NP-Za-km-z]{40,60}\Z")
_LOCK_ID = re.compile(r"\A[1-9A-HJ-NP-Za-km-z]{6,64}\Z")
#: A PLT symbol, e.g. t-USDT or eGOLD.
_TAG = re.compile(r"\A[0-9a-zA-Z._-]{1,32}\Z")
#: A CIS-2 token id is hex, and is legitimately empty for a single-token
#: contract -- wGBM's is -- so this has to admit the empty string.
_TOKEN_ID = re.compile(r"\A[0-9a-fA-F]{0,64}\Z")

#: No real identifier here is longer than a 64-character hash.
MAX_SEGMENT = 80


@dataclass(frozen=True)
class Kind:
    """One sort of page that gets a card.

    ``api`` turns the identifier parts into the path to ask the API for;
    ``build`` turns what came back into an image.
    """

    name: str
    api: Callable[[str, tuple[str, ...]], str]
    build: Callable
    ttl: int
    #: Tried when the first endpoint has nothing. /<net>/tokens/<tag> serves
    #: both a protocol-level token and a CIS-2 tag, and which one a tag is
    #: cannot be told from the path -- the site itself decides by asking.
    alt: "Kind | None" = None


def _is(value, *shapes):
    return any(shape.match(value) for shape in shapes)


def resolve(entity: str):
    """Map a page path to (net, kind, parts), or None if it is not a card we draw.

    ``entity`` is the page's own path -- ``mainnet/block/52029165``,
    ``mainnet/instance/9337/0`` -- so a card URL is always derivable from the
    address of the page it belongs to, and the two cannot drift apart.

    Everything is validated here, before a single request is made. The route is
    public and unauthenticated and a miss costs an API call plus ~12 ms of
    drawing, so anything that is not a plausible identifier is refused on its
    shape and never reaches the API.
    """
    segments = [segment for segment in str(entity).split("/") if segment]
    if len(segments) < 3 or segments[0] not in NETS:
        return None
    net, head, rest = segments[0], segments[1], tuple(segments[2:])
    if any(len(segment) > MAX_SEGMENT for segment in rest):
        return None

    if head == "block" and len(rest) == 1 and _is(rest[0], _DECIMAL, _HASH):
        return net, KINDS["block"], rest
    if head == "transaction" and len(rest) == 1 and _is(rest[0], _HASH):
        return net, KINDS["transaction"], rest
    if head == "account" and rest and _is(rest[0], _DECIMAL, _ADDRESS):
        # The validator view has no URL of its own -- that tab is fetched by
        # ajax -- so this is a card to link to deliberately rather than one a
        # page points at. Pools get shared often enough to be worth having.
        if len(rest) == 2 and rest[1] == "validator":
            return net, KINDS["validator"], (rest[0],)
        if len(rest) == 1:
            return net, KINDS["account"], rest
        return None
    if head == "instance" and len(rest) == 2 and all(_DECIMAL.match(s) for s in rest):
        return net, KINDS["contract"], rest
    if head == "token" and len(rest) in (2, 3) and all(_DECIMAL.match(s) for s in rest[:2]):
        token_id = rest[2] if len(rest) == 3 else ""
        if _TOKEN_ID.match(token_id):
            return net, KINDS["token"], (rest[0], rest[1], token_id)
        return None
    if head == "tokens" and len(rest) == 3 and rest[:2] == ("plt", "lock"):
        return (net, KINDS["lock"], (rest[2],)) if _LOCK_ID.match(rest[2]) else None
    if head == "tokens" and len(rest) == 1 and _TAG.match(rest[0]):
        return net, KINDS["plt"], rest
    return None


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
    """microCCD as a readable CCD amount, or None if it was not a number.

    Large stakes are abbreviated because a stat column is about 330px wide and
    "420,000,000.00 CCD" is not: it came back from the first validator card as
    "420,000,000.00 C...", which is worse than losing the pennies nobody reads
    on a social card anyway.
    """
    try:
        amount = int(micro) / 1_000_000
    except (TypeError, ValueError):
        return None
    if abs(amount) >= 1_000_000_000:
        return f"{amount / 1_000_000_000:,.2f}B CCD"
    if abs(amount) >= 1_000_000:
        return f"{amount / 1_000_000:,.2f}M CCD"
    return f"{amount:,.2f} CCD"


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

    # Dropped before the width is computed, not after: sizing the columns by
    # how many stats were offered while drawing only three made every column
    # narrower than the space it actually had. A validator's "538.49M CCD"
    # missed the resulting 240px by one pixel and rendered as "538.49M CC...".
    stats = [(label, value) for label, value in stats if value not in (None, "")][:MAX_STATS]
    if stats:
        draw.line([(margin, 420), (CARD_WIDTH - margin, 420)], fill=RULE, width=2)
        label_font = _font(24)
        value_font = _font(38)
        column = inner // len(stats)
        for index, (label, value) in enumerate(stats):
            x = margin + (index * column)
            draw.text(
                (x, 456),
                _fit(draw, str(label).upper(), label_font, column - 24),
                font=label_font,
                fill=MUTED,
            )
            draw.text(
                (x, 490), _fit(draw, value, value_font, column - 24), font=value_font, fill=TEXT
            )

    if footer:
        footer_font = _font(26)
        draw.text(
            (margin, 566), _fit(draw, footer, footer_font, inner), font=footer_font, fill=MUTED
        )

    return image


def token_amount(amount):
    """A PLT amount, which the API sends as {"value": "...", "decimals": n}."""
    if not isinstance(amount, dict):
        return None
    try:
        value = int(amount["value"])
        decimals = int(amount.get("decimals") or 0)
    except (KeyError, TypeError, ValueError):
        return None
    return f"{value / (10**decimals):,.{min(decimals, 6)}f}"


def scaled_amount(raw, decimals):
    """A raw token amount divided by its own decimals.

    The API returns CIS-2 supply in base units and the decimals separately, so
    printing token_amount as-is overstates it by 10**decimals. wGBM has seven
    of them: 2,239,000,001 base units is a supply of 223.90, and the first
    version of this card claimed 2.24 billion.
    """
    try:
        value = int(raw)
        places = int(decimals or 0)
    except (TypeError, ValueError):
        return None
    return f"{value / (10**places):,.{2 if places else 0}f}"


def block_card(net, parts, block):
    height = block.get("height")
    validator = block.get("baker")
    return render_card(
        net,
        "Block",
        f"{height:,}" if isinstance(height, int) else str(height or parts[0]),
        shorten_hash(block.get("hash")),
        [
            ("Transactions", f"{block.get('transaction_count', 0):,}"),
            ("Validator", f"#{validator}" if validator is not None else None),
            ("Finalized", "Yes" if block.get("finalized") else "No"),
        ],
        when(block.get("slot_time")),
    )


def account_card(net, parts, account):
    """The account as an account: what it holds.

    A validator gets named in the footer but not given the headline -- these
    links are shared for the account far more often than for the pool, and the
    pool has a card of its own at .../account/<index>/validator.
    """
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

    # The account nonce is the next sequence number, so one less than it is
    # exactly how many transactions this account has sent. It says nothing
    # about what it received, which is why the label is "sent" rather than a
    # bare transaction count.
    nonce = account.get("sequence_number")
    sent = f"{nonce - 1:,}" if isinstance(nonce, int) and nonce >= 1 else None

    # Only three fit. Available is listed last because it is the one that is
    # usually redundant: it equals the balance on an account that is not
    # staking, and balance minus stake on one that is.
    amount = account.get("amount")
    available = account.get("available_balance")
    candidates = [
        ("Balance", ccd(amount)),
        ("Staked", staked),
        ("Transactions sent", sent),
        ("Available", ccd(available) if available not in (None, amount) else None),
    ]

    return render_card(
        net,
        "Account",
        f"#{index:,}" if isinstance(index, int) else str(index or parts[0]),
        shorten_hash(account.get("address"), head=12, tail=10),
        [(label, value) for label, value in candidates if value],
        role,
    )


def validator_card(net, parts, pool):
    """The pool behind an account: what it stakes and how it is doing."""
    baker = pool.get("baker")
    payday = pool.get("current_payday_info") or {}
    lottery = payday.get("lottery_power")
    blocks = payday.get("blocks_baked")

    footer = []
    if isinstance(blocks, int):
        footer.append(f"{blocks:,} blocks this payday")
    open_status = (pool.get("pool_info") or {}).get("open_status")
    if open_status:
        footer.append(str(open_status).replace("_", " ").capitalize())
    if pool.get("is_suspended"):
        footer.append("SUSPENDED")

    return render_card(
        net,
        "Validator",
        f"#{baker}" if baker is not None else str(parts[0]),
        shorten_hash(pool.get("address"), head=12, tail=10),
        [
            ("Staked", ccd(pool.get("equity_capital"))),
            ("Delegated", ccd(pool.get("delegated_capital"))),
            (
                "Lottery power",
                f"{lottery * 100:.4f}%" if isinstance(lottery, (int, float)) else None,
            ),
        ],
        " · ".join(footer) or None,
    )


def transaction_card(net, parts, transaction):
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


def plt_card(net, parts, plt):
    """A protocol-level token: supply and what the module allows."""
    state = plt.get("token_state") or {}
    module = state.get("module_state") or {}
    token_id = plt.get("token_id") or parts[0]
    name = module.get("name")

    flags = [
        label
        for label, on in (
            ("Mintable", module.get("mintable")),
            ("Burnable", module.get("burnable")),
            ("Allow list", module.get("allow_list")),
            ("Deny list", module.get("deny_list")),
            ("PAUSED", module.get("paused")),
        )
        if on
    ]

    return render_card(
        net,
        "Protocol-level token",
        str(token_id),
        name if name and name != token_id else None,
        [
            ("Supply", token_amount(state.get("total_supply"))),
            ("Decimals", str(state.get("decimals")) if state.get("decimals") is not None else None),
            (
                "Governance",
                shorten_hash((module.get("governance_account") or {}).get("account"), 6, 6),
            ),
        ],
        " · ".join(flags) or None,
    )


def token_card(net, parts, token):
    """A CIS-2 token, named from its metadata where it has any."""
    metadata = token.get("token_metadata") or {}
    tag = token.get("tag_information") or {}
    # This builder serves two paths: /token/<index>/<subindex>/<id>, which has
    # three parts, and /tokens/<tag>, which has one. The payload carries both
    # values either way, so the parts are only a last resort -- and must not be
    # indexed blindly.
    contract = token.get("contract") or (f"<{parts[0]},{parts[1]}>" if len(parts) > 1 else "")
    token_id = token.get("token_id") or (parts[2] if len(parts) > 2 else "")
    holders = token.get("current_holders_count")
    if not isinstance(holders, int) and isinstance(token.get("token_holders"), dict):
        holders = len(token["token_holders"])

    decimals = metadata.get("decimals")
    if decimals is None:
        decimals = (token.get("verified_information") or {}).get("decimals")

    headline = metadata.get("name") or tag.get("_id") or token_id or contract
    subline = contract if not token_id else f"{contract} · {shorten_hash(token_id, 8, 6)}"

    return render_card(
        net,
        "Token",
        str(headline),
        subline,
        [
            ("Symbol", metadata.get("symbol")),
            ("Holders", f"{holders:,}" if isinstance(holders, int) else None),
            ("Supply", scaled_amount(token.get("token_amount"), decimals)),
        ],
        metadata.get("description"),
    )


def lock_card(net, parts, lock):
    """A PLT lock: what it holds, for whom, and until when."""
    controller = lock.get("controller") or {}
    tokens = controller.get("tokens") or []
    funds = lock.get("funds") or []
    status = lock.get("status")

    return render_card(
        net,
        "PLT lock",
        str(parts[0]),
        " · ".join(str(token) for token in tokens) or None,
        [
            ("Status", str(status).title() if status else None),
            ("Funded by", f"{len(funds):,}" if funds else None),
            ("Recipients", str(lock.get("recipients") or "").title() or None),
        ],
        f"Expires {when(lock.get('expiry'))}" if lock.get("expiry") else None,
    )


def contract_card(net, parts, contract):
    """A smart contract instance: its name, balance and whether it is verified."""
    v1 = contract.get("v1") or {}
    methods = v1.get("methods") or []
    name = str(v1.get("name") or "").removeprefix("init_")
    verified = (contract.get("module_verification") or {}).get("verified")

    return render_card(
        net,
        "Smart contract",
        name or f"<{parts[0]},{parts[1]}>",
        contract.get("_id") or f"<{parts[0]},{parts[1]}>",
        [
            ("Balance", ccd(v1.get("amount"))),
            ("Methods", f"{len(methods):,}" if methods else None),
            ("Verified", "Yes" if verified else "No"),
        ],
        f"Owner {shorten_hash(v1.get('owner'), 12, 10)}" if v1.get("owner") else None,
    )


KINDS = {
    "block": Kind("block", lambda net, p: f"/v2/{net}/block/{p[0]}", block_card, TTL_IMMUTABLE),
    "account": Kind(
        "account", lambda net, p: f"/v2/{net}/account/{p[0]}/info", account_card, TTL_MUTABLE
    ),
    "validator": Kind(
        "validator",
        lambda net, p: f"/v2/{net}/account/{p[0]}/pool-info",
        validator_card,
        TTL_MUTABLE,
    ),
    "transaction": Kind(
        "transaction",
        lambda net, p: f"/v2/{net}/transaction/{p[0]}",
        transaction_card,
        TTL_IMMUTABLE,
    ),
    "tag": Kind("tag", lambda net, p: f"/v2/{net}/token/tag/{p[0]}/info", token_card, TTL_MUTABLE),
    "token": Kind(
        "token",
        # A single-token contract has an empty token id -- wGBM does -- and the
        # API spells that "_", the same placeholder the site's own handler uses.
        lambda net, p: f"/v2/{net}/token/{p[0]}/{p[1]}/{p[2] or '_'}/info",
        token_card,
        TTL_MUTABLE,
    ),
    "lock": Kind("lock", lambda net, p: f"/v2/{net}/plt/lock/{p[0]}", lock_card, TTL_MUTABLE),
    "contract": Kind(
        "contract",
        lambda net, p: f"/v2/{net}/contract/{p[0]}/{p[1]}/info",
        contract_card,
        TTL_MUTABLE,
    ),
}


#: Added after the table, because it points at an entry in it: a tag is tried
#: as a protocol-level token first and as a CIS-2 token second, which is the
#: same order the site's own /<net>/tokens/<tag> handler tries them in.
KINDS["plt"] = Kind(
    "plt",
    lambda net, p: f"/v2/{net}/plt/{p[0]}/info",
    plt_card,
    TTL_MUTABLE,
    alt=KINDS["tag"],
)


def fallback_card(net="mainnet"):
    """What a crawler gets when the entity is unknown or the API said no.

    Served with 200, not 404. A preview consumer that receives an error falls
    back to showing the bare link, which looks worse than a plain branded card
    -- and a mistyped or deleted link is not a reason to make somebody's whole
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


def build_png(net: str, kind, parts, payload) -> bytes:
    """Render a card to PNG bytes. Blocking; call it off the event loop."""
    if kind is None or not payload:
        return to_png(fallback_card(net))
    try:
        return to_png(kind.build(net, parts, payload))
    except Exception as error:  # a card that cannot be drawn is still a card
        print(f"og-cards: failed to draw {kind.name} {parts} on {net} ({error})")
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

    def discard(self, key: str) -> None:
        """Forget one entry, so the next request redraws it.

        Used by the plot warmer: a cached chart is not re-rendered by asking
        for it, so refreshing one means dropping it first.
        """
        self._entries.pop(key, None)

    def __len__(self) -> int:
        return len(self._entries)
