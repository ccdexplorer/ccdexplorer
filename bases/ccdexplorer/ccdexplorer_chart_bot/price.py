"""/p -- what CCD costs, without a chart.

One venue, said out loud. Every traded figure here is Kraken CCD/USD, taken
from the api route the site's candle chart already reads, so the price, the
high, the low, the change and the volume all describe the same order book and
cannot disagree with each other.

The 24h window is worked out here rather than read off the route. That route
reports its high, low and change across every bar it returns -- 120 of them,
which at fifteen minutes is thirty hours -- because that is the span its chart
draws. Calling any of those a 24h figure would be wrong by six hours, so the
last `BARS_PER_DAY` bars are sliced off first and everything is computed from
those.

The 24h range is drawn as well as listed. A sideways candlestick -- the wick
spanning low to high, the body spanning open to close -- carries four numbers
in one glyph run, and because the window ends now the close *is* the current
price. Underneath it the low and the high are named, one per line, in a
column: "H/L: a | b" was two numbers on one line with nothing saying which
was which, and nothing placing the price between them.

Market cap and rank are the two things one venue cannot know, having no
supply to multiply by. They come from the same CoinMarketCap record the site's
homepage box reads -- `fully_diluted_market_cap` and `cmc_rank`, the identical
fields -- so /p and the front page never quote different sizes for the same
chain. The footer names both sources, which is the only thing that makes two
of them defensible in one message.
"""

#: CCD trades on mainnet and nowhere else; the route refuses anything else.
NET = "mainnet"

#: Short enough that 24h divides into it exactly, long enough that the route's
#: 120 bars still reach past a day. 1h would make the window 24 bars but pin
#: the high and low to hourly extremes; 5m would not reach back far enough.
OHLC_INTERVAL = "15m"

#: 96 x 15 minutes = 24 hours.
BARS_PER_DAY = 96

#: What the site's homepage box prints, so /p agrees with it. Not `market_cap`,
#: which is the circulating-supply figure CoinMarketCap leads with: two numbers
#: for the size of one chain, on two pages of the same explorer, is worse than
#: either of them being the one a reader expected.
MARKET_CAP_FIELD = "fully_diluted_market_cap"

#: As home.py writes the price. Four would round CCD's third significant digit
#: away; the homepage settled on five and this follows it.
PRICE_DECIMALS = 5

_UNITS = ((1_000_000_000_000, "Tr"), (1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K"))

#: Cells the sideways candle is drawn in. Wide enough that a body of a few
#: tenths of a percent still occupies one, narrow enough that the line fits a
#: phone's width with a price label at each end and is not wrapped in half.
CANDLE_WIDTH = 20

#: The range the candle spans, and the open-to-close body on it. Both are
#: East Asian Ambiguous, as are the block elements generally -- so they take
#: the same width as each other in whatever font Telegram picks for <code>,
#: which is the only property the drawing actually depends on.
WICK = "─"
BODY = "█"

#: Indent on the LO and HI lines. Enough to sit them under the candle rather
#: than under the emoji that labels it, so the two numbers read as belonging
#: to the drawing above and not to the message's left edge.
LABEL_INDENT = "   "


def api_paths() -> dict[str, str]:
    """The two routes /p reads, both of which the api already serves.

    Nothing new is asked of the api and no third party is called from the bot:
    Kraken is reached through the route that caches it, and CoinMarketCap
    through the record a dagster job refreshes every ten minutes.
    """
    return {
        "ohlc": f"{NET}/misc/ccd-ohlc/{OHLC_INTERVAL}",
        "markets": "markets/info",
    }


def stats(candles: list[dict]) -> dict | None:
    """The last 24 hours of one order book, or None if there is no book.

    A series shorter than a day is a narrower window rather than an error.
    Kraken is not ours and the route degrades to stale candles when it is
    unreachable, so the one thing this must not do is raise.
    """
    window = candles[-BARS_PER_DAY:]
    if not window:
        return None

    opened = window[0]["open"]
    price = window[-1]["close"]
    return {
        "price": price,
        "high": max(candle["high"] for candle in window),
        "low": min(candle["low"] for candle in window),
        # The open of the window, which together with price, high and low is
        # the whole of the 24h candle the message draws.
        "open": opened,
        # Against the open of the oldest bar *in the window*, not of the oldest
        # bar the route returned -- that one is thirty hours back.
        "change_pct": ((price / opened) - 1) * 100 if opened else None,
        # The route's own `volume` is CCD traded. A dollar sign in front of
        # that overstates it by whatever CCD happens to cost, so each bar is
        # valued at its own close before being summed.
        "volume_usd": sum(candle["volume"] * candle["close"] for candle in window),
    }


def market(markets: dict | None) -> dict:
    """Market cap and rank from the CoinMarketCap record, as far as it goes.

    Either can be absent: the api answers 404 for markets/info when the
    record has not been written yet, and home.py falls back to a rank of 0
    when it is missing -- so a 0 here means unknown, not first place.
    """
    if not markets:
        return {}

    found = {}
    rank = markets.get("cmc_rank")
    if rank:
        found["rank"] = rank

    usd = (markets.get("quote") or {}).get("USD") or {}
    cap = usd.get(MARKET_CAP_FIELD)
    if cap:
        found["market_cap"] = float(cap)
    return found


def _cell(value: float, low: float, high: float, width: int) -> int:
    """Which cell of the track a price falls in, clamped to the track.

    A flat market has no range to place anything in, so the middle is as
    true as any other cell and the arithmetic has no zero to divide by.
    """
    if high <= low:
        return width // 2
    scaled = round((value - low) / (high - low) * (width - 1))
    return min(width - 1, max(0, scaled))


def candle(opened: float, close: float, high: float, low: float, width: int = CANDLE_WIDTH) -> str:
    """The 24h candle, lying on its side.

    The wick runs the whole range because the ends of the track *are* the low
    and the high; the body runs open to close. The window ends now, so the
    close is the current price -- which is how one glyph run says the high,
    the low and the price at once, where a "H/L: a | b" line said two of the
    three and left the reader to place the third between them.

    Which end of the body is the close is not drawn: a rise and a fall of the
    same size are the same body. The signed change sits on the line above and
    says it in words, which is less pretty and harder to misread than a second
    glyph nobody has a key for.
    """
    start, end = sorted(
        (
            _cell(min(opened, close), low, high, width),
            _cell(max(opened, close), low, high, width),
        )
    )
    # A body narrower than a cell rounds to nothing, and a candle with no body
    # is a range bar -- so a doji keeps one cell rather than disappearing.
    if end == start:
        end = min(width - 1, start + 1)

    track = [WICK] * width
    for position in range(start, end + 1):
        track[position] = BODY
    return "".join(track)


def _price(value: float) -> str:
    return f"${value:,.{PRICE_DECIMALS}f}"


def _compact(value: float) -> str:
    """$52.91M. The full figure is a number nobody reads off a phone."""
    for size, suffix in _UNITS:
        if abs(value) >= size:
            return f"${value / size:,.2f}{suffix}"
    return f"${value:,.2f}"


def summary(ohlc: dict | None, markets: dict | None) -> str | None:
    """The message, or None when there is no price to put in it.

    None rather than a message with gaps in it: /p is the price, and the four
    Kraken figures are the whole of what it promises. Market cap and rank are
    dropped silently when CoinMarketCap has nothing, along with the half of
    the footer that credits it -- naming a source that supplied no number is
    its own small lie.
    """
    candles = (ohlc or {}).get("candles") or []
    traded = stats(candles)
    if traded is None:
        return None

    cap = market(markets)
    rank = f" — #{cap['rank']}" if "rank" in cap else ""
    change = traded["change_pct"]

    lines = [
        f"<b>Concordium · CCD</b>{rank}",
        "",
        f"💰 Price: <b>{_price(traded['price'])}</b>",
    ]
    # Before the candle, because it is what says which end of the body is the
    # close -- the drawing cannot, and reads the same either way.
    if change is not None:
        arrow = "🚀" if change > 0 else "📉" if change < 0 else "➡"
        lines.append(f"{arrow} 24h: {change:+.2f}%")
    # <code> because Telegram sets the rest of the message in a proportional
    # font, and a candle whose cells are not all one width is not a candle.
    drawn = candle(traded["open"], traded["price"], traded["high"], traded["low"])
    lines.append(f"🕯 <code>{drawn}</code>")
    # One <code> per line rather than one span across all three. The labels
    # are what aligns the two values, and "LO: " and "HI: " are equal width
    # only in a monospace font -- in a proportional one I is narrow where O
    # is wide, and the column the labels exist to create would not survive.
    # High above low, the way a price axis reads, rather than following the
    # candle's left-to-right. The drawing and the list disagree about which
    # end comes first, and the list is the one being read as numbers.
    lines.append(f"<code>{LABEL_INDENT}HI: {_price(traded['high'])}</code>")
    lines.append(f"<code>{LABEL_INDENT}LO: {_price(traded['low'])}</code>")
    # The candle and its two numbers are one thing; the totals below are
    # another. A blank line is the cheapest way to say so.
    lines.append("")
    lines.append(f"📊 24h Vol: {_compact(traded['volume_usd'])}")
    if "market_cap" in cap:
        lines.append(f"💎 MCap: {_compact(cap['market_cap'])}")

    # Built from what actually arrived. Crediting CoinMarketCap with a market
    # cap it had no field for is the same small lie as printing one.
    borrowed = [
        label for key, label in (("market_cap", "market cap"), ("rank", "rank")) if key in cap
    ]
    # The legend is not optional. Without it the candle reads as a progress
    # bar, which would make a cheap market look like a nearly-empty one.
    credit = "Price from Kraken CCD/USD · body = open→close"
    if borrowed:
        credit += f" · {' and '.join(borrowed)} from CoinMarketCap"
    lines += ["", f"<i>{credit}</i>"]
    return "\n".join(lines)
