"""/p -- what CCD costs, without a chart.

Every traded figure comes from one venue. The bot asks the api for Kraken
candles and works the 24h window out itself: the route's own high, low and
change span all 120 bars it returns, which at 15m is thirty hours, and a
"24h high" that reaches back thirty is simply wrong.

Market cap and rank are the two things Kraken cannot know -- it has no
supply -- so they come from the same CoinMarketCap record the site's
homepage box reads, and the footer says so.
"""

from types import SimpleNamespace

from ccdexplorer.ccdexplorer_chart_bot import direct
from ccdexplorer.ccdexplorer_chart_bot import price as price_mod


# --- fakes ------------------------------------------------------------------


class _Message:
    def __init__(self, text=""):
        self.text = text
        self.photos = []
        self.htmls = []
        self.markups = []

    async def reply_photo(self, photo, **kw):
        self.photos.append((photo, kw))

    async def reply_html(self, text, **kw):
        self.htmls.append(text)
        self.markups.append(kw.get("reply_markup"))


def _candles(n, close=0.00386, volume=1_000_000.0, high=None, low=None):
    """A flat series, newest last. Each candle is 15 minutes."""
    return [
        {
            "at": f"2026-10-17T00:{i:02d}:00Z",
            "open": close,
            "high": high if high is not None else close,
            "low": low if low is not None else close,
            "close": close,
            "volume": volume,
            "trades": 1,
        }
        for i in range(n)
    ]


def _markets(price=0.00386, rank=461, mcap=52_910_000.0):
    return {
        "cmc_rank": rank,
        "quote": {
            "USD": {
                "price": price,
                "percent_change_24h": 3.4,
                "volume_24h": 322_640.0,
                "market_cap": 49_270_000.0,
                "fully_diluted_market_cap": mcap,
            }
        },
    }


# --- the routes it reads -----------------------------------------------------


def test_it_reads_the_two_routes_that_already_exist():
    """No new api route and no new third party: both of these are served."""
    paths = price_mod.api_paths()

    assert paths["ohlc"] == "mainnet/misc/ccd-ohlc/15m"
    assert paths["markets"] == "markets/info"


def test_the_interval_is_one_the_api_accepts():
    """A typo here is a 422 and a bot that can never answer /p."""
    from ccdexplorer.ccdexplorer_api.app.routers.v2.misc_v2 import KRAKEN_INTERVALS

    assert price_mod.OHLC_INTERVAL in KRAKEN_INTERVALS


def test_the_window_is_exactly_twenty_four_hours():
    """96 bars x 15 minutes. The arithmetic is the whole point of the pair."""
    from ccdexplorer.ccdexplorer_api.app.routers.v2.misc_v2 import KRAKEN_INTERVALS

    minutes = KRAKEN_INTERVALS[price_mod.OHLC_INTERVAL]
    assert price_mod.BARS_PER_DAY * minutes == 24 * 60


def test_the_api_returns_enough_bars_to_fill_the_window():
    """120 bars reach back thirty hours, so 96 of them exist to be sliced."""
    from ccdexplorer.ccdexplorer_api.app.routers.v2.misc_v2 import KRAKEN_MAX_BARS

    assert KRAKEN_MAX_BARS >= price_mod.BARS_PER_DAY


# --- the 24h window ---------------------------------------------------------


def test_only_the_last_day_counts_towards_the_window():
    """The older bars the route returns are not part of a 24h figure."""
    old = _candles(24, close=0.002, high=0.009, low=0.001, volume=5.0)
    recent = _candles(96, close=0.00386, high=0.00392, low=0.00369)

    stats = price_mod.stats(old + recent)

    assert stats["high"] == 0.00392, "a bar from thirty hours ago set the high"
    assert stats["low"] == 0.00369, "a bar from thirty hours ago set the low"


def test_the_price_is_the_latest_close():
    candles = _candles(96)
    candles[-1]["close"] = 0.00401

    assert price_mod.stats(candles)["price"] == 0.00401


def test_the_change_is_measured_against_the_open_a_day_back():
    candles = _candles(96, close=0.004)
    candles[0]["open"] = 0.002
    candles[-1]["close"] = 0.004

    assert price_mod.stats(candles)["change_pct"] == 100.0


def test_the_change_is_measured_from_inside_the_window_not_before_it():
    """Taking the open of the oldest bar the route returned, rather than of
    the oldest bar in the window, measures thirty hours and calls it a day."""
    old = _candles(24, close=0.001)
    old[0]["open"] = 0.001
    recent = _candles(96, close=0.004)
    recent[0]["open"] = 0.002

    assert price_mod.stats(old + recent)["change_pct"] == 100.0


def test_volume_is_in_usd():
    """The route's own volume is CCD traded. A dollar sign in front of it
    would overstate the number by a factor of about 260."""
    candles = _candles(96, close=0.01, volume=100.0)

    assert price_mod.stats(candles)["volume_usd"] == 96 * 100.0 * 0.01


def test_a_short_series_still_answers():
    """Kraken is not ours. Fewer bars than a day is a narrower window, not
    an exception -- and the one thing /p must not do is raise."""
    stats = price_mod.stats(_candles(5, close=0.003))

    assert stats is not None
    assert stats["price"] == 0.003


def test_no_candles_is_nothing_to_say():
    assert price_mod.stats([]) is None


def test_a_zero_open_does_not_divide_by_zero():
    candles = _candles(96)
    candles[0]["open"] = 0

    assert price_mod.stats(candles)["change_pct"] is None


# --- the market cap and rank ------------------------------------------------


def test_the_market_cap_is_the_field_the_homepage_box_uses():
    """home.py reads fully_diluted_market_cap. /p disagreeing with the front
    page about the size of the thing would be worse than either number."""
    assert price_mod.market(_markets(mcap=52_910_000.0))["market_cap"] == 52_910_000.0


def test_the_rank_comes_through():
    assert price_mod.market(_markets(rank=461))["rank"] == 461


def test_a_missing_markets_record_is_not_fatal():
    assert price_mod.market(None) == {}


def test_a_markets_record_without_a_quote_is_not_fatal():
    assert price_mod.market({"cmc_rank": 461}) == {"rank": 461}


def test_a_zero_rank_is_no_rank():
    """home.py falls back to cmc_rank 0 when the record is missing, so a 0
    here means unknown rather than first place."""
    assert "rank" not in price_mod.market(_markets(rank=0))


# --- the message ------------------------------------------------------------


#: `markets=None` has to mean "no market data", so the default cannot be None.
_DEFAULT = object()


def _summary(candles=_DEFAULT, markets=_DEFAULT):
    return price_mod.summary(
        {"candles": _candles(96, high=0.00392, low=0.00369) if candles is _DEFAULT else candles},
        _markets() if markets is _DEFAULT else markets,
    )


def test_the_message_has_the_five_figures_that_were_asked_for():
    """The high and the low are no longer a label and two numbers -- they are
    the endpoints of the candle -- so this asks for them where they now are."""
    said = _summary()

    assert "Price:" in said
    assert "24h:" in said
    assert "Vol:" in said
    assert "MCap:" in said
    assert price_mod.BODY in said, "no candle drawn"


def test_the_price_is_written_the_way_the_homepage_writes_it():
    """f"{price:,.5f}" on the front page. $0.0 would be true and useless."""
    said = _summary()

    assert "$0.00386" in said


def test_the_high_and_low_are_both_there():
    said = _summary()

    assert "$0.00392" in said
    assert "$0.00369" in said


def test_a_rise_is_signed():
    candles = _candles(96, close=0.004)
    candles[0]["open"] = 0.002

    assert "+100.00%" in _summary(candles)


def test_a_fall_is_signed_too():
    candles = _candles(96, close=0.001)
    candles[0]["open"] = 0.002

    assert "-50.00%" in _summary(candles)


def test_large_numbers_are_compact():
    """$52910000.00 is a number nobody reads."""
    said = _summary()

    assert "$52.91M" in said
    assert "52910000" not in said


def test_the_rank_is_on_the_message():
    assert "461" in _summary()


def test_the_footer_names_both_sources():
    """Two sources in one message is defensible only if it says so."""
    said = _summary()

    assert "Kraken" in said
    assert "CoinMarketCap" in said


def test_the_message_still_works_with_no_market_data():
    """The api answers 404 for markets/info when the record is missing. The
    four Kraken figures are still worth sending."""
    said = _summary(markets=None)

    assert said is not None
    assert "Price:" in said
    assert "MCap:" not in said, "claimed a market cap it did not have"
    assert "CoinMarketCap" not in said, "credited a source it did not use"


def test_no_candles_means_no_message():
    """Without Kraken there is no price, and /p is about the price."""
    assert price_mod.summary({"candles": []}, _markets()) is None


def test_a_missing_ohlc_payload_means_no_message():
    assert price_mod.summary(None, _markets()) is None


# --- the command ------------------------------------------------------------


def _context(*args):
    return SimpleNamespace(bot=SimpleNamespace(username="ccdexplorer_chart_bot"), args=list(args))


async def _run(monkeypatch, ohlc, markets):
    async def fake_fetch():
        return {"ohlc": ohlc, "markets": markets}

    monkeypatch.setattr(direct, "fetch_price", fake_fetch)
    update = SimpleNamespace(message=_Message())
    await direct.price_command_handler()(update, _context())
    return update.message


async def test_p_answers_with_text_and_no_chart(monkeypatch):
    """/p is a quote. A chart would make it a different command."""
    message = await _run(monkeypatch, {"candles": _candles(96)}, _markets())

    assert message.htmls, "said nothing"
    assert not message.photos


async def test_p_prints_the_summary(monkeypatch):
    message = await _run(monkeypatch, {"candles": _candles(96)}, _markets())

    assert "Price:" in message.htmls[0]
    assert "MCap:" in message.htmls[0]


async def test_a_failed_lookup_gets_a_sentence_rather_than_silence(monkeypatch):
    async def fails():
        return None

    monkeypatch.setattr(direct, "fetch_price", fails)
    update = SimpleNamespace(message=_Message())
    await direct.price_command_handler()(update, _context())

    assert update.message.htmls, "a dead api made the bot say nothing"
    assert not update.message.photos


async def test_an_empty_kraken_response_gets_a_sentence_too(monkeypatch):
    message = await _run(monkeypatch, {"candles": []}, _markets())

    assert message.htmls
    assert "Price:" not in message.htmls[0]


async def test_the_command_takes_no_arguments(monkeypatch):
    """/p is about one token. Anything typed after it is ignored, not an error."""

    async def fake_fetch():
        return {"ohlc": {"candles": _candles(96)}, "markets": _markets()}

    monkeypatch.setattr(direct, "fetch_price", fake_fetch)
    update = SimpleNamespace(message=_Message())
    await direct.price_command_handler()(update, _context("nonsense", "words"))

    assert "Price:" in update.message.htmls[0]


# --- registration -----------------------------------------------------------


def test_p_is_registered():
    import inspect

    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    source = inspect.getsource(main)
    assert 'CommandHandler(["p", "price"], price_command_handler())' in source


def test_p_is_registered_before_the_plain_text_handler():
    """Registered after the MessageHandler, a command still routes correctly,
    but the ordering comment in main says commands come first -- keep it true."""
    import inspect

    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    source = inspect.getsource(main)
    assert source.index('CommandHandler(["p", "price"]') < source.index("MessageHandler(")


async def test_start_teaches_the_new_command():
    """A command nothing mentions is a command nobody types."""
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    message = _Message()
    await main.start(SimpleNamespace(message=message), _context())

    assert "/p" in message.htmls[0]


# --- the 24h candle, lying on its side -------------------------------------
#
# A sideways candlestick says four things in one line of text: the wick runs
# the 24h range, the body runs open to close, and because the window ends now
# the close is the current price. That is the high, the low and the price in
# one glyph run, which is what the plain "H/L: a | b" line could not do.


def _candle(opened, close, high=0.00414, low=0.00371, width=None):
    return price_mod.candle(opened, close, high, low, **({} if width is None else {"width": width}))


def test_the_candle_is_the_width_it_says_it_is():
    """Telegram wraps a line that overruns, which would split the candle."""
    assert len(_candle(0.00382, 0.00386)) == price_mod.CANDLE_WIDTH


def test_the_width_does_not_depend_on_the_numbers():
    narrow = _candle(0.00382, 0.003821)
    wide = _candle(0.00371, 0.00414)

    assert len(narrow) == len(wide) == price_mod.CANDLE_WIDTH


def test_the_body_spans_open_to_close():
    """open at the low, close a third up: body from the left edge to a third."""
    drawn = _candle(0.00371, 0.00386, width=20)
    body = [i for i, cell in enumerate(drawn) if cell == price_mod.BODY]

    assert body[0] == 0, drawn
    assert 5 <= body[-1] <= 8, drawn


def test_a_down_candle_draws_the_same_span():
    """A fall is the same body, entered from the other end. Which end is the
    close is said by the signed change on the line above, not by the glyphs."""
    up = _candle(0.00380, 0.00400)
    down = _candle(0.00400, 0.00380)

    assert up == down


def test_the_wick_fills_the_rest_of_the_range():
    drawn = _candle(0.00382, 0.00386)

    assert set(drawn) == {price_mod.WICK, price_mod.BODY}
    assert drawn[0] == price_mod.WICK, "a body at the low when open is not the low"
    assert drawn[-1] == price_mod.WICK, "a body at the high when close is not the high"


def test_a_candle_with_no_wicks_is_all_body():
    """open at the low and close at the high: nothing left to wick."""
    drawn = _candle(0.00371, 0.00414)

    assert drawn == price_mod.BODY * price_mod.CANDLE_WIDTH


def test_a_doji_still_has_a_visible_body():
    """open == close rounds to a zero-width body, which would draw a candle
    with no body at all -- indistinguishable from a plain range bar."""
    drawn = _candle(0.00386, 0.00386)

    assert price_mod.BODY in drawn
    assert drawn.count(price_mod.BODY) >= 1


def test_a_flat_market_does_not_divide_by_zero():
    """Every bar at one price: high == low, and the range has no width."""
    drawn = price_mod.candle(0.0039, 0.0039, 0.0039, 0.0039)

    assert len(drawn) == price_mod.CANDLE_WIDTH
    assert price_mod.BODY in drawn


def test_the_body_never_escapes_the_track():
    """A close outside the high/low cannot happen from one window, but the
    route serves stale candles when Kraken is down and arithmetic on a
    mismatched pair should still draw inside its own box."""
    drawn = price_mod.candle(0.001, 0.009, 0.00414, 0.00371)

    assert len(drawn) == price_mod.CANDLE_WIDTH
    assert set(drawn) <= {price_mod.WICK, price_mod.BODY}


# --- the open, which the candle needs and the old message did not ----------


def test_stats_exposes_the_open():
    candles = _candles(96, close=0.004)
    candles[0]["open"] = 0.002

    assert price_mod.stats(candles)["open"] == 0.002


def test_the_open_is_the_one_inside_the_window():
    old = _candles(24, close=0.001)
    old[0]["open"] = 0.0005
    recent = _candles(96, close=0.004)
    recent[0]["open"] = 0.002

    assert price_mod.stats(old + recent)["open"] == 0.002


# --- the candle in the message ---------------------------------------------


def test_the_candle_line_carries_the_low_and_the_high():
    said = _summary()

    assert "$0.00369 " in said, said
    assert " $0.00392" in said, said


def test_the_candle_is_wrapped_for_a_monospace_font():
    """Telegram draws the message in a proportional font unless told not to,
    and a candle whose cells are different widths is not a candle."""
    said = _summary()

    assert "<code>" in said
    body = said.split("<code>")[1].split("</code>")[0]
    assert price_mod.BODY in body, body


def test_the_footer_says_what_the_body_means():
    """Without it the drawing reads as a progress bar."""
    assert "open" in _summary() and "close" in _summary()


def test_the_legend_survives_cmc_being_down():
    """The candle is drawn from Kraken alone, so its legend is still owed
    even when the market half of the message is missing."""
    said = _summary(markets=None)

    assert price_mod.BODY in said
    assert "open" in said and "close" in said


def test_there_is_no_sparkline():
    """Dropped deliberately: on a shared scale with the candle it was flat,
    and on its own scale it claimed a range the labels did not describe."""
    said = _summary()

    assert "▁" not in said and "▂" not in said and "▇" not in said
