"""The chart bot offers charts the site can actually draw.

The catalogue is written out here rather than imported from the site, because
a base may not import another base and because an inline result wants one line
where a page wants a paragraph. The cost of that is drift: a chart added to the
site would never appear in the bot, and one removed would leave the bot
offering a URL that returns an HTML error page to Telegram.

So the first test reads the site's routes and compares. It is the reason the
duplication is safe.
"""

import re
from pathlib import Path

import pytest

from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS, MAX_RESULTS, NET, search
from ccdexplorer.ccdexplorer_chart_bot.inline import nothing_found, photo_result, results_for

SITE = "https://ccdexplorer.io"
ROUTERS = Path(__file__).resolve().parents[4] / "bases/ccdexplorer/ccdexplorer_site/app/routers"


def site_plot_names() -> set[str]:
    source = "".join(p.read_text() for p in ROUTERS.rglob("*.py"))
    return set(re.findall(r'"/plots/\{net\}/([a-z0-9_]+)/image\.png"', source))


def test_the_bot_never_offers_a_chart_the_site_cannot_draw():
    """The direction that actually breaks something.

    A name here with no route on the site is a URL handed to Telegram that
    answers with an HTML error page, so the reader gets a broken result. That
    must fail.

    The other direction is allowed on purpose: the site may serve a chart the
    bot does not list yet. Requiring equality meant the two had to ship in one
    commit and deploy together, with no way to put a chart on the site first
    and add it to the bot once it had proved itself.
    """
    catalogue = {chart.name for chart in CHARTS}
    served = site_plot_names()
    assert catalogue <= served, f"offered by the bot, not served: {sorted(catalogue - served)}"


def test_charts_the_site_serves_but_the_bot_does_not_offer():
    """Not a failure -- a list, so it is a decision rather than an oversight.

    Printed by pytest only when something is missing, which is the moment
    somebody should be asked whether it belongs in the bot.
    """
    missing = sorted(site_plot_names() - {chart.name for chart in CHARTS})
    if missing:
        print(f"\n  not offered by the chart bot: {missing}")


def test_every_chart_has_a_title_and_a_one_line_description():
    for chart in CHARTS:
        assert chart.title and not chart.title.endswith(".")
        assert chart.description and len(chart.description) < 60, chart.name


def test_names_are_unique():
    assert len({chart.name for chart in CHARTS}) == len(CHARTS)


# --- the urls -------------------------------------------------------------


def test_the_image_url_is_the_one_the_site_serves():
    chart = next(c for c in CHARTS if c.name == "staking_validator_count")
    assert chart.image_url(SITE).startswith(
        f"{SITE}/plots/mainnet/staking_validator_count/image.png"
    )
    assert chart.page_url(SITE) == f"{SITE}/plots/mainnet/staking_validator_count"


def test_the_bot_asks_for_light_charts():
    """These land in somebody else's chat, mostly a light one, where a dark
    chart reads as a black rectangle rather than a graph."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import THEME

    assert THEME == "light"
    assert all(c.image_url(SITE).endswith("?theme=light") for c in CHARTS)


def test_the_page_link_carries_no_theme():
    """The page is the site's own, and the site picks its own theme."""
    assert all("theme=" not in c.page_url(SITE) for c in CHARTS)


def test_a_trailing_slash_on_the_site_url_does_not_double_up():
    chart = CHARTS[0]
    assert "//plots" not in chart.image_url("https://ccdexplorer.io/")


def test_every_chart_is_mainnet():
    """The routes exist for other nets and answer 200 with an HTML error page.

    Telegram would fetch that and show a broken result, so the net is not a
    parameter anywhere in this bot.
    """
    assert NET == "mainnet"
    assert all("/plots/mainnet/" in c.image_url(SITE) for c in CHARTS)


# --- search ---------------------------------------------------------------


def test_an_empty_query_shows_the_catalogue_rather_than_nothing():
    """Someone who types the bot's name and stops should see what there is."""
    assert len(search("")) == len(CHARTS)
    assert search("   ") == search("")


@pytest.mark.parametrize(
    "query, expected",
    [
        ("staking_validator_count", "staking_validator_count"),
        ("validator stake", "staking_validator_staked_amounts"),
        ("delegators per pool", "staking_avg_delegator_per_pool_count"),
        ("fees", "transaction_fees"),
        ("exchanges", "ccd_on_exchanges"),
        ("accounts per day", "accounts_per_day"),
    ],
)
def test_a_query_finds_the_chart_it_describes(query, expected):
    assert search(query)[0].name == expected


def test_every_word_has_to_match():
    """Narrowing, not widening: a list that ignores half of what was typed
    reads as broken."""
    assert len(search("validator")) > len(search("validator stake"))


def test_an_unmatched_query_returns_nothing_from_search():
    assert search("kittens") == []


def test_results_are_capped():
    assert len(search("", limit=3)) == 3


# --- inline results -------------------------------------------------------


def test_a_match_becomes_a_photo_telegram_can_fetch():
    result = photo_result(CHARTS[0], SITE)
    assert result.photo_url == result.thumbnail_url == CHARTS[0].image_url(SITE)
    assert (result.photo_width, result.photo_height) == (1200, 630)


def test_a_miss_is_an_article_not_an_empty_answer():
    """An empty answer renders as a silent empty panel, which reads as the bot
    being broken rather than the query being wrong."""
    results = results_for("kittens", SITE)
    assert len(results) == 1
    assert results[0].id == "no-match"
    assert "kittens" in results[0].title


def test_the_miss_message_points_somewhere_useful():
    article = nothing_found("kittens", SITE)
    assert SITE in article.input_message_content.message_text


def test_a_query_that_matches_returns_only_photos():
    results = results_for("staking", SITE)
    assert results
    assert all(r.id != "no-match" for r in results)


# --- the entry point ------------------------------------------------------


def test_the_entry_point_imports():
    """It was not imported by anything, so nothing caught that SITE_URL was
    never exported from ccdexplorer.env -- the whole suite passed green while
    the bot could not have started. The site base carries the same test for
    the same reason."""
    from ccdexplorer.ccdexplorer_chart_bot import __main__

    assert callable(__main__.main)


def test_it_refuses_to_start_without_a_token(monkeypatch):
    """A bot with no token polls nothing and says nothing; failing loudly at
    boot is better than a container that looks healthy and is deaf."""
    from ccdexplorer.ccdexplorer_chart_bot import __main__

    monkeypatch.setattr(__main__, "CHART_BOT_TOKEN", "")
    with pytest.raises(SystemExit):
        __main__.main()


# --- discovery ------------------------------------------------------------
#
# An inline bot is browsed, not searched: someone types its name, looks at what
# comes back, and picks. Everything below is about that working.


def test_an_empty_query_shows_every_chart():
    """Capping this below the catalogue size hid charts from the only means
    anyone has of browsing -- twice: first at twelve of eighteen, then again
    when three price charts were added to a cap pinned at eighteen."""
    assert len(search("")) == len(CHARTS)


def test_the_catalogue_still_fits_in_one_telegram_answer():
    """Fifty is Telegram's limit, not ours. Past it, results are silently
    dropped again."""
    assert len(CHARTS) <= MAX_RESULTS


@pytest.mark.parametrize(
    "query, expected",
    [
        ("how much is staked", "staking_percentage_staked"),
        ("show me the validators", "staking_validator_count"),
        ("what are the fees", "transaction_fees"),
        ("whales", "daily_limits"),
        ("tps", "network_activity_tps"),
        ("bakers", "staking_validator_count"),
        ("binance", "ccd_on_exchanges"),
        ("price", "ccd_price_24h"),
        ("realized", "realized_prices"),
    ],
)
def test_the_words_people_actually_type_find_the_chart(query, expected):
    """The route names are ours, not the reader's."""
    assert search(query)[0].name == expected


def test_filler_words_do_not_sink_a_query():
    """Every word having to match meant a whole question found nothing."""
    assert search("how much is staked") == search("staked")


def test_a_near_miss_beats_an_empty_panel():
    """Half-right is worth showing; the reader can see it and reject it."""
    hits = search("validator kittens")
    assert hits and hits[0].name.startswith("staking_validator")


def test_genuine_nonsense_still_finds_nothing():
    assert search("kittens") == []


def test_every_chart_carries_words_that_reach_it():
    for chart in CHARTS:
        assert chart.keywords, chart.name
        for word in chart.keywords:
            assert chart.name in {c.name for c in search(word)}, f"{word} misses {chart.name}"


def test_stopwords_do_not_swallow_discriminating_words():
    """ccd, pool and per appear in chart names and must keep working."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import STOPWORDS

    assert not {"ccd", "pool", "per"} & STOPWORDS
    assert search("ccd on exchanges")[0].name == "ccd_on_exchanges"
    assert search("delegators per pool")[0].name == "staking_avg_delegator_per_pool_count"


def test_the_picker_carries_a_way_to_learn_the_terms():
    """The button Telegram draws above the results is where somebody who is
    stuck is actually looking."""
    from ccdexplorer.ccdexplorer_chart_bot.inline import HELP_BUTTON

    assert HELP_BUTTON.text
    assert HELP_BUTTON.start_parameter


# --- the bot's own chat ---------------------------------------------------
#
# Inline mode is for other people's conversations. The private chat is where
# somebody goes to find out what the bot does, and the first version answered
# a plain message there with silence.


class _Message:
    """Records what the handler replied with, instead of calling Telegram."""

    def __init__(self, text):
        self.text = text
        self.photos = []
        self.htmls = []

    async def reply_photo(self, photo, caption=None, parse_mode=None, reply_markup=None):
        self.photos.append((photo, caption, reply_markup))

    async def reply_html(self, text, **kwargs):
        self.htmls.append(text)


class _Update:
    def __init__(self, text):
        self.message = _Message(text)


class _Context:
    bot = type("bot", (), {"username": "ccdexplorer_chart_bot"})()


async def _send(text):
    from ccdexplorer.ccdexplorer_chart_bot.direct import handler

    update = _Update(text)
    await handler(SITE)(update, _Context())
    return update.message


async def test_a_plain_message_returns_the_chart():
    """Open the bot, type a word, get the chart -- the most obvious way to try
    it, and the one that used to do nothing at all."""
    message = await _send("accounts")
    assert message.photos
    photo, caption, _ = message.photos[0]
    assert photo.startswith(f"{SITE}/plots/mainnet/accounts_per_day/image.png")
    assert photo.endswith("?theme=light")
    assert "Accounts per day" in caption


async def test_a_whole_question_works_in_the_chat_too():
    message = await _send("how much is staked")
    assert message.photos
    assert "staking_percentage_staked" in message.photos[0][0]


async def test_each_reply_offers_to_send_it_to_a_chat():
    """So the private chat doubles as a way to find the right chart first."""
    message = await _send("accounts")
    _, _, markup = message.photos[0]
    button = markup.inline_keyboard[0][0]
    assert button.switch_inline_query == "accounts_per_day"


async def test_a_broad_query_is_capped_and_says_so():
    """Otherwise the chat becomes a wall of near-identical line charts."""
    from ccdexplorer.ccdexplorer_chart_bot.direct import MAX_REPLIES

    message = await _send("staking")
    assert len(message.photos) == MAX_REPLIES
    assert message.htmls and "more" in message.htmls[0]


async def test_nonsense_gets_an_answer_rather_than_silence():
    message = await _send("kittens")
    assert not message.photos
    assert message.htmls and "kittens" in message.htmls[0]
