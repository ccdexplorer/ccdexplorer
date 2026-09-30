"""/ccd is the whole surface now.

Plain text used to answer with a chart, which made the bot noisy in group
chats. It is gone -- but silence would leave anyone who types "price" with no
chart and no reason, so plain text gets one line pointing at the command.
"""

from types import SimpleNamespace

from ccdexplorer.ccdexplorer_chart_bot import direct
from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS, family_default

SITE = "https://ccdexplorer.io"
CATEGORY_ORDER = ["price", "txs", "agents", "tvl", "other"]


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


def _update(text=""):
    return SimpleNamespace(message=_Message(text))


def _context(*args):
    return SimpleNamespace(
        bot=SimpleNamespace(username="ccdexplorer_chart_bot"), args=list(args)
    )


async def _run(*args):
    update, ctx = _update(), _context(*args)
    await direct.command_handler(SITE)(update, ctx)
    return update.message


async def test_a_word_sends_that_chart():
    message = await _run("price")

    assert message.photos, "no chart was sent"


async def test_a_family_word_opens_the_family_default():
    message = await _run("price")

    assert family_default("price").name in message.photos[0][0]


async def test_a_non_family_chart_is_sent_as_itself():
    """A word matching something in `other`, which has no family default."""
    message = await _run("exchanges")

    assert message.photos, "a chart in other was not sent"


async def test_a_word_matching_nothing_says_so():
    message = await _run("zzzznothing")

    assert not message.photos
    assert message.htmls, "said nothing at all"


async def test_bare_ccd_offers_the_categories_in_order():
    """Derived from the catalogue: txs, agents and tvl arrive in later tasks."""
    message = await _run()
    expected = [g for g in CATEGORY_ORDER if any(c.group == g for c in CHARTS)]

    markup = message.markups[0]
    assert markup is not None, "no menu keyboard"
    labels = [b.text for row in markup.inline_keyboard for b in row]
    assert labels == expected


async def test_bare_ccd_sends_no_chart():
    """The menu is a choice, not a chart nobody asked for."""
    message = await _run()

    assert not message.photos


async def test_a_multi_word_query_is_joined():
    message = await _run("ccd", "price")

    assert message.photos


async def test_plain_text_gets_a_nudge_and_no_chart():
    update, ctx = _update("price"), _context()

    await direct.nudge_handler()(update, ctx)

    assert not update.message.photos, "plain text still sent a chart"
    assert any("/ccd" in h for h in update.message.htmls)


# --- the category menu's buttons -------------------------------------------


class _CallbackQuery:
    def __init__(self, data):
        self.data = data
        self.answers = []
        self.media = []
        self.messages = []

    async def answer(self, text=None, **kw):
        self.answers.append(text)

    async def edit_message_media(self, media, **kw):
        self.media.append((media, kw))

    async def edit_message_text(self, text, **kw):
        self.messages.append((text, kw))


async def _tap(data):
    query = _CallbackQuery(data)
    await direct.callback_handler(SITE)(SimpleNamespace(callback_query=query), _context())
    return query


async def test_tapping_a_family_category_sends_its_default():
    query = await _tap(f"{direct.MENU_PREFIX}price")

    assert query.media, "no chart swapped in"
    assert family_default("price").name in query.media[0][0].media


async def test_tapping_other_offers_the_charts_in_it():
    query = await _tap(f"{direct.MENU_PREFIX}other")
    others = [c for c in CHARTS if c.group == "other"]

    assert query.messages, "other offered nothing"
    markup = query.messages[0][1]["reply_markup"]
    labels = [b.text for row in markup.inline_keyboard for b in row]
    assert len(labels) == len(others)


async def test_tapping_an_unknown_category_is_answered_not_hung():
    """Telegram spins forever unless the callback query is answered."""
    query = await _tap(f"{direct.MENU_PREFIX}nonsense")

    assert query.answers, "the spinner would never stop"
    assert not query.media


async def test_an_interval_button_still_works():
    """The existing chart-name callback must not be swallowed by the new prefix."""
    query = await _tap(f"{direct.CALLBACK_PREFIX}ccd_kraken_1d")

    assert query.media
    assert "ccd_kraken_1d" in query.media[0][0].media


# --- review finding 1: the advertised command sent the same chart three times


async def test_a_family_word_sends_the_chart_once():
    """/ccd price is what the nudge tells everyone to type.

    It matched all seven Kraken intervals, resolved each to the family default
    and sent that default once per match.
    """
    message = await _run("price")

    sent = [p for p, _kw in message.photos]
    assert len(sent) == len(set(sent)), f"sent the same chart more than once: {sent}"


async def test_the_overflow_count_counts_what_is_left_after_collapsing():
    """"…and 6 more" when two distinct charts remain is a lie."""
    message = await _run("price")

    assert not message.htmls, f"claimed there were more: {message.htmls}"


async def test_a_word_matching_two_families_sends_one_of_each():
    message = await _run("ccd")
    sent = [p for p, _kw in message.photos]

    assert len(sent) == len(set(sent))


# --- review findings 5 and 6 -----------------------------------------------


def test_the_nudge_only_answers_private_chats():
    """Answering every plain message is the noise this branch set out to remove.

    If the bot's privacy mode is off it sees every group message, and the nudge
    would reply to all of them -- one for one with the behaviour being removed.
    """
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    source = __import__("inspect").getsource(main)
    assert "filters.ChatType.PRIVATE" in source, "the nudge answers group chats"


async def test_start_does_not_claim_more_charts_than_the_picker_shows():
    """It said "all 37 charts"; the picker collapsed families and shows 22."""
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main
    from ccdexplorer.ccdexplorer_chart_bot.inline import pickable

    message = _Message()
    await main.start(SimpleNamespace(message=message), _context())
    text = message.htmls[0]

    assert f"all {len(CHARTS)} charts" not in text, "still claims the pre-collapse count"
    assert str(len(pickable(CHARTS))) in text


async def test_start_mentions_the_command():
    """Plain text now tells people to use /ccd; the help must know it exists."""
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    message = _Message()
    await main.start(SimpleNamespace(message=message), _context())

    assert "/ccd" in message.htmls[0]
