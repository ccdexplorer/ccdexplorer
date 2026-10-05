"""/ccd is the whole surface now.

Plain text used to answer with a chart, which made the bot noisy in group
chats. It is gone -- but silence would leave anyone who types "price" with no
chart and no reason, so plain text gets one line pointing at the command.
"""

from types import SimpleNamespace

from ccdexplorer.ccdexplorer_chart_bot import direct
from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS, family_default

SITE = "https://ccdexplorer.io"
# Mirrors direct.CATEGORY_ORDER: the site gallery's categories, plus txs
# for the transactions chart, which the site files under chain.
CATEGORY_ORDER = [
    "price",
    "txs",
    "chain",
    "accounts",
    "staking",
    "exchanges",
    "plt",
    "agents",
]


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
    return SimpleNamespace(bot=SimpleNamespace(username="ccdexplorer_chart_bot"), args=list(args))


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
    """A word matching one thing in `other`, which has no family default.

    Was "exchanges", which matches two charts and now opens the picker. The
    property this guards -- that a chart outside a family is sent as itself
    rather than resolved to some family's default -- needs a word that
    matches exactly one.
    """
    message = await _run("whales")

    assert message.photos, "a chart in other was not sent"
    assert "daily_limits" in message.photos[0][0]


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
    assert any("/c" in h for h in update.message.htmls)


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
    """ "…and 6 more" when two distinct charts remain is a lie."""
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


# --- the command, and the names it used to answer to -----------------------
#
# /c is short enough to collide: any other bot in the same group may claim it.
# PTB ignores /c@otherbot (it compares the part after @ to its own username),
# so the only ambiguous case is a bare /c in a group, where both bots answer.
# /ccd stays registered as the form that cannot be mistaken.


def test_e_is_the_command_that_works():
    import inspect

    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    source = inspect.getsource(main)
    assert 'CommandHandler(["e"], command_handler(site_url))' in source


def test_the_old_commands_are_registered_to_say_so():
    """Unregistering them would make /c do nothing at all -- an unknown
    command is not routed to the plain-text handler either, so anyone who
    learned /c would get silence rather than a redirect."""
    import inspect

    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    source = inspect.getsource(main)
    assert 'CommandHandler(["c", "ccd"], retired_command_handler())' in source


async def test_start_says_what_to_do_when_another_bot_claims_the_command():
    """A group with two bots offering /c is the one case a user cannot resolve
    without being told the qualified form exists."""
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    message = _Message()
    await main.start(SimpleNamespace(message=message), _context())

    assert "/ccd" in message.htmls[0] or "@" in message.htmls[0]


# --- a word that matches more than one chart -------------------------------
#
# /c validator sent two charts, /c staking sent three and said "and 6 more".
# A wall of near-identical line charts is the thing the reader has to sort
# out, and the bot is the one holding the list. So a query that matches more
# than one asks which, and sends nothing until it knows.


async def test_a_word_matching_two_charts_asks_which():
    message = await _run("validator")

    assert not message.photos, "sent charts instead of asking"
    assert message.htmls, "asked nothing"
    markup = message.markups[0]
    assert markup is not None, "no buttons to pick from"
    labels = [b.text for row in markup.inline_keyboard for b in row]
    assert len(labels) == 2, labels


async def test_the_buttons_are_the_charts_that_matched():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families, search

    message = await _run("validator")
    expected = {c.name for c in resolve_families(search("validator"))}

    markup = message.markups[0]
    offered = {
        b.callback_data.removeprefix(direct.CALLBACK_PREFIX)
        for row in markup.inline_keyboard
        for b in row
    }
    assert offered == expected


async def test_the_buttons_are_titles_not_route_names():
    message = await _run("validator")
    labels = [b.text for row in message.markups[0].inline_keyboard for b in row]

    assert "Validator count" in labels
    assert not any("_" in label for label in labels), labels


async def test_one_match_still_arrives_as_a_chart():
    """Asking "which?" about a single answer is a question with one option."""
    for word in ("price", "tps", "whales", "accounts"):
        message = await _run(word)
        assert message.photos, f"{word} no longer sends its chart"
        assert not message.markups or message.markups == [None] * len(message.markups)


async def test_every_match_is_offered_rather_than_three_and_a_note():
    """`staking` matches nine. Three charts and "…and 6 more" made the reader
    retype; nine buttons is the whole answer in one message."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families, search

    message = await _run("staking")
    expected = len(resolve_families(search("staking")))
    assert expected > 3, "pick a word that actually overflows"

    labels = [b.text for row in message.markups[0].inline_keyboard for b in row]
    assert len(labels) == expected
    assert not any("more" in h for h in message.htmls), message.htmls


async def test_the_question_names_the_word_that_was_asked():
    message = await _run("validator")
    assert "validator" in message.htmls[0].lower()


async def test_no_match_is_unchanged():
    message = await _run("zzzznothing")
    assert not message.photos
    assert "No chart matches" in message.htmls[0]


# --- picking from a message that has no photo in it ------------------------
#
# editMessageMedia needs media to edit. The picker is a text message, so a tap
# on one of its buttons has to answer with a new message instead.
#
# That was already broken before the picker existed: `other` has no default,
# so tapping it writes "Which one?" as text, and tapping a chart there went
# straight to edit_message_media -- which Telegram refuses.


class _PhotolessMessage:
    """A text message: `photo` is an empty tuple on a real one."""

    photo = ()

    def __init__(self):
        self.photos = []

    async def reply_photo(self, photo, **kw):
        self.photos.append((photo, kw))


class _PhotoMessage(_PhotolessMessage):
    photo = ("a-photo-size",)


async def _tap_on(data, message):
    query = _CallbackQuery(data)
    query.message = message
    await direct.callback_handler(SITE)(SimpleNamespace(callback_query=query), _context())
    return query


async def test_picking_from_a_text_message_sends_a_new_chart():
    message = _PhotolessMessage()
    query = await _tap_on(f"{direct.CALLBACK_PREFIX}staking_validator_count", message)

    assert not query.media, "edit_message_media on a text message is refused by Telegram"
    assert message.photos, "nothing was sent"
    assert "staking_validator_count" in message.photos[0][0]


async def test_the_chart_it_sends_carries_its_own_buttons():
    message = _PhotolessMessage()
    await _tap_on(f"{direct.CALLBACK_PREFIX}staking_validator_count", message)

    markup = message.photos[0][1]["reply_markup"]
    assert markup is not None and markup.inline_keyboard


async def test_picking_from_a_chart_still_swaps_it_in_place():
    """The period and grouping buttons sit on a photo, and replacing the
    message for each tap would fill the chat with near-identical charts."""
    message = _PhotoMessage()
    query = await _tap_on(f"{direct.CALLBACK_PREFIX}staking_validator_count", message)

    assert query.media, "the in-place swap is gone"
    assert not message.photos, "sent a new message as well as editing"


async def test_the_category_which_one_path_now_answers():
    """`/e` -> staking -> "Which one?" -> a chart. The last step errored."""
    menu = await _tap(f"{direct.MENU_PREFIX}staking")
    first = menu.messages[0][1]["reply_markup"].inline_keyboard[0][0]

    message = _PhotolessMessage()
    query = await _tap_on(first.callback_data, message)

    assert message.photos, "the category picker still leads nowhere"
    assert not query.media


# --- the command is /e now -------------------------------------------------
#
# /c was two things at once: short enough to be convenient and short enough
# that another bot in the same group claims it. /e is for explorer.


async def test_the_old_command_points_at_the_new_one():
    update, ctx = _update("/c price"), _context("price")

    await direct.retired_command_handler()(update, ctx)

    assert not update.message.photos, "the old command still did the work"
    assert any("/e" in html for html in update.message.htmls), update.message.htmls


async def test_the_nudge_points_at_the_new_command():
    update, ctx = _update("price"), _context()

    await direct.nudge_handler()(update, ctx)

    said = update.message.htmls[0]
    assert "/e" in said
    assert "/c " not in said and "/c<" not in said, said


async def test_an_empty_command_offers_the_categories_under_the_new_name():
    message = await _run()

    assert message.markups[0] is not None, "no category menu"
    assert any("/e" in html for html in message.htmls), message.htmls


async def test_start_teaches_the_new_command():
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    message = _Message()
    await main.start(SimpleNamespace(message=message), _context())
    said = message.htmls[0]

    assert "/e " in said or "/e&" in said or "/e<" in said, said[:200]


async def test_start_still_says_what_to_do_when_another_bot_claims_it():
    """/e is shorter than /c, so the collision it warned about is more
    likely rather than less. The qualified form has to be the new one."""
    import ccdexplorer.ccdexplorer_chart_bot.__main__ as main

    message = _Message()
    await main.start(SimpleNamespace(message=message), _context())

    assert "/e@" in message.htmls[0]


async def test_the_no_match_reply_names_the_new_command_too():
    """The one place /c survived: a dead end that told the reader to try a
    command the bot no longer does anything with."""
    message = await _run("zzzznothing")

    said = message.htmls[0]
    assert "/e" in said
    assert "/c<" not in said and "/c " not in said, said


# --- the category menu names what is behind each button --------------------
#
# /e offered price, chain, PLT, agents and other. "chain" held exactly one
# chart -- Transactions -- so the most-asked-for chart on the bot was behind
# a button whose name did not say so, and anyone who looked under "other"
# for it found seventeen charts and no transactions.


def test_the_menu_offers_txs_as_well_as_chain():
    """chain is back, but holding the other three chain charts rather than
    standing in for the transactions one."""
    assert "txs" in direct.CATEGORY_ORDER
    assert "chain" in direct.CATEGORY_ORDER
    assert [c.title for c in CHARTS if c.group == "txs"] == ["Transactions"]
    assert "Transactions" not in [c.title for c in CHARTS if c.group == "chain"]


def test_the_transactions_chart_is_the_one_behind_it():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import BY_NAME

    assert BY_NAME["transactions_count"].group == "txs"


async def test_tapping_txs_sends_the_transactions_chart():
    query = await _tap(f"{direct.MENU_PREFIX}txs")

    assert query.media or query.messages, "the txs button answered nothing"
    if query.media:
        assert "transactions_count" in query.media[0][0].media


def test_the_menu_reaches_every_chart():
    """Completeness, asked of the whole catalogue rather than eyeballed.

    Every chart has to sit behind one of the buttons, or it can only be
    found by already knowing its name.
    """
    buttons = {
        b.callback_data.removeprefix(direct.MENU_PREFIX)
        for row in direct.category_menu().inline_keyboard
        for b in row
    }
    unreachable = [c.name for c in CHARTS if c.group not in buttons]

    assert unreachable == [], unreachable


def test_every_button_has_something_behind_it():
    """The other direction: a button leading to an empty list is a promise
    of nothing."""
    groups = {c.group for c in CHARTS}
    empty = [g for g in direct.CATEGORY_ORDER if g not in groups]

    assert empty == [], empty


# --- the menu uses the site's categories -----------------------------------
#
# "other" was a junk drawer of seventeen: staking, accounts and exchanges
# charts in one undifferentiated list, while the gallery on the site had
# already sorted exactly those into categories. Two taxonomies for one set
# of charts, and the worse one was the one in the bot.


def test_there_is_no_junk_drawer_left():
    assert "other" not in direct.CATEGORY_ORDER
    assert [c.name for c in CHARTS if c.group == "other"] == []


def test_the_menu_is_the_site_categories_plus_txs():
    assert direct.CATEGORY_ORDER == (
        "price",
        "txs",
        "chain",
        "accounts",
        "staking",
        "exchanges",
        "plt",
        "agents",
    )


def test_each_chart_sits_where_the_site_puts_it():
    """One taxonomy, read off the registry rather than kept by hand.

    Two deliberate departures: the Kraken intervals are "price" in the bot
    because they are one chart at seven intervals rather than seven
    charts, and transactions_count is "txs" because it is the chart people
    open the bot for and "chain" did not say so.
    """
    from ccdexplorer.charts.registry import spec_for_plot

    departures = {"transactions_count": "txs"}
    wrong = []
    for chart in CHARTS:
        spec = spec_for_plot(chart.name)
        if spec is None or chart.period:
            continue
        expected = departures.get(chart.name, spec.category)
        if chart.group != expected:
            wrong.append((chart.name, chart.group, expected))

    assert wrong == [], wrong


async def test_a_category_holding_several_offers_them_all():
    """staking is nine now, where it used to be nine of seventeen."""
    staking = [c for c in CHARTS if c.group == "staking"]
    assert len(staking) > 1

    query = await _tap(f"{direct.MENU_PREFIX}staking")
    labels = [b.text for row in query.messages[0][1]["reply_markup"].inline_keyboard for b in row]

    assert sorted(labels) == sorted(c.title for c in staking)


async def test_a_category_holding_one_sends_it_straight_away():
    """No point asking "which one?" about a list of one."""
    query = await _tap(f"{direct.MENU_PREFIX}agents")

    assert query.media, "agents asked instead of answering"
    assert not query.messages
