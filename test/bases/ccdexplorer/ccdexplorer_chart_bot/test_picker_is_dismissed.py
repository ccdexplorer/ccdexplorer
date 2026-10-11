"""The menu goes away once it has been used.

/e offers a category, tapping one turns that same message into a list of
charts, and tapping a chart sends the chart -- leaving eleven buttons
sitting in the chat under a question that has been answered. On a phone
that list is most of a screen.

It cannot be replaced in place: Telegram refuses editMessageMedia on a text
message, so the chart has to be its own message and the menu has to be
taken away behind it.
"""

from types import SimpleNamespace

from telegram.error import BadRequest, Forbidden

from ccdexplorer.ccdexplorer_chart_bot import direct


class _Menu:
    """The bot's own "Which one?" message: text, so it carries no photo."""

    photo = ()

    def __init__(self, chat_type="supergroup", delete_raises=None):
        self.chat = SimpleNamespace(type=chat_type)
        self.photos = []
        self.deleted = False
        self._delete_raises = delete_raises
        self.events = []

    async def reply_photo(self, photo, **kw):
        self.photos.append((photo, kw))
        self.events.append("photo")

    async def delete(self):
        self.events.append("delete")
        if self._delete_raises is not None:
            raise self._delete_raises
        self.deleted = True


class _Chart(_Menu):
    """A chart the bot already sent: it has a photo, so it is edited."""

    photo = ("a-photo-size",)


class _Query:
    def __init__(self, data, message, markup_raises=None):
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=42, first_name="Sander", username=None)
        self.answers = []
        self.media = []
        self.texts = []
        self.markups = []
        self._markup_raises = markup_raises

    async def answer(self, text=None, **kw):
        self.answers.append(text)

    async def edit_message_media(self, media, **kw):
        self.media.append((media, kw))

    async def edit_message_text(self, text, **kw):
        self.texts.append((text, kw))

    async def edit_message_reply_markup(self, reply_markup=None, **kw):
        if self.message is not None:
            self.message.events.append("markup")
        if self._markup_raises is not None:
            raise self._markup_raises
        self.markups.append(reply_markup)


async def _tap(data, message, markup_raises=None):
    query = _Query(data, message, markup_raises)
    await direct.callback_handler("https://ccdexplorer.io")(
        SimpleNamespace(callback_query=query),
        SimpleNamespace(bot=SimpleNamespace(username="b")),
    )
    return query


PICK = f"{direct.CALLBACK_PREFIX}staking_validator_count"


async def test_picking_a_chart_takes_the_menu_away():
    menu = _Menu()

    await _tap(PICK, menu)

    assert menu.photos, "no chart was sent"
    assert menu.deleted, "the menu is still sitting in the chat"


async def test_the_chart_is_sent_before_the_menu_goes():
    """The other order loses the chart if the delete raises."""
    menu = _Menu()

    await _tap(PICK, menu)

    assert menu.events == ["photo", "delete"]


async def test_the_chart_does_not_quote_the_menu():
    """It would quote the message that is about to be deleted, and a quote
    of a deleted message is what the reader would be left looking at."""
    menu = _Menu()

    await _tap(PICK, menu)

    assert menu.photos[0][1]["do_quote"] is False


async def test_a_menu_too_old_to_delete_loses_its_buttons_instead():
    """Telegram refuses to delete a message older than 48 hours, which a
    button tapped on a stale menu reaches."""
    menu = _Menu(delete_raises=BadRequest("message can't be deleted"))

    query = await _tap(PICK, menu)

    assert menu.photos, "the chart still has to arrive"
    assert query.markups == [None], "the buttons are still there"


async def test_a_bot_forbidden_from_deleting_also_falls_back():
    menu = _Menu(delete_raises=Forbidden("not enough rights"))

    query = await _tap(PICK, menu)

    assert query.markups == [None]


async def test_both_failing_still_leaves_the_chart_sent():
    """A leftover menu is worth less than an exception out of a button
    press, which Telegram retries."""
    menu = _Menu(delete_raises=BadRequest("nope"))

    await _tap(PICK, menu, markup_raises=BadRequest("also nope"))

    assert menu.photos, "the chart is what the reader asked for"


async def test_swapping_a_period_edits_the_chart_and_deletes_nothing():
    """A chart's own buttons still swap it in place. Deleting there would
    make every tap a new message, which is what editing in place avoids."""
    chart = _Chart()

    query = await _tap(PICK, chart)

    assert query.media, "the chart was not swapped in place"
    assert not chart.deleted
    assert not chart.photos


async def test_an_inline_result_has_no_menu_to_delete():
    """The button is on a result sitting in someone else's chat, where the
    bot has no message of its own."""
    query = await _tap(PICK, None)

    assert query.media, "it is still edited by inline_message_id"


async def test_a_category_that_resolves_straight_to_a_chart_clears_its_menu():
    """g:price has a family default, so the tap sends a chart rather than
    opening a second list -- and the category menu is then what is left
    behind. Same for a category holding a single chart."""
    menu = _Menu()

    await _tap(f"{direct.MENU_PREFIX}price", menu)

    assert menu.photos, "no chart was sent"
    assert menu.deleted


async def test_a_category_holding_one_chart_clears_its_menu_too():
    menu = _Menu()

    await _tap(f"{direct.MENU_PREFIX}plt", menu)

    assert menu.photos, "no chart was sent"
    assert menu.deleted


async def test_opening_the_second_list_keeps_the_message():
    """A category with several charts edits the menu into the next list --
    there is no chart yet, so there is nothing to clear behind."""
    menu = _Menu()

    query = await _tap(f"{direct.MENU_PREFIX}staking", menu)

    assert query.texts, "the second list was not opened"
    assert not menu.deleted
    assert not menu.photos
