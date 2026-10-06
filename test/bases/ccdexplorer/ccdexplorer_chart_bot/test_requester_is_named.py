"""A chart sent by a button says who asked for it.

Telegram delivers a button press to the bot alone: the group sees no
"X tapped staking", no trace at all. And the chart arrives as a reply to
the bot's own "Which one?" menu, so nothing in the message connects it to
the person who asked. In a busy group a chart simply appears.

The bot is the only one who knows -- callback_query.from_user -- so it
says. Only in groups: in a private chat there is exactly one person it
could be.
"""

from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_chart_bot import direct


def _query(chat_type="supergroup", first_name="Sander", user_id=42, username=None):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, first_name=first_name, username=username),
        message=SimpleNamespace(chat=SimpleNamespace(type=chat_type)),
    )


def test_a_group_chart_names_who_asked():
    note = direct.requester_note(_query())

    assert "asked by" in note
    assert "Sander" in note
    assert 'href="tg://user?id=42"' in note


def test_a_private_chart_says_nothing():
    """There is one person in the chat and they just pressed the button."""
    assert direct.requester_note(_query(chat_type="private")) == ""


def test_an_inline_result_says_nothing():
    """No message means the button is on an inline result in someone
    else's chat -- where the person who sent it is already named by
    Telegram, above the message."""
    query = SimpleNamespace(from_user=SimpleNamespace(id=1, first_name="X", username=None))
    query.message = None

    assert direct.requester_note(query) == ""


def test_a_name_cannot_break_the_caption():
    """Display names are user-supplied and the caption is HTML. A name
    containing a tag would make Telegram reject the whole message, so the
    chart would not arrive at all."""
    note = direct.requester_note(_query(first_name="<b>boss</b>"))

    assert "<b>boss</b>" not in note
    assert "&lt;b&gt;boss&lt;/b&gt;" in note


def test_a_user_with_no_first_name_still_gets_named():
    note = direct.requester_note(_query(first_name=None, username="sander"))

    assert "sander" in note


@pytest.mark.parametrize("chat_type", ["group", "supergroup"])
def test_both_kinds_of_group_are_covered(chat_type):
    assert direct.requester_note(_query(chat_type=chat_type)) != ""


# --- and the handler actually uses it --------------------------------------


class _Message:
    """A text message in a group: the "Which one?" menu."""

    photo = ()

    def __init__(self, chat_type="supergroup"):
        self.chat = SimpleNamespace(type=chat_type)
        self.photos = []

    async def reply_photo(self, photo, **kw):
        self.photos.append((photo, kw))


class _PhotoMessage(_Message):
    photo = ("a-photo-size",)


class _CallbackQuery:
    def __init__(self, data, message):
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=42, first_name="Sander", username=None)
        self.answers = []
        self.media = []
        self.messages = []

    async def answer(self, text=None, **kw):
        self.answers.append(text)

    async def edit_message_media(self, media, **kw):
        self.media.append((media, kw))

    async def edit_message_text(self, text, **kw):
        self.messages.append((text, kw))


async def _tap(data, message):
    query = _CallbackQuery(data, message)
    await direct.callback_handler("https://ccdexplorer.io")(
        SimpleNamespace(callback_query=query),
        SimpleNamespace(bot=SimpleNamespace(username="b")),
    )
    return query


async def test_a_chart_picked_from_the_menu_names_the_asker():
    """The case in the screenshot: a group, a "Which one?" list, a tap."""
    message = _Message()

    await _tap(f"{direct.CALLBACK_PREFIX}staking_validator_count", message)

    caption = message.photos[0][1]["caption"]
    assert "asked by" in caption and "Sander" in caption


async def test_swapping_the_period_names_whoever_swapped_it():
    """The chart is edited in place, so the caption is the only place the
    group can learn who changed it."""
    query = await _tap(f"{direct.CALLBACK_PREFIX}staking_validator_count", _PhotoMessage())

    assert "asked by" in query.media[0][0].caption


async def test_a_private_chat_caption_is_left_alone():
    message = _Message(chat_type="private")

    await _tap(f"{direct.CALLBACK_PREFIX}staking_validator_count", message)

    assert "asked by" not in message.photos[0][1]["caption"]


async def test_the_chart_itself_is_unchanged():
    """The note is an addition, not a replacement."""
    message = _Message()

    await _tap(f"{direct.CALLBACK_PREFIX}staking_validator_count", message)

    caption = message.photos[0][1]["caption"]
    assert "Validator count" in caption
    assert "/mainnet/charts/" in caption
