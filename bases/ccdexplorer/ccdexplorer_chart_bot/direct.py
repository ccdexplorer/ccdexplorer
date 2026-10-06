"""Answering a plain message in the bot's own chat.

The first version handled inline queries and two commands, which meant the most
natural way to try the bot -- open it, type "accounts" -- did nothing at all,
silently. Inline mode is for other people's conversations; this is for the one
place someone goes to find out what the bot does.

The reply carries a button that hands the same chart to a chat, so the private
chat doubles as a way to find the chart you want before sending it somewhere
it matters.
"""

import html
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode

from ccdexplorer.charts import ChartState

from .catalogue import (
    CALLBACK_PREFIX,
    CHARTS,
    Chart,
    callback_data,
    family_default,
    image_url,
    page_url,
    parse_callback,
    resolve_families,
    search,
    siblings,
    state_for,
)

log = logging.getLogger(__name__)

#: Telegram will render more, unreadably narrow.
BUTTONS_PER_ROW = 4

#: Prefix for the category menu's buttons, kept distinct from CALLBACK_PREFIX
#: so a tap on a category cannot be read as a chart name.
MENU_PREFIX = "g:"

#: The order the categories read in. price and txs first because they are
#: what most people open the bot for.
#:
#: These are the site gallery's categories, so one set of charts is sorted
#: one way rather than two. "other" used to hold seventeen -- staking,
#: accounts and exchanges charts in one undifferentiated list -- while the
#: gallery had already sorted exactly those.
#:
#: txs is the one departure: transactions_count is "chain" on the site,
#: but each name here is the whole of a button's label, and "chain" said
#: nothing about the transactions behind it.
CATEGORY_ORDER = (
    "price",
    "txs",
    "chain",
    "accounts",
    "staking",
    "exchanges",
    "plt",
    "agents",
)


def keyboard_for(
    chart: Chart, state: ChartState | None = None, send_button: bool = True
) -> InlineKeyboardMarkup:
    """The rows this chart can actually offer, and a way to send it onward.

    A spec-backed chart gets a window row and a grouping row built from what
    its spec allows. One that has not been migrated keeps its interval family,
    if it has one. A row with a single entry is a button that does nothing, so
    it is absent rather than empty.
    """
    rows = []
    spec = chart.spec
    if spec is not None:
        if state is None:
            state = state_for(chart)
        if len(spec.windows) > 1:
            rows.append(
                [
                    InlineKeyboardButton(
                        f"· {w.value} ·" if w is state.window else w.value,
                        callback_data=callback_data(chart, w, state.grouping),
                    )
                    for w in spec.windows
                ]
            )
        # Absent, not dead: a chart whose every series is a closing value
        # redraws the same measurement at a different point count, so the
        # grouping is a resolution the span answers better than a button --
        # and over thirty days a monthly button answers with two points.
        if len(spec.groupings) > 1 and not spec.automatic_grouping:
            rows.append(
                [
                    InlineKeyboardButton(
                        f"· {g.value} ·" if g is state.grouping else g.value,
                        callback_data=callback_data(chart, state.window, g),
                    )
                    for g in spec.groupings
                ]
            )
    else:
        family = siblings(chart)
        if len(family) > 1:
            buttons = [
                InlineKeyboardButton(
                    f"· {s.period} ·" if s.name == chart.name else s.period,
                    callback_data=f"{CALLBACK_PREFIX}{s.name}",
                )
                for s in family
            ]
            # Four to a row. Seven intervals in one row is unreadable at phone
            # width, which is where these are looked at.
            rows.extend(
                buttons[i : i + BUTTONS_PER_ROW] for i in range(0, len(buttons), BUTTONS_PER_ROW)
            )
    if send_button:
        rows.append([InlineKeyboardButton("Send to a chat", switch_inline_query=chart.name)])
    return InlineKeyboardMarkup(rows)


def chart_picker(charts) -> InlineKeyboardMarkup:
    """One button per chart, by title.

    One to a row: "Average delegator stake" and "Distribution of rewards" do
    not fit beside anything at phone width, and these are read on phones.

    The callback is the plain chart name, the form parse_callback has always
    understood, so a picked chart opens in its default state and its own
    buttons take it from there.
    """
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(chart.title, callback_data=f"{CALLBACK_PREFIX}{chart.name}")]
            for chart in charts
        ]
    )


def category_menu() -> InlineKeyboardMarkup:
    """One button per category, in a fixed order.

    Built from the catalogue rather than a literal list, so a category that
    does not exist yet does not offer a button that answers nothing.
    """
    present = [g for g in CATEGORY_ORDER if any(c.group == g for c in CHARTS)]
    buttons = [
        InlineKeyboardButton(group, callback_data=f"{MENU_PREFIX}{group}") for group in present
    ]
    return InlineKeyboardMarkup(
        [buttons[i : i + BUTTONS_PER_ROW] for i in range(0, len(buttons), BUTTONS_PER_ROW)]
    )


def caption(chart: Chart, site_url: str, state: ChartState | None = None) -> str:
    return f"<b>{chart.title}</b> — {chart.description}\n{page_url(chart, site_url, state)}"


#: Chats where more than one person could have asked.
GROUP_CHATS = ("group", "supergroup")


def requester_note(query) -> str:
    """Who pressed the button, for a caption, or "" where it adds nothing.

    Telegram delivers a button press to the bot alone -- the group sees no
    trace of it -- and the chart arrives as a reply to the bot's own
    "Which one?" menu rather than to anything the asker wrote. So a chart
    appears in a busy group attached to nobody. The bot is the only one
    who knows, and this is it saying so.

    Silent in a private chat, where there is one person it could be, and
    on an inline result, where Telegram already names the sender above the
    message.
    """
    message = getattr(query, "message", None)
    chat = getattr(message, "chat", None)
    if getattr(chat, "type", None) not in GROUP_CHATS:
        return ""

    user = getattr(query, "from_user", None)
    if user is None:
        return ""

    # Escaped: a display name is whatever its owner typed, the caption is
    # HTML, and a name containing a tag would have Telegram reject the
    # message rather than render it -- so the chart would not arrive.
    name = html.escape(user.first_name or user.username or "someone")
    # A tg:// mention rather than @username, which not everyone has.
    return f'\n<i>asked by <a href="tg://user?id={user.id}">{name}</a></i>'


def caption_for_button(chart: Chart, site_url: str, state, query) -> str:
    """The caption a chart gets when a button produced it."""
    return caption(chart, site_url, state) + requester_note(query)


def command_handler(site_url: str):
    """/e <word> sends a chart; /e on its own offers the categories."""

    async def reply_to_command(update, context) -> None:
        message = update.message
        if message is None:
            return

        query = " ".join(getattr(context, "args", None) or []).strip()
        if not query:
            await message.reply_html(
                "Which chart? Pick a category, or say <code>/e &lt;word&gt;</code> "
                "— for example <code>/e staking</code>.",
                reply_markup=category_menu(),
            )
            return

        # Collapsed before the slice, not after: "price" matches all seven
        # Kraken intervals, and resolving each one afterwards sent the same
        # default once per match -- three identical charts for the command the
        # nudge tells everyone to type.
        matches = resolve_families(search(query))
        log.info("/e %r -> %d chart(s)", query, len(matches))
        # Escaped because the reply is HTML and the word is the reader's: a
        # query containing < made Telegram reject the message outright, so
        # the bot answered a slightly odd question with silence.
        asked = html.escape(query)
        if not matches:
            await message.reply_html(f"No chart matches “{asked}”. Try <code>/e</code> on its own.")
            return

        # More than one match is a question, not an answer. "validator" sent
        # two charts and "staking" sent three and a note saying six more
        # existed, which left the reader to sort out a wall of near-identical
        # line charts -- while the bot was the one holding the list.
        if len(matches) > 1:
            await message.reply_html(
                f"{len(matches)} charts match “{asked}”. Which one?",
                reply_markup=chart_picker(matches),
            )
            return

        chart = matches[0]
        state = state_for(chart)
        await message.reply_photo(
            photo=image_url(chart, site_url, state),
            caption=caption(chart, site_url, state),
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard_for(chart, state),
        )

    return reply_to_command


def retired_command_handler():
    """/c and /ccd say where the command went.

    Left registered rather than removed. An unknown command is not routed
    to the plain-text handler either, so unregistering these would answer
    anyone who learned /c with silence -- which reads as the bot being
    broken rather than as the command having moved.
    """

    async def point_at_the_new_command(update, context) -> None:
        message = update.message
        if message is None:
            return
        await message.reply_html(
            "I answer <code>/e</code> now — try <code>/e price</code>, "
            "or <code>/e</code> on its own for the list."
        )

    return point_at_the_new_command


def nudge_handler():
    """Plain text no longer searches -- it points at the command.

    Going silent would leave anyone who types "price" today with no chart and
    no reason, which reads as the bot being broken.
    """

    async def nudge(update, context) -> None:
        message = update.message
        if message is None or not message.text:
            return
        await message.reply_html(
            "I answer commands now — try <code>/e price</code>, "
            "or <code>/e</code> on its own for the list."
        )

    return nudge


def callback_handler(site_url: str):
    """Swap the chart in place when an interval button is tapped.

    editMessageMedia re-fetches the URL, so every tap is a request to the site.
    The plot cache and its warmer already cover that, which is what makes this
    cheap enough to be a button rather than a new message.
    """

    async def switch_interval(update, context) -> None:
        query = update.callback_query
        if query is None:
            return
        data = query.data or ""

        state = None
        if data.startswith(MENU_PREFIX):
            group = data[len(MENU_PREFIX) :]
            chart = family_default(group)
            if chart is None:
                members = [c for c in CHARTS if c.group == group]
                if not members:
                    # Answered rather than ignored: Telegram spins on an
                    # unanswered callback query until it times out.
                    await query.answer("That category is no longer available.")
                    return
                if len(members) == 1:
                    chart = members[0]
                else:
                    # A category with no default is a bag of unrelated charts,
                    # so the tap opens a second menu rather than guessing.
                    await query.answer()
                    buttons = [
                        InlineKeyboardButton(c.title, callback_data=f"{CALLBACK_PREFIX}{c.name}")
                        for c in members
                    ]
                    await query.edit_message_text(
                        "Which one?",
                        reply_markup=InlineKeyboardMarkup(
                            [buttons[i : i + 1] for i in range(len(buttons))]
                        ),
                    )
                    return
            state = state_for(chart)
        else:
            resolved = parse_callback(data)
            if resolved is None:
                # Telegram keeps showing the spinner unless the query is
                # answered, so a stale button gets a reason rather than a hang.
                await query.answer("That chart is no longer available.")
                return
            chart, state = resolved
            if state is None:
                state = state_for(chart)
        log.info("button %r", chart.name)
        await query.answer()

        # editMessageMedia needs media to edit, and Telegram refuses a text
        # message outright. The picker and the category's "Which one?" are
        # both text, so a tap there answers with a new message; a tap on a
        # chart's own period or grouping button swaps that chart in place,
        # because replacing it each time would fill the chat with near
        # identical pictures.
        # No message at all means the button is on an inline result sitting in
        # someone else's chat, where there is nothing to reply to and
        # edit_message_media addresses it by inline_message_id instead.
        picked_from = getattr(query, "message", None)
        if picked_from is not None and not getattr(picked_from, "photo", None):
            await query.message.reply_photo(
                photo=image_url(chart, site_url, state),
                caption=caption_for_button(chart, site_url, state, query),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard_for(chart, state),
            )
            return

        await query.edit_message_media(
            media=InputMediaPhoto(
                media=image_url(chart, site_url, state),
                caption=caption_for_button(chart, site_url, state, query),
                parse_mode=ParseMode.HTML,
            ),
            reply_markup=keyboard_for(chart, state),
        )

    return switch_interval
