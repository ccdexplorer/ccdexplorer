"""Answering a plain message in the bot's own chat.

The first version handled inline queries and two commands, which meant the most
natural way to try the bot -- open it, type "accounts" -- did nothing at all,
silently. Inline mode is for other people's conversations; this is for the one
place someone goes to find out what the bot does.

The reply carries a button that hands the same chart to a chat, so the private
chat doubles as a way to find the chart you want before sending it somewhere
it matters.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode

from .catalogue import BY_NAME, Chart, search, siblings

log = logging.getLogger(__name__)

#: More than this and the chat becomes a wall of near-identical line charts.
#: The rest are a query away, and the reply says so.
MAX_REPLIES = 3


#: Prefix on callback_data, which Telegram caps at 64 bytes. Chart names are
#: well inside that, so the name itself is the payload.
CALLBACK_PREFIX = "c:"

#: Telegram will render more, unreadably narrow.
BUTTONS_PER_ROW = 4


def keyboard_for(chart: Chart, send_button: bool = True) -> InlineKeyboardMarkup:
    """Interval buttons above, and a way to send the chart onward below.

    A chart with no siblings gets only the send button -- an interval row with
    one entry is a button that does nothing.
    """
    rows = []
    family = siblings(chart)
    if len(family) > 1:
        buttons = [
            InlineKeyboardButton(
                f"· {sibling.period} ·" if sibling.name == chart.name else sibling.period,
                callback_data=f"{CALLBACK_PREFIX}{sibling.name}",
            )
            for sibling in family
        ]
        # Four to a row. Seven intervals in one row is unreadable at phone
        # width, which is where these are looked at.
        rows.extend(
            buttons[i : i + BUTTONS_PER_ROW] for i in range(0, len(buttons), BUTTONS_PER_ROW)
        )
    if send_button:
        rows.append([InlineKeyboardButton("Send to a chat", switch_inline_query=chart.name)])
    return InlineKeyboardMarkup(rows)


def caption(chart: Chart, site_url: str) -> str:
    return f"<b>{chart.title}</b> — {chart.description}\n{chart.page_url(site_url)}"


def handler(site_url: str):
    """Build the message handler, closing over where the charts are served from."""

    async def reply_with_chart(update, context) -> None:
        message = update.message
        if message is None or not message.text:
            return

        query = message.text.strip()
        matches = search(query)
        log.info("message %r -> %d match(es)", query, len(matches))

        if not matches:
            username = context.bot.username or "ccdexplorer_chart_bot"
            await message.reply_html(
                f"No chart matches “{query}”.\n\n"
                "Try <i>validators</i>, <i>delegators</i>, <i>fees</i>, "
                "<i>accounts</i> or <i>exchanges</i> — or /start for the full list.\n\n"
                f"In any chat, type <code>@{username}</code> to browse them.",
                disable_web_page_preview=True,
            )
            return

        for chart in matches[:MAX_REPLIES]:
            await message.reply_photo(
                photo=chart.image_url(site_url),
                caption=caption(chart, site_url),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard_for(chart),
            )

        if len(matches) > MAX_REPLIES:
            await message.reply_html(
                f"…and {len(matches) - MAX_REPLIES} more. "
                "Narrow it down, or /start for the full list."
            )

    return reply_with_chart


def callback_handler(site_url: str):
    """Swap the chart in place when an interval button is tapped.

    editMessageMedia re-fetches the URL, so every tap is a request to the site.
    The plot cache and its warmer already cover that, which is what makes this
    cheap enough to be a button rather than a new message.
    """

    async def switch_interval(update, context) -> None:
        query = update.callback_query
        if query is None or not (query.data or "").startswith(CALLBACK_PREFIX):
            return
        chart = BY_NAME.get(query.data[len(CALLBACK_PREFIX) :])
        if chart is None:
            # Telegram will keep showing the spinner unless the query is
            # answered, so a stale button gets a reason rather than a hang.
            await query.answer("That chart is no longer available.")
            return

        log.info("button %r", chart.name)
        await query.answer()
        await query.edit_message_media(
            media=InputMediaPhoto(
                media=chart.image_url(site_url),
                caption=caption(chart, site_url),
                parse_mode=ParseMode.HTML,
            ),
            reply_markup=keyboard_for(chart),
        )

    return switch_interval
