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

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .catalogue import Chart, search

log = logging.getLogger(__name__)

#: More than this and the chat becomes a wall of near-identical line charts.
#: The rest are a query away, and the reply says so.
MAX_REPLIES = 3


def send_button(chart: Chart) -> InlineKeyboardMarkup:
    """A button that opens a chat picker with this chart's query filled in."""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Send to a chat", switch_inline_query=chart.name)]]
    )


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
                reply_markup=send_button(chart),
            )

        if len(matches) > MAX_REPLIES:
            await message.reply_html(
                f"…and {len(matches) - MAX_REPLIES} more. "
                "Narrow it down, or /start for the full list."
            )

    return reply_with_chart
