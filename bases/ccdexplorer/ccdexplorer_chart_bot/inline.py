"""Answering inline queries: @ccdexplorer_chart_bot <something> in any chat.

Inline mode was chosen over a Mini App for the first version because it is how
a chart actually spreads. Nobody opens an app to share a picture; they type
into the conversation they are already having, and the chart arrives as a photo
they can react to.

The bot fetches nothing and renders nothing. Telegram is handed a URL and
collects the image itself, so this process holds no chain data, no database
connection and no credentials beyond its own token -- and a chart it has never
seen still works the moment the site can draw it.
"""

from telegram import (
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InlineQueryResultsButton,
    InputTextMessageContent,
)
from telegram.constants import ParseMode

from .catalogue import Chart, search

#: How long Telegram may reuse an answer. The plots behind it are cached for an
#: hour by the site, so anything under that costs nothing and spares both ends
#: the round trip while somebody is still typing.
CACHE_SECONDS = 300

#: 1200x630 is what the site renders; telling Telegram up front stops it
#: guessing and reflowing the bubble once the image lands.
PHOTO_WIDTH = 1200
PHOTO_HEIGHT = 630


def photo_result(chart: Chart, site_url: str) -> InlineQueryResultPhoto:
    """One chart, as a photo Telegram will fetch for itself."""
    image = chart.image_url(site_url)
    return InlineQueryResultPhoto(
        id=chart.name,
        photo_url=image,
        thumbnail_url=image,
        photo_width=PHOTO_WIDTH,
        photo_height=PHOTO_HEIGHT,
        title=chart.title,
        description=chart.description,
        caption=f"<b>{chart.title}</b> — {chart.description}\n{chart.page_url(site_url)}",
        parse_mode=ParseMode.HTML,
    )


def nothing_found(query: str, site_url: str) -> InlineQueryResultArticle:
    """Something to select when the query matches no chart.

    An empty answer renders as a silent, empty panel, which reads as the bot
    being broken rather than the query being wrong.
    """
    return InlineQueryResultArticle(
        id="no-match",
        title=f"No chart matches “{query}”",
        description="Try validators, delegators, fees, accounts, exchanges",
        input_message_content=InputTextMessageContent(
            f"No ccdexplorer chart matches “{query}”.\n{site_url.rstrip('/')}/mainnet/statistics"
        ),
    )


def results_for(query: str, site_url: str) -> list:
    matches = search(query)
    if not matches:
        return [nothing_found(query, site_url)]
    return [photo_result(chart, site_url) for chart in matches]


#: Telegram draws this above the results, inside the picker. It is the only
#: place a reader is actually looking when they are trying to work out what to
#: type, so the answer to "which words select which chart" belongs here rather
#: than in a help command nobody opens.
HELP_BUTTON = InlineQueryResultsButton(text="What can I ask for?", start_parameter="charts")


def handler(site_url: str):
    """Build the inline handler, closing over where the charts are served from."""

    async def answer_inline_query(update, context) -> None:
        inline_query = update.inline_query
        if inline_query is None:
            return
        await inline_query.answer(
            results_for(inline_query.query or "", site_url),
            cache_time=CACHE_SECONDS,
            button=HELP_BUTTON,
            # The answer depends only on the query, never on who asked, so
            # Telegram may share one cached answer between everybody.
            is_personal=False,
        )

    return answer_inline_query
