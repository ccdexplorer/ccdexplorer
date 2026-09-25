"""ccdexplorer-chart-bot: Concordium charts, inline, in any chat.

Separate from ccdexplorer_bot on purpose. That one pushes notifications to
people who asked for them and holds Mongo, gRPC and a notifier token to do it.
This one answers inline queries inside other people's conversations and needs
none of that -- no database, no node, no chain data, and no credential beyond
its own token. Keeping them apart keeps the blast radius of an inline bot,
which by design runs where strangers can reach it, down to nothing.

Enable inline mode for the bot in BotFather (/setinline) or Telegram never
sends it a query, and nothing here will ever run.
"""

import logging

from ccdexplorer.env import CHART_BOT_TOKEN, SITE_URL
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    MessageHandler,
    filters,
)

from .catalogue import CHARTS
from .direct import callback_handler
from .direct import handler as direct_handler
from .inline import handler as inline_handler

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("chart_bot")

DEFAULT_SITE_URL = "https://ccdexplorer.io"


async def start(update: Update, context) -> None:
    """The list of charts, and the words that select them.

    Reached two ways: opening the bot directly, and the "What can I ask for?"
    button Telegram draws above the inline results. The second is the one that
    matters -- it is in front of someone who is mid-query and stuck, which is
    exactly when a list of words is worth having.
    """
    username = context.bot.username or "ccdexplorer_chart_bot"
    lines = [
        "I put Concordium charts into any chat.",
        "",
        f"Type <code>@{username}</code> in any conversation, then any of these:",
        "",
    ]
    for chart in CHARTS:
        terms = ", ".join(chart.keywords[:3])
        lines.append(f"<b>{chart.title}</b> — <i>{terms}</i>")

    lines += [
        "",
        f"Or type <code>@{username}</code> and nothing else to see all {len(CHARTS)} charts.",
        "",
        "Whole questions work too — <i>how much is staked</i>, <i>what are the fees</i>.",
    ]
    await update.message.reply_html("\n".join(lines), disable_web_page_preview=True)


def main() -> None:
    if not CHART_BOT_TOKEN:
        raise SystemExit("CHART_BOT_TOKEN is not set; the bot has nothing to connect with.")

    site_url = SITE_URL or DEFAULT_SITE_URL
    log.info("serving %s charts from %s", len(CHARTS), site_url)

    application = ApplicationBuilder().token(CHART_BOT_TOKEN).build()
    application.add_handler(CommandHandler(["start", "help"], start))
    application.add_handler(InlineQueryHandler(inline_handler(site_url)))
    application.add_handler(CallbackQueryHandler(callback_handler(site_url)))
    # Anything else typed at the bot directly is treated as a chart query.
    # Registered last, so it cannot swallow the commands above.
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, direct_handler(site_url))
    )
    application.run_polling(allowed_updates=["message", "inline_query", "callback_query"])


if __name__ == "__main__":
    main()
