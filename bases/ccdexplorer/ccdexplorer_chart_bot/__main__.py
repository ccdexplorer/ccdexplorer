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
from telegram.ext import ApplicationBuilder, CommandHandler, InlineQueryHandler

from .catalogue import CHARTS
from .inline import handler

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("chart_bot")

DEFAULT_SITE_URL = "https://ccdexplorer.io"


async def start(update: Update, context) -> None:
    """What someone sees when they open the bot directly rather than inline."""
    lines = [
        "I put Concordium charts into any chat.",
        "",
        "Type <code>@%s</code> followed by what you want, in any conversation:"
        % (context.bot.username or "ccdexplorer_chart_bot"),
        "",
        "  <code>validators</code>   <code>delegators</code>   <code>fees</code>   "
        "<code>accounts</code>   <code>exchanges</code>",
        "",
        f"There are {len(CHARTS)} charts. Type nothing to see them all.",
    ]
    await update.message.reply_html("\n".join(lines))


def main() -> None:
    if not CHART_BOT_TOKEN:
        raise SystemExit("CHART_BOT_TOKEN is not set; the bot has nothing to connect with.")

    site_url = SITE_URL or DEFAULT_SITE_URL
    log.info("serving %s charts from %s", len(CHARTS), site_url)

    application = ApplicationBuilder().token(CHART_BOT_TOKEN).build()
    application.add_handler(CommandHandler(["start", "help"], start))
    application.add_handler(InlineQueryHandler(handler(site_url)))
    application.run_polling(allowed_updates=["message", "inline_query"])


if __name__ == "__main__":
    main()
