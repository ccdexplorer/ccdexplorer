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
from .direct import (
    callback_handler,
    command_handler,
    nudge_handler,
    retired_command_handler,
)
from .inline import handler as inline_handler
from .inline import pickable

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
    # The picker collapses a family to one entry, so this counts what a reader
    # will actually see rather than how many Chart objects exist.
    offered = pickable(CHARTS)
    lines = [
        "I put Concordium charts into any chat.",
        "",
        "Here: <code>/e &lt;word&gt;</code> — for example <code>/e price</code>. "
        "<code>/e</code> on its own lists the categories.",
        "",
        f"Anywhere else: type <code>@{username}</code>, then any of these:",
        "",
    ]
    for chart in offered:
        terms = ", ".join(chart.keywords[:3])
        lines.append(f"<b>{chart.title}</b> — <i>{terms}</i>")

    lines += [
        "",
        f"Or type <code>@{username}</code> and nothing else to see all {len(offered)} charts.",
        "",
        "Charts with intervals arrive with buttons for their other periods.",
        "",
        "In a group where another bot also answers <code>/e</code>, use "
        "<code>/e@" + username + "</code>.",
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
    # /e for explorer. Short enough that another bot in the same group may
    # claim it -- PTB ignores /e@other_bot, comparing the part after @ to its
    # own username, so the only ambiguous case is a bare /e in a group, where
    # both bots answer. The help says to qualify it there.
    application.add_handler(CommandHandler(["e"], command_handler(site_url)))
    # The names it used to answer to. Registered rather than dropped: an
    # unknown command does not reach the plain-text handler either, so
    # removing these would answer anyone who learned /c with silence.
    application.add_handler(CommandHandler(["c", "ccd"], retired_command_handler()))
    # Plain text no longer searches -- it points at /e. Registered last, so
    # it cannot swallow the commands above. Answering everything typed at the
    # bot made it noisy in group chats; going silent instead would read as the
    # bot being broken, so it says one line and nothing more.
    application.add_handler(
        MessageHandler(
            # Private only. Answering every plain message is the group-chat
            # noise this replaced; if the bot's privacy mode is off it sees
            # every message in every group, and the nudge would reply to all
            # of them -- one for one with what was removed.
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            nudge_handler(),
        )
    )
    application.run_polling(
        allowed_updates=[
            "message",
            "inline_query",
            "callback_query",
            "chosen_inline_result",
        ]
    )


if __name__ == "__main__":
    main()
