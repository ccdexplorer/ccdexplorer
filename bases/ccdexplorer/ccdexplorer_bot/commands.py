"""The commands the bot answers, and where it answers them.

Every one of them replies about an account -- a way to log in to it, what it
is subscribed to, when its validators next bake -- so every one is limited to
a private chat. Without the filter a command typed in a group (/login@bot) is
answered in that group, to everyone in it.

Kept out of __main__ so it can be imported: that module connects to Mongo and
a node as it loads.
"""

from telegram.ext import CommandHandler, filters


def command_handlers(bot) -> list[CommandHandler]:
    private = filters.ChatType.PRIVATE
    return [
        CommandHandler("login", bot.user_login, filters=private),
        CommandHandler("start", bot.user_login, filters=private),
        CommandHandler("wintime", bot.user_win_time, filters=private),
        CommandHandler("me", bot.user_me, filters=private),
    ]
