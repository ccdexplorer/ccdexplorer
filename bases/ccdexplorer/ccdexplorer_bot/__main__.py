# ruff: noqa: F403, F405, E402, E501

from ccdexplorer.tooter import Tooter
from ccdexplorer.grpc_client import GRPCClient

from ccdexplorer.mongodb import MongoDB, MongoMotor
from .bot import Bot, Connections
from ccdexplorer.env import *  # type: ignore

from telegram.ext import (
    CommandHandler,
    ApplicationBuilder,
)
from ccdexplorer.env import API_TOKEN

# bump
grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
mongomotor = MongoMotor(tooter)

import logging

logging.getLogger("apscheduler").propagate = False

import sentry_sdk

sentry_sdk.init(
    dsn="https://9e4d7daa546f8e53fae6a8ce4ba6cac0@o4503924901347328.ingest.us.sentry.io/4510815580389376",
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
)

if __name__ == "__main__":
    bot = Bot(
        Connections(tooter=tooter, mongodb=mongodb, mongomoter=mongomotor, grpcclient=grpcclient)
    )
    bot.do_initial_reads_from_collections()
    bot.do_missed_rounds_read()

    application = ApplicationBuilder().token(API_TOKEN).build()
    application.add_handler(CommandHandler("login", bot.user_login))
    application.add_handler(CommandHandler("start", bot.user_login))
    application.add_handler(CommandHandler("wintime", bot.user_win_time))
    application.add_handler(CommandHandler("me", bot.user_me))
    application.bot_data = {"net": "mainnet"}

    job_queue = application.job_queue
    assert job_queue
    job_minute = job_queue.run_repeating(bot.get_new_blocks_from_mongo, interval=2)
    job_minute = job_queue.run_repeating(bot.process_new_blocks, interval=2, first=1)
    job_minute = job_queue.run_repeating(bot.send_notification_queue, interval=2, first=2)
    job_minute = job_queue.run_repeating(
        bot.async_read_users_from_collection, interval=10, first=10
    )
    job_minute = job_queue.run_repeating(
        bot.async_read_contracts_with_tag_info, interval=10, first=10
    )
    job_minute = job_queue.run_repeating(bot.async_read_labeled_accounts, interval=10, first=10)
    job_minute = job_queue.run_repeating(
        bot.async_read_nightly_accounts, interval=60 * 60, first=60 * 60
    )
    job_minute = job_queue.run_repeating(
        bot.async_read_payday_last_blocks_validated, interval=10, first=1
    )
    job_minute = job_queue.run_repeating(
        bot.get_new_dashboard_nodes_from_mongo, interval=5 * 60, first=60
    )
    application.run_polling()
