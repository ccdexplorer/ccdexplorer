# ruff: noqa: F403, F405, E402, E501, F401
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
# pyright: reportOptionalOperand=false
# pyright: reportOptionalIterable=false

from ccdexplorer.tooter import TooterChannel, TooterType
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from pymongo import ReplaceOne
from .messages_logic import Mixin as messages_logic

from ccdexplorer.site_user import (
    SiteUser,
    NotificationPreferences,
    AccountForUser,
    AccountNotificationPreferences,
    ValidatorNotificationPreferences,
    OtherNotificationPreferences,
)
import uuid
from ccdexplorer.env import *  # type: ignore
from ccdexplorer.mongodb import (
    CollectionsUtilities,
)
from telegram import Update
from telegram.ext import ContextTypes
from rich.console import Console
from rich import print
from ..notification_classes import *

console = Console()


class Mixin:
    async def user_login(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Either send login for existing user, or create an account and send login for new user."""
        telegram_user = update.effective_user
        user: SiteUser | None = self.users.get(str(telegram_user.id))
        if user:
            existing_user = user
            console.log("Existing user.")

            mess = f"Hey <b>{existing_user.first_name}</b>! Use this <a href='https://ccdexplorer.io/token/{existing_user.token}'>link</a> to login on CCDExplorer."
            # console.log(f"{message.from_user.id} | {mess}")
            await update.message.reply_html(mess)

        else:
            console.log("New user.")
            new_user = SiteUser(
                token=str(uuid.uuid4()),
                telegram_chat_id=telegram_user.id,
                first_name=telegram_user.first_name,
                username=telegram_user.username,
                language_code=telegram_user.language_code,
                last_modified=dt.datetime.now().astimezone(tz=dt.timezone.utc),
            )
            try:
                self.connections.tooter.relay(
                    channel=TooterChannel.NOTIFIER,
                    title="",
                    chat_id=ADMIN_CHAT_ID,
                    body=f"New user account created for '{telegram_user.username}'.",
                    notifier_type=TooterType.INFO,
                )
            except Exception as e:
                console.log(f"Exception sending notification for newly created user: {e}")

            try:
                self.connections: Connections
                self.connections.mongodb.utilities[CollectionsUtilities.users_v2_prod].bulk_write(
                    [
                        ReplaceOne(
                            {"_id": str(new_user.telegram_chat_id)},
                            new_user.model_dump(exclude_none=True),
                            upsert=True,
                        )
                    ]
                )
            except Exception as e:
                print(e)
            mess = f"Hey <b>{new_user.first_name}</b>! Welcome to CCDExplorer. I've created an account for you. Use this <a href='https://ccdexplorer.io/token/{new_user.token}'>link</a> to login on CCDExplorer with your new account."
            try:
                await update.message.reply_html(mess)
            except Exception as e:
                console.log(f"Exception sending message to newly created user: {e}")

    async def user_win_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send earliest win time for all validators"""
        telegram_user = update.effective_user
        user: SiteUser | None = self.users.get(str(telegram_user.id))
        message = """
        <b>Earliest Win Time</b><br/><br/>
        
        """
        if user:
            for account_index, account in user.accounts.items():
                account: AccountForUser
                account_info = self.connections.grpcclient.get_account_info(
                    "last_final", account_index=int(account_index)
                )
                if account_info.stake.baker:
                    earliest_win_time = self.connections.grpcclient.get_baker_earliest_win_time(
                        int(account_index)
                    )
                    if earliest_win_time:
                        ewt = messages_logic.verbose_timedelta(
                            self,
                            (earliest_win_time - dt.datetime.now().astimezone(dt.timezone.utc)),
                        )
                        message += (
                            f"<code>{int(account_index):6,.0f}</code> --> <code>{ewt}</code><br/>"
                        )
            # message += "</table>"
            self.connections.tooter.relay(
                channel=TooterChannel.BOT,
                title="",
                chat_id=user.telegram_chat_id,
                body=message,
                notifier_type=TooterType.INFO,
            )
            # await update.message.reply_html(message)

    async def user_me(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Either send login for existing user, or create an account and send login for new user."""
        telegram_user = update.effective_user
        user: SiteUser | None = self.users.get(str(telegram_user.id))
        if user:
            for account_index, account in user.accounts.items():
                account: AccountForUser
                message = f"""
                    {account.label} ({account.account_index})\n
                    """
                for field in AccountNotificationPreferences.model_fields:
                    if account.account_notification_preferences:
                        if account.account_notification_preferences.__getattribute__(field):
                            pref: NotificationPreferences = (
                                account.account_notification_preferences.__getattribute__(field)
                            )
                            if pref.telegram:
                                t = "✅" if pref.telegram.enabled else "❌"
                                tl = (
                                    f" ({(pref.telegram.limit / 1_000_000):,.0f} CCD)"
                                    if pref.telegram.limit
                                    else ""
                                )
                            else:
                                t = "❌"
                                tl = ""

                            if pref.email:
                                e = "✅" if pref.email.enabled else "❌"
                                el = (
                                    f" ({(pref.email.limit / 1_000_000):,.0f} CCD)"
                                    if pref.email.limit
                                    else ""
                                )
                            else:
                                e = "❌"
                                el = ""
                            if (t != "❌") or (e != "❌"):
                                message += f"\n{field}\n - T: {t}{tl} - E: {e}{el}\n"

                for field in ValidatorNotificationPreferences.model_fields:
                    if account.validator_notification_preferences:
                        if account.validator_notification_preferences.__getattribute__(field):
                            pref: NotificationPreferences = (
                                account.validator_notification_preferences.__getattribute__(field)
                            )
                            if pref.telegram:
                                t = "✅" if pref.telegram.enabled else "❌"
                            else:
                                t = "❌"
                            if pref.email:
                                e = "✅" if pref.email.enabled else "❌"
                            else:
                                e = "❌"

                            if (t != "❌") or (e != "❌"):
                                message += f"\n{field}\n - T: {t} - E: {e}\n"

                await update.message.reply_text(message)

            message = ""
            other_notification_preferences = user.other_notification_preferences
            for field in OtherNotificationPreferences.model_fields:
                if other_notification_preferences:
                    if other_notification_preferences.__getattribute__(field):
                        pref: NotificationPreferences = (
                            other_notification_preferences.__getattribute__(field)
                        )
                        if pref.telegram:
                            t = "✅" if pref.telegram.enabled else "❌"
                            tl = (
                                f" ({(pref.telegram.limit / 1_000_000):,.0f} CCD)"
                                if pref.telegram.limit
                                else ""
                            )
                        else:
                            t = "❌"
                            tl = ""

                        if pref.email:
                            e = "✅" if pref.email.enabled else "❌"
                            el = (
                                f" ({(pref.email.limit / 1_000_000):,.0f} CCD)"
                                if pref.email.limit
                                else ""
                            )
                        else:
                            e = "❌"
                            el = ""

                        if (t != "❌") or (e != "❌"):
                            message += f"\n{field}\n - T: {t}{tl} - E: {e}{el}\n"

        await update.message.reply_text(message)
