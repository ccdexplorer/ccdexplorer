from enum import Enum

import re

import aiohttp
import requests

from ccdexplorer.env import (
    ADMIN_CHAT_ID,
    API_TOKEN,
    BRANCH,
    ENVIRONMENT,
    FASTMAIL_TOKEN,
    MAILTO_LINK,
    MAILTO_USER,
    NOTIFIER_API_TOKEN,
)



def for_telegram(body: str) -> str:
    """One line break per intended break, for Telegram only.

    The message definitions are HTML and break lines with `<br/>`, but they are
    also readable multi-line f-strings, so many bodies carry real newlines too.
    Apprise used to collapse the whitespace around a `<br>`; since 2.1.0 it does
    not, so `...(Tx fees)<br/>` followed by `"\nDistribution..."` became two
    newlines where it had been one -- blank lines right through every
    notification (2026-10-29).

    Rather than rewrite ~270 break sites across six files, every break is
    normalised here, where all three Telegram paths meet: tags become newlines,
    and a run of them collapses to at most one blank line. Email keeps its tags,
    because real HTML mail needs them.
    """
    # A tag that ends a line swallows the source newline after it: the author
    # wrote one break, the newline is just how the f-string was laid out.
    # Consecutive tags keep their count, so `<br/><br/>` is still a blank line.
    body = re.sub(r"[ \t]*<br\s*/?>[ \t]*\n?", "\n", body, flags=re.IGNORECASE)
    body = re.sub(r"[ \t]+\n", "\n", body)
    # Never more than one blank line, whatever the source did.
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


class TooterType(Enum):
    INFO = "info"
    GRAPHQL_ERROR = "graphql error"
    MONGODB_ERROR = "mongodb error"
    REQUESTS_ERROR = "requests error"
    BOT_MAIN_LOOP_ERROR = "bot_main_loop_error"


class TooterChannel(Enum):
    BOT = "bot"
    NOTIFIER = "notifier"
    SMART = "smart"


class Tooter:
    def __init__(self) -> None:
        self.environment = ENVIRONMENT
        self.branch = BRANCH
        self.NOTIFIER_API_TOKEN = NOTIFIER_API_TOKEN
        self.BOT_API_TOKEN = API_TOKEN
        self.FASTMAIL_TOKEN = FASTMAIL_TOKEN
        self.url = "https://tooter.ccdexplorer.io/notify/"
        self.plain_url = "https://tooter.ccdexplorer.io"
        self.email_part_1 = MAILTO_LINK
        self.email_part_2 = f"""&user={MAILTO_USER}&pass={FASTMAIL_TOKEN}&from=CCDExplorer Bot<bot@ccdexplorer.io>"""
        # self.email_part_2_api = f"""&user={MAILTO_USER}&pass={FASTMAIL_TOKEN}&from=CCDExplorer API Bot<bot@ccdexplorer.io>"""

    def email(self, title: str, body: str, email_address: str, value=None, error=None):
        """Send email"""
        body_signature = """
Please visit your <a href='https://ccdexplorer.io/settings/user/overview'>account</a> to adjust notification settings.

        """
        payload = {
            "urls": f"{self.email_part_1}{email_address}{self.email_part_2}",
            "title": title,
            "body": body + body_signature,
            "format": "html",
        }
        _ = requests.post(self.url, json=payload)

    def email_api(self, title: str, body: str, email_address: str, value=None, error=None):
        body_signature = """
        """
        payload = {
            "urls": f"{self.email_part_1}{email_address}{self.email_part_2}",
            "title": title,
            "body": body + body_signature,
            "format": "html",
        }
        _ = requests.post(self.url, json=payload)

    def relay(
        self,
        channel: TooterChannel,
        body: str,
        notifier_type: TooterType,
        title: str,
        chat_id: str | int | None = None,
        value=None,
        error=None,
        bcc=False,
    ):
        API_TO_USE = self.BOT_API_TOKEN if channel == TooterChannel.BOT else self.NOTIFIER_API_TOKEN
        chat_id = ADMIN_CHAT_ID if channel == TooterChannel.NOTIFIER else chat_id

        payload = {
            "urls": f"tgram://{API_TO_USE}/{chat_id}",
            "title": for_telegram(f"{title}<br/>"),
            "body": for_telegram(body),
            "format": "html",
        }
        _ = requests.post(self.url, json=payload)

    async def async_relay(
        self,
        channel: TooterChannel,
        body: str,
        notifier_type: TooterType,
        title: str,
        chat_id: str | int | None = None,
    ) -> int:
        """Post one notification, and report what apprise said about it.

        The status is returned rather than only printed because `424` is not
        noise -- it is Telegram saying this recipient has blocked the bot, and
        the caller is the only one in a position to remember that. Printing it
        and returning None is what let four blocked chats be sent to for weeks.
        """
        API_TO_USE = self.BOT_API_TOKEN if channel == TooterChannel.BOT else self.NOTIFIER_API_TOKEN
        chat_id = ADMIN_CHAT_ID if channel == TooterChannel.NOTIFIER else chat_id
        payload = {
            "urls": f"tgram://{API_TO_USE}/{chat_id}",
            "title": for_telegram(f"{title}<br/>"),
            "body": for_telegram(body),
            "format": "html",
        }
        async with aiohttp.ClientSession(self.plain_url) as session:
            async with session.post("/notify", json=payload) as response:
                if response.status != 200:
                    print(
                        f"Error sending notification to {chat_id}. Status code= {response.status}."
                    )
                return response.status

    def send(
        self,
        channel: TooterChannel,
        message: str,
        notifier_type: TooterType,
        chat_id: str | int | None = None,
        value=None,
        error=None,
    ):
        API_TO_USE = self.BOT_API_TOKEN if channel == TooterChannel.BOT else self.NOTIFIER_API_TOKEN
        chat_id = ADMIN_CHAT_ID if channel == TooterChannel.NOTIFIER else chat_id

        title = f"t: {notifier_type.value} | e: {self.environment} | b: {self.branch}"
        body = message
        if value:
            body += "| value: {value} "
        if error:
            body += "| error: {error}."

        payload = {
            "urls": f"tgram://{API_TO_USE}/{chat_id}",
            "title": for_telegram(title),
            "body": for_telegram(body),
            "format": "html",
        }
        _ = requests.post(self.url, json=payload)

    def send_to_tooter(self, msg: str):
        self.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=msg,
            notifier_type=TooterType.INFO,
        )
