from enum import Enum

import re
import time

import aiohttp
import requests

from ccdexplorer.env import (
    ADMIN_CHAT_ID,
    API_TOKEN,
    BARK_URL,
    BRANCH,
    ENVIRONMENT,
    FASTMAIL_TOKEN,
    MAILTO_LINK,
    MAILTO_USER,
    NOTIFIER_API_TOKEN,
)



# How long one condition stays quiet on Bark after it has rung. Watchtower
# re-checks every 60 seconds, so without this a fault that lasts an hour is
# sixty critical alerts, each ringing for thirty seconds -- which is how a
# pager gets muted and then ignored. Telegram is unaffected and keeps the
# full minute-by-minute record.
CRITICAL_COOLDOWN_SECONDS = 30 * 60


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
    # Reserved for Tooter.critical(). The existing members are not usable as a
    # severity -- watchtower funnels every alert it has through one inform()
    # as REQUESTS_ERROR, and INFO covers fifteen notifier sites including real
    # failures -- so "this is down" needed a name of its own.
    CRITICAL = "critical"


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
        self.bark_url = BARK_URL
        # Per-condition cooldown clock, keyed by the caller's `key`. Process
        # local and in memory on purpose; see bark()'s docstring.
        self._bark_last_sent: dict[str, float] = {}
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

    def critical(
        self,
        message: str,
        key: str | None = None,
        chat_id: str | int | None = None,
    ) -> None:
        """An alert that means something is down, not something is interesting.

        Everything else here goes to Telegram and waits to be read. A Telegram
        notification cannot wake anyone: it obeys silent mode and Do Not
        Disturb, which is correct for the hundred informational alerts a day
        and useless for the replica set losing its primary at 3am.

        So this sends twice. Telegram keeps the record -- same channel, same
        chat, same text as before, so nothing is lost from the existing log --
        and Bark pushes an iOS critical alert, which ignores silent and DND by
        design and, with `call`, keeps ringing for thirty seconds.

        Reserve it for the things live-everything paints red: no primary, a
        member down, Sentinel without quorum, a stalled change stream, a
        container being OOM-killed. An alert that pages for something that
        does not need a human teaches you to ignore the ones that do.

        `key` identifies the condition, not the wording, and is what the
        cooldown counts against. Pass a stable one wherever the text carries a
        changing number -- "Member X is 12 mins behind" becomes 13 next minute
        and would otherwise ring forever. It defaults to the message, which is
        right only for alerts whose text is fixed.
        """
        # Telegram first and outside the try, because it is the alert of
        # record. Bark is the escalation on top, and a misconfigured device
        # key, an expired one, or Bark's server being down must never be able
        # to swallow the notification that something is broken.
        #
        # Telegram is deliberately not throttled: it behaves exactly as the
        # inform() it replaces, so the running log of an ongoing fault stays
        # complete. Only the thing that makes a noise is rate limited.
        self.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=chat_id or ADMIN_CHAT_ID,
            body=message,
            notifier_type=TooterType.CRITICAL,
        )
        self.bark(message, key=key or message)

    def bark(self, message: str, key: str | None = None) -> None:
        """Push one iOS critical alert, if a device is configured and due.

        Unset BARK_URL is the ordinary case rather than an error: most of the
        deployables that embed this component have no reason to page anybody,
        and they all share one env component. Silence beats an exception, and
        beats a log line repeated by twenty services.

        The cooldown is the difference between this being usable and being
        switched off within a day. Watchtower checks every 60 seconds, and a
        replica set can sit without a primary for an hour -- that is sixty
        pages, each ringing for thirty seconds. Once per condition per
        CRITICAL_COOLDOWN_SECONDS still nags while something is broken without
        making the phone unusable.

        It is held in memory, so a restart re-pages. That is the right way
        round: a watchtower that just came up should re-assert what it finds
        rather than stay quiet about it.
        """
        if not self.bark_url:
            return
        now = time.monotonic()
        last = self._bark_last_sent.get(key or message)
        if last is not None and (now - last) < CRITICAL_COOLDOWN_SECONDS:
            return
        self._bark_last_sent[key or message] = now
        payload = {
            # level=critical is the whole point -- it is what ignores silent
            # and Do Not Disturb. call=1 repeats the ringtone for 30s so a
            # single buzz at 3am is not the only chance to notice. volume is
            # on Bark's own 0-10 scale and applies to critical alerts only.
            "urls": f"{self.bark_url}{'&' if '?' in self.bark_url else '?'}level=critical&call=1&volume=8",
            "title": f"CCDExplorer {self.environment}",
            "body": message,
            "format": "text",
        }
        try:
            _ = requests.post(self.url, json=payload, timeout=10)
        except Exception as e:
            # Nothing useful to do: the Telegram alert has already gone, and
            # raising here would propagate into whatever was reporting the
            # original fault and bury it.
            print(f"Bark critical alert failed: {e}")

    def send_to_tooter(self, msg: str):
        self.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body=msg,
            notifier_type=TooterType.INFO,
        )
