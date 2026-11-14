from enum import Enum

import datetime as dt
import re
import time
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
import requests

from ccdexplorer.env import (
    ADMIN_CHAT_ID,
    API_TOKEN,
    BARK_QUIET_END,
    BARK_QUIET_START,
    BARK_QUIET_TZ,
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


def _now() -> dt.datetime:
    """Wall clock, aware. Deliberately separate from `time.monotonic()`.

    The cooldown measures elapsed time and must not move when the clock is
    set; the quiet window asks what time it is where the phone is. Two
    different questions, so two different clocks.
    """
    return dt.datetime.now(dt.UTC)


class QuietHours:
    """A wall-clock window in which Bark stays silent.

    A critical alert is built to defeat Do Not Disturb -- that is the entire
    reason it exists -- so the phone cannot silence it and this is the only
    place a night window can be honoured. Telegram is left alone: it obeys
    silent mode already, so it cannot wake anyone, and it is the record of
    what the estate did overnight.

    The zone is explicit rather than read from the host. Almost none of these
    containers set TZ, so they run on UTC -- an hour behind Amsterdam in
    winter and two in summer -- and a naive `datetime.now()` would hold
    00:00-07:00 local in January and 01:00-08:00 in July. ZoneInfo also keeps
    the window from drifting when the clocks change.
    """

    def __init__(self, start: dt.time, end: dt.time, tz: ZoneInfo) -> None:
        self.start = start
        self.end = end
        self.tz = tz

    @classmethod
    def parse(cls, start: str, end: str, tz: str) -> "QuietHours | None":
        """A window from three env strings, or None if there is not one.

        Fails open, and says so. A typo in BARK_QUIET_START must not turn
        paging off: silence is indistinguishable from a healthy estate, so
        that failure would stay invisible for exactly as long as it mattered.
        """
        if not (start or "").strip() or not (end or "").strip():
            return None
        try:
            return cls(_clock(start), _clock(end), ZoneInfo(tz.strip()))
        except (ValueError, ZoneInfoNotFoundError, KeyError) as exc:
            print(f"Bark: ignoring quiet hours {start!r}-{end!r} {tz!r}: {exc}")
            return None

    def covers(self, moment: dt.datetime) -> bool:
        """Whether `moment` -- in any zone -- falls inside the window.

        Half-open, so "23:00-06:00" reads as it sounds: asleep at 23:00,
        awake at 06:00. A start equal to its end is an empty window rather
        than an all-day mute; guessing the other way would silence paging
        completely and invisibly.
        """
        local = moment.astimezone(self.tz).time()
        if self.start <= self.end:
            return self.start <= local < self.end
        return local >= self.start or local < self.end

    def __str__(self) -> str:
        return f"{self.start:%H:%M}-{self.end:%H:%M} {self.tz.key}"


def _clock(text: str) -> dt.time:
    """ "23:00", or the "23" anyone types first."""
    parts = text.strip().split(":")
    if not 1 <= len(parts) <= 2:
        raise ValueError(f"not a time of day: {text!r}")
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) == 2 else 0
    return dt.time(hour, minute)


class RelayStatus(int):
    """The status apprise answered with, and what it said when it refused.

    Still the number every caller compares against. The reason rides along
    because the status cannot tell a recipient who blocked the bot from a
    send that failed for any other cause -- both are `424` -- and only the
    caller is in a position to act on the difference.
    """

    detail: str

    def __new__(cls, status: int, detail: str = "") -> "RelayStatus":
        self = super().__new__(cls, status)
        self.detail = detail
        return self


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


#: Apprise renders its own captured notification log into the response body,
#: at whatever level was asked for -- and that log is the only place the
#: provider's reason for refusing appears. Pinned here rather than left to the
#: server's `APPRISE_LOG_LEVEL`, which is unset on ours and so defaults to
#: WARNING; a quieter default set later would silently cost every diagnosis.
APPRISE_HEADERS = {"X-Apprise-Log-Level": "WARNING"}

#: How much of apprise's response body goes into the log. Its errors are one
#: line; anything longer is an HTML page from something in front of it, and a
#: container log is not the place for that.
APPRISE_DETAIL_LIMIT = 400


async def _apprise_detail(response, token: str) -> str:
    """Why apprise refused this notification, fit to be logged.

    The status alone cannot tell a transient refusal from a blocked recipient:
    both are `424`. On 2026-11-09 one of two payday notifications to a working
    chat was refused 240 ms after the other was accepted, and the reason was
    unrecoverable -- the body was discarded here and apprise-api keeps no
    application log of its own.

    Never raises. Reading the body is a second network read, and the caller
    acts on the status, so a body we could not read must not cost it.

    The token is scrubbed because apprise echoes the `tgram://` url it was
    handed, which carries it. Logging the body verbatim would publish the bot
    token to every container log and to whatever ships them.
    """
    try:
        body = (await response.text()).strip()
    except Exception as exc:  # noqa: BLE001 - logged, never raised
        return f"(body unreadable: {exc!r})"
    if not body:
        return "(no body)"
    if token:
        body = body.replace(token, "<token>")
    if len(body) > APPRISE_DETAIL_LIMIT:
        body = body[:APPRISE_DETAIL_LIMIT] + "..."
    return body


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


#: What an address has to look like before it is put in a mail url.
#:
#: Narrower than the RFC on purpose. Mail leaves through apprise, which takes
#: the server, the login and the recipient in one url and reads the recipient
#: a second time once the url is decoded -- splitting it on commas and spaces,
#: and taking a quote as the end of a display name. So `o'brien@example.com`
#: is delivered to brien@example.com, which is somebody else's mailbox. An
#: address this pattern turns away gets an error its owner can read; one that
#: apprise misreads gets silence, or a stranger's inbox.
#:
#: Letters, digits and `. _ -` before the @, with at most one `+tag`; a
#: hostname with a dot after it. One tag because apprise takes one: a second
#: `+` makes it drop the recipient without a word, and an address accepted
#: here and discarded there is a mail that never arrives.
_EMAIL_NAME = r"[A-Za-z0-9_][A-Za-z0-9_-]*(?:\.[A-Za-z0-9_-]+)*"
_EMAIL_ADDRESS = re.compile(
    rf"{_EMAIL_NAME}(?:\+{_EMAIL_NAME})?"
    r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])"
)
#: The longest address a mail server will take (RFC 5321).
EMAIL_ADDRESS_MAX_LENGTH = 254


def is_acceptable_email_address(value: object) -> bool:
    """Whether `value` is an address mail can safely be sent to.

    fullmatch, not match with a `$`: that anchor is satisfied just before a
    trailing newline, and a newline is one of the characters this is here to
    keep out. Anything that is not a string is not an address either -- the
    stored value comes from Mongo and is whatever was once written there.
    """
    return (
        isinstance(value, str)
        and len(value) <= EMAIL_ADDRESS_MAX_LENGTH
        and _EMAIL_ADDRESS.fullmatch(value) is not None
    )


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
        #: Hours in which Bark stays silent; see QuietHours. None means page
        #: at any hour, which is how this behaved before the window existed.
        self.bark_quiet = QuietHours.parse(BARK_QUIET_START, BARK_QUIET_END, BARK_QUIET_TZ)
        # Per-condition cooldown clock, keyed by the caller's `key`. Process
        # local and in memory on purpose; see bark()'s docstring.
        self._bark_last_sent: dict[str, float] = {}
        self.email_part_1 = MAILTO_LINK
        self.email_part_2 = f"""&user={MAILTO_USER}&pass={FASTMAIL_TOKEN}&from=CCDExplorer Bot<bot@ccdexplorer.io>"""
        # self.email_part_2_api = f"""&user={MAILTO_USER}&pass={FASTMAIL_TOKEN}&from=CCDExplorer API Bot<bot@ccdexplorer.io>"""

    def _mail_url(self, email_address: object) -> str | None:
        """The apprise url for one recipient, or None if there is no safe one.

        The address is percent-encoded so that it stays a value. It used to be
        pasted in as it came, and the url carries the mail server and its
        login: an address ending in `&smtp=<host>` sent that login to a server
        of the sender's choosing, and one ending in `,<scheme>://<host>` added
        a second destination of any kind apprise knows.

        Encoding alone is not enough, because apprise decodes the value and
        then splits it again; see _EMAIL_ADDRESS. Both, so that neither has to
        be right about everything.
        """
        if not is_acceptable_email_address(email_address):
            return None
        return f"{self.email_part_1}{quote(str(email_address), safe='')}{self.email_part_2}"

    def _send_mail(self, title: str, body: str, email_address: object) -> bool:
        """Post one mail. False means it was refused and nothing was sent.

        Refused rather than raised: this runs for addresses stored long ago as
        well as ones typed a moment ago, and a notification that cannot be
        delivered should cost that one notification, not the task around it.
        The address is deliberately not logged.
        """
        urls = self._mail_url(email_address)
        if urls is None:
            return False
        payload = {"urls": urls, "title": title, "body": body, "format": "html"}
        _ = requests.post(self.url, json=payload)
        return True

    def email(self, title: str, body: str, email_address: str, value=None, error=None) -> bool:
        """Send email"""
        body_signature = """
Please visit your <a href='https://ccdexplorer.io/settings/user/overview'>account</a> to adjust notification settings.

        """
        return self._send_mail(title, body + body_signature, email_address)

    def email_api(self, title: str, body: str, email_address: str, value=None, error=None) -> bool:
        body_signature = """
        """
        return self._send_mail(title, body + body_signature, email_address)

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
            async with session.post("/notify", json=payload, headers=APPRISE_HEADERS) as response:
                if response.status != 200:
                    detail = await _apprise_detail(response, API_TO_USE)
                    print(
                        f"Error sending notification to {chat_id}. "
                        f"Status code= {response.status}. {detail}"
                    )
                    return RelayStatus(response.status, detail)
                return RelayStatus(response.status)

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

        Between BARK_QUIET_START and BARK_QUIET_END nothing is sent at all --
        see QuietHours. This is the only place that window can be honoured,
        because a critical alert exists specifically to override the phone's
        own silence. Telegram still goes out, so nothing is lost from the log.
        """
        if not self.bark_url:
            return
        # Suppressed, not swallowed: the cooldown clock is left untouched, so
        # a fault that lasted the night rings at the end of the window rather
        # than starting the morning inside a half-hour cooldown for an alert
        # that was never delivered. Telegram has gone out either way.
        if self.bark_quiet is not None and self.bark_quiet.covers(_now()):
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
