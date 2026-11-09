"""Paging, as opposed to notifying.

Every alert this estate sends went to Telegram, which obeys silent mode and
Do Not Disturb -- correct for the hundred informational messages a day and
useless for the replica set losing its primary at 3am. `Tooter.critical` adds
an iOS critical alert via Bark on top of the unchanged Telegram message.

Two properties matter more than the transport and are what most of this file
pins: a Bark problem must never swallow the Telegram alert, and one ongoing
fault must not ring sixty times an hour.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from ccdexplorer.tooter import core


@pytest.fixture
def tooter(monkeypatch):
    """A Tooter with a configured device and every POST captured."""
    posts = []
    monkeypatch.setattr(
        core.requests, "post", lambda url, json=None, timeout=None: posts.append(json)
    )
    t = core.Tooter()
    t.bark_url = "bark://bark.example/devicekey"
    t.posts = posts
    return t


def barked(t):
    return [p for p in t.posts if p["urls"].startswith("bark")]


def telegrammed(t):
    return [p for p in t.posts if p["urls"].startswith("tgram")]


# --- the transport --------------------------------------------------------


def test_the_alert_is_sent_as_an_ios_critical_alert(tooter):
    """level=critical is the entire reason for this feature: it is what
    ignores silent mode and Do Not Disturb."""
    tooter.bark("ReplicaSet has no PRIMARY.")

    (sent,) = barked(tooter)
    assert "level=critical" in sent["urls"]
    assert "call=1" in sent["urls"]  # keeps ringing for 30s
    assert sent["body"] == "ReplicaSet has no PRIMARY."


def test_query_parameters_are_appended_not_overwritten(tooter):
    """A device url may already carry its own options, e.g. an encryption
    key -- joining with `?` a second time would produce an invalid url."""
    tooter.bark_url = "bark://bark.example/devicekey?group=ccdexplorer"

    tooter.bark("x")

    (sent,) = barked(tooter)
    assert sent["urls"].count("?") == 1
    assert "group=ccdexplorer&level=critical" in sent["urls"]


def test_no_configured_device_is_silence_not_an_error(tooter):
    """The ordinary case for most of the ~20 deployables that embed this
    component: they share one env component and have no business paging."""
    tooter.bark_url = ""

    tooter.bark("x")

    assert tooter.posts == []


# --- the cooldown ---------------------------------------------------------


def test_one_condition_rings_once_per_cooldown(tooter):
    """Watchtower checks every 60s. Without this, an hour without a primary
    is sixty pages of thirty seconds each."""
    for _ in range(10):
        tooter.bark("ReplicaSet has no PRIMARY.", key="no-primary")

    assert len(barked(tooter)) == 1


def test_a_different_condition_is_not_suppressed(tooter):
    """Two faults at once must both get through; the cooldown is per
    condition, not global."""
    tooter.bark("no primary", key="no-primary")
    tooter.bark("redis down", key="redis")

    assert len(barked(tooter)) == 2


def test_the_key_identifies_the_condition_not_the_wording(tooter):
    """ "Member X is 12 mins behind" becomes 13 next minute. Keyed on the text
    it would ring forever, which is the trap this parameter exists for."""
    for minutes in range(10, 20):
        tooter.bark(f"Member rs2 is {minutes} mins behind primary.", key="member-lag:rs2")

    assert len(barked(tooter)) == 1


def test_it_rings_again_once_the_cooldown_lapses(tooter, monkeypatch):
    """Still nags about an unresolved fault -- it is a cooldown, not a mute."""
    clock = [1000.0]
    monkeypatch.setattr(core.time, "monotonic", lambda: clock[0])

    tooter.bark("no primary", key="no-primary")
    clock[0] += core.CRITICAL_COOLDOWN_SECONDS - 1
    tooter.bark("no primary", key="no-primary")
    assert len(barked(tooter)) == 1

    clock[0] += 2
    tooter.bark("no primary", key="no-primary")
    assert len(barked(tooter)) == 2


def test_the_cooldown_is_half_an_hour():
    assert core.CRITICAL_COOLDOWN_SECONDS == 30 * 60


# --- critical() ties the two together -------------------------------------


def test_critical_sends_both(tooter):
    tooter.critical("ReplicaSet has no PRIMARY.", key="no-primary")

    assert len(telegrammed(tooter)) == 1
    assert len(barked(tooter)) == 1


def test_telegram_is_not_throttled(tooter):
    """The Telegram message behaves exactly as the inform() it replaces, so
    the minute-by-minute record of an ongoing fault stays complete. Only the
    thing that makes a noise is rate limited."""
    for _ in range(5):
        tooter.critical("no primary", key="no-primary")

    assert len(telegrammed(tooter)) == 5
    assert len(barked(tooter)) == 1


def test_a_bark_failure_cannot_swallow_the_telegram_alert(monkeypatch):
    """The whole point of sending Telegram first and separately. An expired
    device key or a Bark outage must not lose the notification that something
    is broken."""
    posts = []

    def post(url, json=None, timeout=None):
        if json["urls"].startswith("bark"):
            raise ConnectionError("bark.example unreachable")
        posts.append(json)

    monkeypatch.setattr(core.requests, "post", post)
    t = core.Tooter()
    t.bark_url = "bark://bark.example/devicekey"

    t.critical("ReplicaSet has no PRIMARY.", key="no-primary")  # must not raise

    assert len(posts) == 1
    assert posts[0]["urls"].startswith("tgram")


def test_an_unconfigured_device_still_sends_telegram(tooter):
    """So the change is a no-op for every service that does not set BARK_URL."""
    tooter.bark_url = ""

    tooter.critical("no primary", key="no-primary")

    assert len(telegrammed(tooter)) == 1
    assert barked(tooter) == []


def test_the_telegram_message_carries_the_critical_type(tooter):
    """TooterType could not be used as a severity -- watchtower sent every
    alert it had as REQUESTS_ERROR -- so CRITICAL exists to make a paged
    alert identifiable in the Telegram log too."""
    assert core.TooterType.CRITICAL.value == "critical"


def test_the_key_defaults_to_the_message(tooter):
    """Right only for alerts whose text is fixed, which is why callers with a
    moving number pass one explicitly."""
    tooter.critical("ReplicaSet has no PRIMARY.")
    tooter.critical("ReplicaSet has no PRIMARY.")

    assert len(barked(tooter)) == 1


# --- quiet hours ----------------------------------------------------------
#
# A critical Bark alert is designed to defeat Do Not Disturb, so it is the one
# notification the phone cannot silence for itself. If there is to be a night
# window at all, it has to be honoured here.
#
# Telegram is deliberately untouched. It obeys silent mode already, so it
# cannot wake anybody, and it is the alert of record -- an estate that paged
# nobody overnight must still be able to show what it was doing.


def at(when: str, tz: str = "Europe/Amsterdam") -> dt.datetime:
    """A wall-clock moment in a named zone, e.g. at("23:30")."""
    hour, minute = (int(part) for part in when.split(":"))
    return dt.datetime(2026, 11, 8, hour, minute, tzinfo=ZoneInfo(tz))


@pytest.fixture
def night(tooter):
    """A tooter whose quiet window is the estate's default."""
    tooter.bark_quiet = core.QuietHours(dt.time(23, 0), dt.time(6, 0), ZoneInfo("Europe/Amsterdam"))
    return tooter


def test_nothing_barks_inside_the_window(night, monkeypatch):
    monkeypatch.setattr(core, "_now", lambda: at("23:30"))

    night.bark("ReplicaSet has no PRIMARY.", key="no-primary")

    assert barked(night) == []


def test_telegram_still_sends_inside_the_window(night, monkeypatch):
    """The point of silencing only the thing that makes a noise. Telegram
    cannot wake anyone and is the record of what happened overnight."""
    monkeypatch.setattr(core, "_now", lambda: at("03:00"))

    night.critical("ReplicaSet has no PRIMARY.", key="no-primary")

    assert len(telegrammed(night)) == 1
    assert barked(night) == []


def test_it_barks_outside_the_window(night, monkeypatch):
    monkeypatch.setattr(core, "_now", lambda: at("14:00"))

    night.bark("no primary", key="no-primary")

    assert len(barked(night)) == 1


def test_the_window_wraps_past_midnight(night, monkeypatch):
    """23:00-06:00 is one window, not an empty one, and 3am is the hour that
    matters -- it is on the far side of the date boundary."""
    monkeypatch.setattr(core, "_now", lambda: at("03:00"))

    night.bark("no primary", key="no-primary")

    assert barked(night) == []


def test_the_start_is_quiet_and_the_end_is_not(night, monkeypatch):
    """Half-open, so the window reads as it sounds: asleep at 23:00, awake
    at 06:00."""
    monkeypatch.setattr(core, "_now", lambda: at("23:00"))
    night.bark("a", key="a")
    assert barked(night) == []

    monkeypatch.setattr(core, "_now", lambda: at("06:00"))
    night.bark("b", key="b")
    assert len(barked(night)) == 1


def test_a_fault_held_through_the_night_rings_at_the_end_of_the_window(night, monkeypatch):
    """Suppressed, not swallowed. Nothing may be written to the cooldown
    clock while the window holds an alert, or the morning starts inside a
    half-hour cooldown for a page that never arrived."""
    for hour in (23, 0, 1, 2, 3, 4, 5):
        monkeypatch.setattr(core, "_now", lambda h=hour: at(f"{h}:30"))
        night.bark("no primary", key="no-primary")
    assert barked(night) == []

    monkeypatch.setattr(core, "_now", lambda: at("06:00"))
    night.bark("no primary", key="no-primary")

    assert len(barked(night)) == 1


def test_the_window_is_read_in_its_own_zone_not_the_containers(night, monkeypatch):
    """The trap this exists to avoid. Most deployables set no TZ, so the
    container clock is UTC -- an hour behind Amsterdam in winter and two in
    summer. A naive datetime.now() would hold 00:00-07:00 local."""
    monkeypatch.setattr(core, "_now", lambda: at("22:30", tz="UTC"))  # 23:30 CET
    night.bark("a", key="a")
    assert barked(night) == []

    monkeypatch.setattr(core, "_now", lambda: at("06:30", tz="UTC"))  # 07:30 CET
    night.bark("b", key="b")
    assert len(barked(night)) == 1


def test_the_window_follows_summer_time(night, monkeypatch):
    """ZoneInfo rather than a fixed offset, so it does not drift an hour
    every spring."""
    july = dt.datetime(2026, 7, 8, 21, 30, tzinfo=ZoneInfo("UTC"))  # 23:30 CEST
    monkeypatch.setattr(core, "_now", lambda: july)

    night.bark("no primary", key="no-primary")

    assert barked(night) == []


def test_no_window_configured_barks_at_any_hour(tooter, monkeypatch):
    """Unset is the behaviour this had before, which is what most of the ~20
    deployables embedding tooter want."""
    tooter.bark_quiet = None
    monkeypatch.setattr(core, "_now", lambda: at("03:00"))

    tooter.bark("no primary", key="no-primary")

    assert len(barked(tooter)) == 1


def test_an_empty_window_is_not_an_all_day_mute(tooter, monkeypatch):
    """A start equal to its end has no sensible reading as "all day", and
    guessing that way silences the pager completely and invisibly."""
    tooter.bark_quiet = core.QuietHours(dt.time(6, 0), dt.time(6, 0), ZoneInfo("Europe/Amsterdam"))
    monkeypatch.setattr(core, "_now", lambda: at("06:00"))

    tooter.bark("no primary", key="no-primary")

    assert len(barked(tooter)) == 1


# --- reading the window from the environment ------------------------------


def test_a_window_is_parsed_from_two_clock_strings():
    quiet = core.QuietHours.parse("23:00", "06:00", "Europe/Amsterdam")

    assert (quiet.start, quiet.end) == (dt.time(23, 0), dt.time(6, 0))
    assert quiet.covers(at("23:30"))


def test_an_unset_window_parses_to_nothing():
    assert core.QuietHours.parse("", "06:00", "Europe/Amsterdam") is None
    assert core.QuietHours.parse("23:00", "", "Europe/Amsterdam") is None


def test_a_bare_hour_is_accepted():
    """ "23" is what anyone types first."""
    assert core.QuietHours.parse("23", "6", "Europe/Amsterdam").start == dt.time(23, 0)


def test_an_unparseable_window_does_not_silence_the_pager():
    """Failing open is the only safe direction: a typo in an env var must not
    turn paging off, because silence looks exactly like a healthy estate."""
    assert core.QuietHours.parse("elevenish", "06:00", "Europe/Amsterdam") is None
    assert core.QuietHours.parse("23:00", "06:00", "Mars/Olympus_Mons") is None


def test_the_default_window_is_the_one_the_estate_asked_for():
    """23:00-06:00 Amsterdam. Stated as a test because it is a behaviour
    change for every deployable that sets BARK_URL and nothing else."""
    from ccdexplorer.env import BARK_QUIET_END, BARK_QUIET_START, BARK_QUIET_TZ

    assert (BARK_QUIET_START, BARK_QUIET_END, BARK_QUIET_TZ) == (
        "23:00",
        "06:00",
        "Europe/Amsterdam",
    )
