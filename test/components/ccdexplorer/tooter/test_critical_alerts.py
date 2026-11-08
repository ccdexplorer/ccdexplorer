"""Paging, as opposed to notifying.

Every alert this estate sends went to Telegram, which obeys silent mode and
Do Not Disturb -- correct for the hundred informational messages a day and
useless for the replica set losing its primary at 3am. `Tooter.critical` adds
an iOS critical alert via Bark on top of the unchanged Telegram message.

Two properties matter more than the transport and are what most of this file
pins: a Bark problem must never swallow the Telegram alert, and one ongoing
fault must not ring sixty times an hour.
"""

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
