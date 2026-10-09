"""The two charts that were plots.

Both are live: the node's current cooldown state, and one exchange's order
book. Neither has a date-keyed collection, which is why neither had a page --
the page only knew how to offer a grouping and a date range.
"""

from ccdexplorer.charts.models import Axis, Interval, Window
from ccdexplorer.charts.registry import ALL_SPECS, BY_NAME, BY_SLUG, BY_SOURCE

KRAKEN = [s for s in ALL_SPECS if s.name.startswith("ccd_kraken")]


def test_the_schedule_is_a_lookback_chart():
    spec = BY_NAME["cooldown_schedule"]
    assert spec.axis is Axis.LOOKBACK
    assert spec.horizon_days == 7
    assert spec.live_source == "cooldown_schedule"


def test_the_schedule_offers_four_lookbacks_and_opens_at_all():
    """`all` is what the average line was measured over before this was a
    control, so making it configurable does not move the line."""
    spec = BY_NAME["cooldown_schedule"]
    assert spec.windows == (Window.D30, Window.D90, Window.Y1, Window.ALL)
    assert spec.default_window is Window.ALL


def test_the_schedule_has_a_page_now():
    """It was listed in the gallery with has_page False, so its tile led back
    to the category page it was clicked from."""
    assert BY_NAME["cooldown_schedule"].has_page
    assert BY_NAME["cooldown_schedule"].listed


def test_the_schedule_offers_no_grouping():
    assert BY_NAME["cooldown_schedule"].groupings == ()


def test_all_seven_kraken_charts_are_interval_charts():
    assert len(KRAKEN) == 7
    for spec in KRAKEN:
        assert spec.axis is Axis.INTERVAL, spec.name
        assert spec.live_source == "kraken_ohlc", spec.name
        assert spec.has_page, spec.name


def test_every_kraken_chart_offers_every_interval():
    """The page's interval row leads to its siblings, so each has to know
    them all."""
    expected = (
        Interval.M1,
        Interval.M5,
        Interval.M15,
        Interval.M30,
        Interval.H1,
        Interval.H4,
        Interval.D1,
    )
    for spec in KRAKEN:
        assert spec.intervals == expected, spec.name


def test_each_kraken_chart_opens_at_its_own_interval():
    for spec in KRAKEN:
        code = spec.name.removeprefix("ccd_kraken_")
        assert spec.default_interval.value == code, spec.name


def test_the_kraken_family_shares_one_page():
    for spec in KRAKEN:
        assert spec.page_slug == "ccd-kraken", spec.name


def test_the_kraken_slugs_stay_distinct():
    """page_slug is shared; slug is not. BY_SLUG is keyed on slug, and seven
    specs sharing one would silently leave six unreachable."""
    assert len({s.slug for s in KRAKEN}) == 7
    for spec in KRAKEN:
        assert BY_SLUG[spec.slug] is spec


def test_only_one_kraken_chart_is_listed():
    """One chart seven ways. Seven tiles of the same picture is not seven
    charts."""
    assert [s.name for s in KRAKEN if s.listed] == ["ccd_kraken_4h"]


def test_the_kraken_charts_keep_their_route_names():
    """These are in Telegram's file cache and in links people have shared,
    and the bot addresses all seven by name."""
    assert {s.name for s in KRAKEN} == {
        f"ccd_kraken_{code}" for code in ("1m", "5m", "15m", "30m", "1h", "4h", "1d")
    }


def test_neither_live_chart_reaches_the_grouping_pipeline():
    """BY_SOURCE is keyed on the mongo type, and neither of these has one.
    Present there, the api would be asked to group a collection that does
    not exist."""
    for spec in [*KRAKEN, BY_NAME["cooldown_schedule"]]:
        assert spec.source == "", spec.name
    assert "" not in BY_SOURCE


def test_the_price_charts_are_untouched():
    """ccd_price_24h/90d/1y draw the chain's own fee rate, not the order
    book, and are out of scope. Pinned so a later pass does not fold them in
    by accident."""
    for code in ("24h", "90d", "1y"):
        spec = BY_NAME[f"ccd_price_{code}"]
        assert spec.axis is Axis.CALENDAR
        assert not spec.has_page
