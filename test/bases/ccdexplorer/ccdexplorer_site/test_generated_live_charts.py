"""A page for a chart whose data is live.

Both of these had a hand-written route and no page: the generated page only
knew how to offer a grouping and a date range over a collection. The figure
comes from a provider now and the state from one path segment, so they get
the same page, share card, csv and docs link as everything else.
"""

import pytest

from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import can_be_generated, router

PATHS = {getattr(r, "path", "") for r in router.routes}

KRAKEN_NAMES = [f"ccd_kraken_{code}" for code in ("1m", "5m", "15m", "30m", "1h", "4h", "1d")]
LIVE_NAMES = ["cooldown_schedule", *KRAKEN_NAMES]


def test_a_live_chart_can_be_generated():
    for name in LIVE_NAMES:
        assert can_be_generated(BY_NAME[name]), name


def test_a_spec_naming_an_unregistered_provider_gets_no_page():
    """Otherwise it registers a page that answers 500 to every caller."""
    spec = BY_NAME["ccd_kraken_4h"].model_copy(update={"live_source": "nothing_registered"})
    assert not can_be_generated(spec)


def test_the_kraken_family_registers_one_page_not_seven():
    """One chart seven ways. Registered per spec, six of the seven routes
    would be shadowed by the first and only look like they worked."""
    assert "/{net}/charts/ccd-kraken" in PATHS
    assert "/{net}/charts/ccd-kraken/{segment}" in PATHS
    assert not [p for p in PATHS if p.startswith("/{net}/charts/ccd-kraken-")]


def test_the_schedule_has_a_page_and_a_lookback_path():
    assert "/{net}/charts/cooldown-schedule" in PATHS
    assert "/{net}/charts/cooldown-schedule/{segment}" in PATHS


@pytest.mark.parametrize("name", LIVE_NAMES)
def test_every_existing_plots_url_still_exists(name):
    """These are in Telegram's file cache and in links people have shared.
    The hand-written routes that served them are gone; these replace them at
    the identical path."""
    assert f"/plots/{{net}}/{name}/image.png" in PATHS
    assert f"/plots/{{net}}/{name}" in PATHS


@pytest.mark.parametrize("name", LIVE_NAMES)
def test_a_live_chart_takes_its_axis_in_the_image_path(name):
    assert f"/plots/{{net}}/{name}/{{segment}}/image.png" in PATHS


def test_a_live_chart_has_no_calendar_routes():
    """A grouping and two months mean nothing here, and a route accepting
    them would answer 200 to a url that names no chart."""
    for slug in ("ccd-kraken", "cooldown-schedule"):
        assert f"/{{net}}/charts/{slug}/{{grouping}}/{{start}}/{{end}}" not in PATHS


def test_a_live_page_has_a_csv_and_a_data_endpoint():
    for slug in ("ccd-kraken", "cooldown-schedule"):
        assert f"/{{net}}/charts/{slug}/{{segment}}/data.csv" in PATHS, slug
        assert f"/{{net}}/charts/{slug}/{{segment}}/data" in PATHS, slug


def test_the_csv_route_is_registered_before_the_segment_route():
    """FastAPI matches in registration order and {segment} is happy to
    capture the literal "data.csv" -- which is exactly what it did once, so
    every chart's Download Data link answered 404."""
    order = [getattr(r, "path", "") for r in router.routes]
    for slug in ("ccd-kraken", "cooldown-schedule"):
        csv = order.index(f"/{{net}}/charts/{slug}/{{segment}}/data.csv")
        segment = order.index(f"/{{net}}/charts/{slug}/{{segment}}")
        assert csv < segment, slug


def test_a_calendar_chart_keeps_its_routes():
    assert "/{net}/charts/transaction-fees/{grouping}/{start}/{end}" in PATHS
    assert "/{net}/charts/transaction-fees/{grouping}/{start}/{end}/data.csv" in PATHS


def test_no_two_routes_claim_the_same_path_and_method():
    """The hand-written Kraken routes lived in statistics.py and these
    replace them at the same paths. Left in place, whichever router the
    factory included first would silently win."""
    seen = set()
    for route in router.routes:
        for method in getattr(route, "methods", set()) or set():
            key = (method, getattr(route, "path", ""))
            assert key not in seen, key
            seen.add(key)
