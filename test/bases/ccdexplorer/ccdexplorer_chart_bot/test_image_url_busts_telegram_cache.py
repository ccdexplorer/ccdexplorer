"""The image url has to change, or Telegram never fetches the chart again.

Telegram downloads a photo url once, keeps the file, and serves its own copy
every subsequent time it is handed that same url. So a url that never changes is
a chart that never updates: the Kraken charts went into the catalogue on
2026-09-25 and were still showing that day's render four days later, while
https://ccdexplorer.io/plots/mainnet/ccd_kraken_4h was current the whole time.

The fix is a bucketed timestamp in the query string. Bucketed rather than raw,
because a url that changes on every single message would make Telegram
re-download a chart that has not been redrawn, and would lose the file reuse
that makes sending one cheap.

The site is unaffected: its png cache is keyed on the path and theme, and the
path does not carry a query string, so this costs no extra renders.
"""

import pytest

from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS, BY_NAME

SITE = "https://ccdexplorer.io"


def _bare(url: str) -> str:
    """The url without its cache-busting parameter."""
    return url.split("&t=")[0]


def test_the_url_is_stable_within_a_bucket():
    """Two sends a second apart must reuse Telegram's copy, not refetch it."""
    chart = BY_NAME["ccd_kraken_1h"]

    assert chart.image_url(SITE, now=1_790_000_000.0) == chart.image_url(
        SITE, now=1_790_000_001.0
    )


def test_the_url_changes_once_the_bucket_rolls_over():
    chart = BY_NAME["ccd_kraken_1h"]
    before = chart.image_url(SITE, now=1_790_000_000.0)
    after = chart.image_url(SITE, now=1_790_000_000.0 + chart.refresh_seconds)

    assert before != after


@pytest.mark.parametrize(
    ("name", "seconds"),
    [
        ("ccd_kraken_1m", 60),
        ("ccd_kraken_5m", 300),
        ("ccd_kraken_15m", 900),
        ("ccd_kraken_30m", 1800),
        ("ccd_kraken_1h", 3600),
        ("ccd_kraken_4h", 3600),
        ("ccd_kraken_1d", 3600),
    ],
)
def test_each_candle_chart_refreshes_at_its_own_cadence(name, seconds):
    """A 1m chart spans two hours, so an hourly refresh is half of it stale."""
    chart = BY_NAME[name]

    assert chart.refresh_seconds == seconds
    # Aligned to a bucket boundary, or "still inside the same bucket" is not
    # what base + seconds - 1 means.
    base = float((1_790_000_000 // seconds) * seconds)
    assert chart.image_url(SITE, now=base) == chart.image_url(SITE, now=base + seconds - 1)
    assert chart.image_url(SITE, now=base) != chart.image_url(SITE, now=base + seconds)


def test_a_chart_that_is_not_a_candle_chart_refreshes_hourly():
    """The site's own png cache is an hour, so nothing gains from asking sooner."""
    assert BY_NAME["staking_validator_count"].refresh_seconds == 3600


def test_no_chart_asks_for_less_than_the_site_can_give():
    """Every cadence must be a whole number of seconds and at most an hour.

    Shorter than the site's matching png ttl would mean Telegram refetching an
    image the site has not redrawn.
    """
    for chart in CHARTS:
        assert 60 <= chart.refresh_seconds <= 3600


def test_busting_leaves_the_path_and_theme_exactly_as_they_were():
    """The site keys its png cache on path plus theme, so both must be untouched.

    If this drifted, every bucket would become a fresh kaleido render.
    """
    for chart in CHARTS:
        assert _bare(chart.image_url(SITE)) == (
            f"{SITE}/plots/mainnet/{chart.name}/image.png?theme=light"
        )


def test_the_parameter_is_a_plain_integer():
    """A float would change on every call and defeat the bucketing."""
    chart = BY_NAME["ccd_kraken_1d"]
    value = chart.image_url(SITE, now=1_790_000_000.5).split("&t=")[1]

    assert value.isdigit()


def test_the_page_link_is_not_busted():
    """It is a link for a person to open, not something Telegram caches."""
    for chart in CHARTS:
        assert "t=" not in chart.page_url(SITE)
