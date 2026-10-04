"""One chart, many windows, one cache.

The image routes used to be one route per variant -- transactions_count_90d --
so the path was the whole of the cache key. Now window and grouping are query
parameters, and the key has to grow with them or every variant overwrites the
last.
"""

from ccdexplorer.ccdexplorer_site.app.og_cards import PngCache
from ccdexplorer.ccdexplorer_site.app.utils import (
    _PLOT_IMAGES,
    plot_cache_key,
    plot_image_ttl,
)

PATH = "/plots/mainnet/transactions_count/image.png"


def test_window_and_grouping_are_part_of_the_key():
    a = plot_cache_key(PATH, "dark", {"window": "90d", "grouping": "weekly"})
    b = plot_cache_key(PATH, "dark", {"window": "30d", "grouping": "weekly"})
    assert a != b


def test_grouping_alone_separates_two_keys():
    a = plot_cache_key(PATH, "dark", {"window": "90d", "grouping": "weekly"})
    b = plot_cache_key(PATH, "dark", {"window": "90d", "grouping": "daily"})
    assert a != b


def test_parameter_order_does_not_change_the_key():
    """Two links to the same chart must not cost two renders."""
    a = plot_cache_key(PATH, "dark", {"window": "90d", "grouping": "weekly"})
    b = plot_cache_key(PATH, "dark", {"grouping": "weekly", "window": "90d"})
    assert a == b


def test_theme_still_separates_keys():
    assert plot_cache_key(PATH, "dark", {}) != plot_cache_key(PATH, "light", {})


def test_omitting_params_matches_the_old_key():
    """The unmigrated charts keep their existing entries."""
    assert plot_cache_key(PATH, "dark") == f"{PATH}?theme=dark"


def test_irrelevant_parameters_are_ignored():
    """Telegram's cache-busting `t` must not multiply the cache by the hour."""
    a = plot_cache_key(PATH, "dark", {"window": "90d", "t": "1"})
    b = plot_cache_key(PATH, "dark", {"window": "90d", "t": "2"})
    assert a == b


def test_ttl_still_resolves_through_the_query_string():
    assert plot_image_ttl("/plots/mainnet/ccd_kraken_1m/image.png") == 60


def test_the_plot_cache_outgrew_the_shared_default():
    """256 was comfortable at ~80 keys. On-demand windows evict the warmed
    defaults, which makes the warmer useless exactly when the bot is busy."""
    assert _PLOT_IMAGES._max >= 512
    assert PngCache()._max == 256  # the OG-card default is unchanged


def test_explicit_dates_are_part_of_the_key():
    """The image routes honour ?from=/&to=, so the key has to carry them.

    Without this, two requests for different years resolve to one key -- and
    it is the same key the warmer repopulates, so a single shared link
    carrying a date range serves its picture to everyone asking for the
    default for up to an hour.
    """
    a = plot_cache_key(PATH, "dark", {"from": "2022-01-01", "to": "2022-06-30"})
    b = plot_cache_key(PATH, "dark", {"from": "2025-01-01", "to": "2025-06-30"})
    assert a != b


def test_a_date_range_does_not_collide_with_the_warmed_default():
    ranged = plot_cache_key(PATH, "dark", {"from": "2022-01-01", "to": "2022-06-30"})
    default = plot_cache_key(PATH, "dark", {"window": "1y", "grouping": "weekly"})
    assert ranged != default
