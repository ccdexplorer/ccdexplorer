"""The bot offers every chart the site lists.

The catalogue is a hand-written tuple, so a chart added to the registry and
listed on the site reaches the gallery and never reaches the bot. That is how
holders and active_addresses went missing: holders was unlisted on the site
and then relisted without anyone touching the bot, and active_addresses was
never added at all.

The guard is the point of this file. The two charts below are the ones that
were missing when it was written; the test that matters is the one that will
fail for the next one.
"""

import pytest

from ccdexplorer.ccdexplorer_chart_bot.catalogue import (
    BY_NAME,
    CHARTS,
    resolve_families,
    search,
)
from ccdexplorer.charts.registry import ALL_SPECS

NEW = ("holders", "active_addresses")


def _listed():
    return {spec.name for spec in ALL_SPECS if spec.listed}


def _hits(query):
    return [chart.name for chart in resolve_families(search(query))]


# --- the guard --------------------------------------------------------------


def test_every_listed_site_chart_is_offered_by_the_bot():
    """A chart on the site's gallery that the bot cannot send is a chart the
    bot's own menu claims a category for and then does not hold.

    Listed, not served. test_catalogue's
    test_charts_the_site_serves_but_the_bot_does_not_offer deliberately only
    prints its list, so that a chart can have a route on the site and be
    added to the bot later rather than both having to ship in one commit.
    That latitude is kept: this asserts on spec.listed, which is the point a
    chart stops proving itself and goes on the gallery, where leaving it out
    of the bot means the bot's accounts button claims a category it does not
    hold.

    No exceptions. The reverse direction has plenty -- the six non-default
    Kraken intervals and staking_validator_staked_amounts are in the bot and
    not on the gallery -- but that is a family being collapsed to one tile,
    which is deliberate and tested in test_gallery_is_curated.
    """
    missing = sorted(_listed() - {chart.name for chart in CHARTS})

    assert missing == [], f"listed on the site, unreachable from the bot: {missing}"


@pytest.mark.parametrize("name", NEW)
def test_the_chart_is_in_the_catalogue(name):
    assert name in BY_NAME


@pytest.mark.parametrize("name", NEW)
def test_the_chart_is_spec_backed(name):
    """Drawn from the registry rather than carrying its own title and
    keywords, like everything migrated."""
    chart = BY_NAME[name]

    assert chart.spec_name == name
    assert chart.spec is not None


@pytest.mark.parametrize("name", NEW)
def test_the_chart_sits_in_accounts(name):
    """Where the site files it, which test_each_chart_sits_where_the_site_puts_it
    checks for the catalogue as a whole."""
    assert BY_NAME[name].group == "accounts"
    assert BY_NAME[name].spec.category == "accounts"


# --- holders reaches holders ------------------------------------------------
#
# `/e holders` sent Daily limits. The haystack search reads names, titles,
# descriptions, groups and keywords, and daily_limits carried "holders" as a
# keyword from when no holders chart was reachable -- so the word led to the
# chart about the top 100 rather than the one counting accounts by balance.


def test_holders_sends_the_holders_chart():
    assert _hits("holders") == ["holders"]


def test_daily_limits_no_longer_answers_to_holders():
    assert "daily_limits" not in _hits("holders")


def test_daily_limits_keeps_the_words_that_are_its_own():
    """It is still the chart about who is at the top; only "holders" moved."""
    for word in ("whales", "rich", "top", "leaderboard", "ranking"):
        assert _hits(word) == ["daily_limits"], word


@pytest.mark.parametrize("word", ("balances", "millionaires", "thresholds"))
def test_the_words_for_a_balance_threshold_reach_holders(word):
    assert _hits(word) == ["holders"], word


# --- active_addresses is reachable at all -----------------------------------


@pytest.mark.parametrize("word", ("active", "addresses", "unique", "dau"))
def test_the_words_for_active_addresses_reach_it(word):
    """All four matched nothing before: the chart was not in the catalogue,
    so the bot answered "No chart matches" to its own gallery."""
    assert _hits(word) == ["active_addresses"], word


# --- both arrive as a chart rather than a question --------------------------


@pytest.mark.parametrize("name", NEW)
def test_the_chart_is_a_single_match_for_its_own_name(name):
    """More than one match opens the picker instead of sending the chart, so
    a new entry whose words collide with an existing one makes two charts
    harder to reach rather than one easier."""
    assert _hits(name.replace("_", " ")) == [name]


@pytest.mark.parametrize("name", NEW)
def test_the_chart_is_in_its_category_menu(name):
    """The accounts button has to lead to it, or it is reachable only by
    someone who already knows the word."""
    from ccdexplorer.ccdexplorer_chart_bot.direct import CATEGORY_ORDER

    assert BY_NAME[name].group in CATEGORY_ORDER
    behind_the_button = [c.name for c in CHARTS if c.group == "accounts"]
    assert name in behind_the_button
