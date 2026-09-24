"""The chart bot offers charts the site can actually draw.

The catalogue is written out here rather than imported from the site, because
a base may not import another base and because an inline result wants one line
where a page wants a paragraph. The cost of that is drift: a chart added to the
site would never appear in the bot, and one removed would leave the bot
offering a URL that returns an HTML error page to Telegram.

So the first test reads the site's routes and compares. It is the reason the
duplication is safe.
"""

import re
from pathlib import Path

import pytest

from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS, MAX_RESULTS, NET, search
from ccdexplorer.ccdexplorer_chart_bot.inline import nothing_found, photo_result, results_for

SITE = "https://ccdexplorer.io"
ROUTERS = Path(__file__).resolve().parents[4] / "bases/ccdexplorer/ccdexplorer_site/app/routers"


def site_plot_names() -> set[str]:
    source = "".join(p.read_text() for p in ROUTERS.rglob("*.py"))
    return set(re.findall(r'"/plots/\{net\}/([a-z0-9_]+)/image\.png"', source))


def test_the_catalogue_matches_the_charts_the_site_serves():
    catalogue = {chart.name for chart in CHARTS}
    served = site_plot_names()
    assert catalogue == served, (
        f"only in the bot: {sorted(catalogue - served)}; "
        f"only on the site: {sorted(served - catalogue)}"
    )


def test_every_chart_has_a_title_and_a_one_line_description():
    for chart in CHARTS:
        assert chart.title and not chart.title.endswith(".")
        assert chart.description and len(chart.description) < 60, chart.name


def test_names_are_unique():
    assert len({chart.name for chart in CHARTS}) == len(CHARTS)


# --- the urls -------------------------------------------------------------


def test_the_image_url_is_the_one_the_site_serves():
    chart = next(c for c in CHARTS if c.name == "staking_validator_count")
    assert chart.image_url(SITE) == f"{SITE}/plots/mainnet/staking_validator_count/image.png"
    assert chart.page_url(SITE) == f"{SITE}/plots/mainnet/staking_validator_count"


def test_a_trailing_slash_on_the_site_url_does_not_double_up():
    chart = CHARTS[0]
    assert "//plots" not in chart.image_url("https://ccdexplorer.io/")


def test_every_chart_is_mainnet():
    """The routes exist for other nets and answer 200 with an HTML error page.

    Telegram would fetch that and show a broken result, so the net is not a
    parameter anywhere in this bot.
    """
    assert NET == "mainnet"
    assert all("/plots/mainnet/" in c.image_url(SITE) for c in CHARTS)


# --- search ---------------------------------------------------------------


def test_an_empty_query_shows_the_catalogue_rather_than_nothing():
    """Someone who types the bot's name and stops should see what there is."""
    assert len(search("")) == MAX_RESULTS
    assert search("   ") == search("")


@pytest.mark.parametrize(
    "query, expected",
    [
        ("staking_validator_count", "staking_validator_count"),
        ("validator stake", "staking_validator_staked_amounts"),
        ("delegators per pool", "staking_avg_delegator_per_pool_count"),
        ("fees", "transaction_fees"),
        ("exchanges", "ccd_on_exchanges"),
        ("accounts per day", "accounts_per_day"),
    ],
)
def test_a_query_finds_the_chart_it_describes(query, expected):
    assert search(query)[0].name == expected


def test_every_word_has_to_match():
    """Narrowing, not widening: a list that ignores half of what was typed
    reads as broken."""
    assert len(search("validator")) > len(search("validator stake"))


def test_an_unmatched_query_returns_nothing_from_search():
    assert search("kittens") == []


def test_results_are_capped():
    assert len(search("", limit=3)) == 3


# --- inline results -------------------------------------------------------


def test_a_match_becomes_a_photo_telegram_can_fetch():
    result = photo_result(CHARTS[0], SITE)
    assert result.photo_url == result.thumbnail_url == CHARTS[0].image_url(SITE)
    assert (result.photo_width, result.photo_height) == (1200, 630)


def test_a_miss_is_an_article_not_an_empty_answer():
    """An empty answer renders as a silent empty panel, which reads as the bot
    being broken rather than the query being wrong."""
    results = results_for("kittens", SITE)
    assert len(results) == 1
    assert results[0].id == "no-match"
    assert "kittens" in results[0].title


def test_the_miss_message_points_somewhere_useful():
    article = nothing_found("kittens", SITE)
    assert SITE in article.input_message_content.message_text


def test_a_query_that_matches_returns_only_photos():
    results = results_for("staking", SITE)
    assert results
    assert all(r.id != "no-match" for r in results)


# --- the entry point ------------------------------------------------------


def test_the_entry_point_imports():
    """It was not imported by anything, so nothing caught that SITE_URL was
    never exported from ccdexplorer.env -- the whole suite passed green while
    the bot could not have started. The site base carries the same test for
    the same reason."""
    from ccdexplorer.ccdexplorer_chart_bot import __main__

    assert callable(__main__.main)


def test_it_refuses_to_start_without_a_token(monkeypatch):
    """A bot with no token polls nothing and says nothing; failing loudly at
    boot is better than a container that looks healthy and is deaf."""
    from ccdexplorer.ccdexplorer_chart_bot import __main__

    monkeypatch.setattr(__main__, "CHART_BOT_TOKEN", "")
    with pytest.raises(SystemExit):
        __main__.main()
