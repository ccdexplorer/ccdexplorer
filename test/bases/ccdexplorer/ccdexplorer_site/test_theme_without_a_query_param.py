"""Which theme an image is drawn in when nothing asks for one.

The page POSTs its theme in the body, so it never shows up in a url. The
image routes are GETs -- an <img src> and Telegram's own fetch -- so they
cannot do that, and theme was the one parameter left in those urls.

A cookie carries it instead. The browser sends it with every <img>, so the
site's own thumbnails finally match the theme the reader chose; today they
are dark whatever that is, because the theme lives in localStorage and no
request ever sees it.

Telegram sends no cookie, and light is what it should get, so light is the
fallback and the bot's urls lose their last parameter.
"""

from types import SimpleNamespace

from ccdexplorer.ccdexplorer_site.app.utils import PLOT_THEMES, theme_from_query


def _request(query=None, cookies=None):
    return SimpleNamespace(query_params=query or {}, cookies=cookies or {})


def test_an_explicit_query_still_wins():
    """The warmer draws both themes by asking for each by name."""
    assert theme_from_query(_request({"theme": "dark"})) == "dark"
    assert theme_from_query(_request({"theme": "light"})) == "light"


def test_the_cookie_is_used_when_the_query_says_nothing():
    assert theme_from_query(_request(cookies={"bsTheme": "dark"})) == "dark"


def test_the_query_beats_the_cookie():
    assert theme_from_query(_request({"theme": "light"}, {"bsTheme": "dark"})) == "light"


def test_nothing_at_all_means_light():
    """Telegram sends neither, and a chart lands in somebody else's chat --
    mostly a light one, where a dark chart reads as a black rectangle."""
    assert theme_from_query(_request()) == "light"


def test_a_nonsense_cookie_is_ignored():
    assert theme_from_query(_request(cookies={"bsTheme": "chartreuse"})) == "light"


def test_a_nonsense_query_falls_through_to_the_cookie():
    assert theme_from_query(_request({"theme": "nope"}, {"bsTheme": "dark"})) == "dark"


def test_both_themes_are_still_reachable():
    assert set(PLOT_THEMES) == {"dark", "light"}
