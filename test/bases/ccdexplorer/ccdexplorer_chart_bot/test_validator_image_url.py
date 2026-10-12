"""The url /v hands Telegram must be one the site answers with a png.

Telegram fetches the url itself and has no way to say "and please make it
an image". The site decides that from the path: return_plot_response
renders png only when the path contains "image.png", and otherwise
returns the interactive page -- about five megabytes of html. Handed that,
Telegram keeps downloading until the bot's request times out, which is
exactly what /v 72723 did in production on 2026-10-11.
"""

import pytest

from ccdexplorer.ccdexplorer_chart_bot.validator import VIEWS, image_url

SITE = "https://ccdexplorer.io"
ADDRESS = "3Y1RLgi5pW3ay4321oJ7CH1rCT3WRt6Vkl8pS8Nn5c2Cwp7Uk8"


def _path(url: str) -> str:
    return url.split("?")[0].removeprefix(SITE)


@pytest.mark.parametrize("view", VIEWS)
def test_every_view_asks_for_an_image(view):
    assert _path(image_url(SITE, 72723, view, ADDRESS)).endswith("/image.png")


@pytest.mark.parametrize("view", VIEWS)
def test_the_site_gate_matches(view):
    """The gate is a substring test, so this is the condition itself."""
    assert "image.png" in _path(image_url(SITE, 72723, view, ADDRESS))
