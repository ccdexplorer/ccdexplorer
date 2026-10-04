"""The menu leads to /charts, and /statistics stays reachable.

The chart pages are where a reader can change anything, so that is what the
menu offers. /statistics keeps working -- its urls have been shared, and it
is still the only place some charts are drawn -- it is just no longer the
advertised way in.
"""

import re
from pathlib import Path

TEMPLATES = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site" / "templates"
NAVBAR = TEMPLATES / "base" / "navbar.html"


def _nav_links() -> list[str]:
    return re.findall(r'class="nav-link[^"]*"\s+href="([^"]+)"', NAVBAR.read_text())


def test_the_menu_offers_charts():
    assert any("/charts" in link for link in _nav_links())


def test_the_menu_no_longer_offers_statistics():
    """Not removed from the site, just no longer advertised."""
    assert not any(link.endswith("/statistics") for link in _nav_links())


def test_the_charts_entry_is_labelled_charts():
    body = NAVBAR.read_text()
    charts = body[body.index('href="/{{net}}/charts"') :][:200]
    assert "Charts" in charts
