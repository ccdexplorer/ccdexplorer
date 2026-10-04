"""Render the charts index covers once, as files, two per category.

Each cover used to be drawn through kaleido when the index was requested:
six full-size figures before the page appeared, and the index is the first
thing anyone opens.

Two changes since the first version.

They were greyscale, on the theory that a cover is a signpost rather than a
reading. At thumbnail size that removed the only thing telling these charts
apart, so the index showed six tiles of grey marks on black that looked like
the same picture six times. They carry their chart's own colours now.

And there is one per theme. A stored file cannot follow the theme toggle the
way an htmx-driven plot can, so a reader who switched to light went on
looking at dark tiles until they reloaded.

Each cover is fetched from the site's own image route rather than built
here. The first version called the generic figure builder directly, which
limited the covers to charts that builder can draw -- it could not draw the
Kraken candles or PLT's TVL, so exchanges got a chart nobody chose and PLT
got no tile at all. Going through the route reaches every chart by the same
path a reader does, and `?cover=1` is what strips the title and legend.

Needs a running api and site. Run `just api` and `just site` (or
`just bot-tunnel`, which starts both), then `just chart-covers`.
"""

import os
import sys

import httpx2 as httpx

from ccdexplorer.ccdexplorer_site.app.routers.charts.charts_home import (
    categories_with_counts,
    cover_chart,
)

OUT = "projects/ccdexplorer_site/static/charts"
SITE = os.environ.get("COVER_SITE_URL", "http://127.0.0.1:8000")
THEMES = ("light", "dark")
NET = "mainnet"


def cover_url(name: str, theme: str) -> str:
    return f"{SITE}/plots/{NET}/{name}/image.png?theme={theme}&cover=1"


def main() -> int:
    try:
        httpx.get(f"{SITE}/{NET}", timeout=10.0)
    except Exception as error:
        print(f"cannot reach the site at {SITE} ({error}).")
        print("start it with `just site` (and `just api`), or set COVER_SITE_URL.")
        return 1

    written = 0
    for category, _count in categories_with_counts():
        spec = cover_chart(category)
        if spec is None:
            print(f"{category}: no chart named in COVER_CHART, skipped")
            continue
        for theme in THEMES:
            url = cover_url(spec.name, theme)
            # Generous: a cold cover is a real kaleido render, and the two
            # themes of six categories are twelve of them.
            response = httpx.get(url, timeout=180.0)
            if response.status_code != 200:
                print(f"{category} [{theme}]: {url} answered {response.status_code}")
                return 1
            if not response.headers.get("content-type", "").startswith("image/"):
                print(f"{category} [{theme}]: {url} answered html, not an image")
                return 1
            with open(f"{OUT}/{category}-{theme}.png", "wb") as out:
                out.write(response.content)
            written += 1
        print(f"{category}: {spec.name}")
    print(f"{written} cover(s) written to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
