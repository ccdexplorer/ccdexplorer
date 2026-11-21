"""`/search` answers with a redirect, and the url ends up in two headers.

Sentry (CCDEXPLORER-IO-2Q6, 3 events on 2026-11-18) caught a search for
"Bahamas " -- a trailing space, which a pasted word often carries -- coming
back as

    LocalProtocolError: Illegal header value b'/mainnet/search_all/Bahamas '

The server refuses to send a header with whitespace at its end, so the person
got an error page instead of a result. The value went into the url exactly as
typed. Anything that is not plain url text -- a space, a newline, a tab, a
character outside ASCII, a "?" that would cut the path short -- has to be
encoded, and the trailing whitespace of a search is not part of what was asked.
"""

import re

import pytest

from ccdexplorer.ccdexplorer_site.app.routers.home import SearchRequest, search

#: What a url in a header may contain: no whitespace, no control characters.
HEADER_SAFE = re.compile(r"^/[\x21-\x7e]*$")

SELECTORS = ["all", "account", "block", "transaction", "contract", "module", "lock", "token"]


async def _redirect(value: str, selector: str = "all", net: str = "mainnet"):
    return await search(
        request=None, search_request=SearchRequest(selector=selector, value=value, net=net)
    )


async def test_a_trailing_space_is_not_part_of_the_search():
    response = await _redirect("Bahamas ")

    assert response.headers["HX-Redirect"] == "/mainnet/search_all/Bahamas"


async def test_the_location_header_agrees_with_the_hx_redirect_header():
    response = await _redirect("Bahamas ")

    assert response.headers["location"] == response.headers["HX-Redirect"]


@pytest.mark.parametrize("selector", SELECTORS)
@pytest.mark.parametrize(
    "value",
    ["two words ", "tab\there", "new\nline", "café", "what?", "a#b", " lead", "nul\x00"],
)
async def test_whatever_is_typed_the_headers_are_plain_url_text(selector, value):
    response = await _redirect(value, selector=selector)

    for name in ("HX-Redirect", "location"):
        assert HEADER_SAFE.match(response.headers[name]), (selector, value, response.headers[name])


async def test_an_inner_space_is_encoded_not_dropped():
    response = await _redirect("south africa")

    assert response.headers["HX-Redirect"] == "/mainnet/search_all/south%20africa"


async def test_a_question_mark_stays_inside_the_path():
    """Unencoded it would start a query string and cut the search short."""
    response = await _redirect("what?")

    assert "?" not in response.headers["HX-Redirect"]
