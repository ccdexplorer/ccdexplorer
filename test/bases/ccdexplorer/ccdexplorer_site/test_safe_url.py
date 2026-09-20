"""URLs that reach an href must carry a scheme a browser can safely follow.

metadata_url, a validator's pool_info.url and a module's link_to_source_code
are all supplied by whoever deployed the thing being displayed. Escaping keeps
them inside the attribute; it does not stop `href="javascript:..."`, which
needs no quotes to break out of and runs on this origin on one click.
"""

import pytest

from ccdexplorer.ccdexplorer_site.app.utils import LINKABLE_URL_SCHEMES, safe_url


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/metadata.json",
        "http://example.org/metadata.json",
        "HTTPS://Example.ORG/Metadata.json",  # scheme match is case-insensitive
        "https://example.org/a b",  # spaces inside the path are not our problem
    ],
)
def test_a_real_link_is_returned_unchanged(url):
    assert safe_url(url) == url.strip()


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(document.domain)",
        "JavaScript:alert(1)",
        "  javascript:alert(1)",  # browsers ignore leading whitespace
        "java\tscript:alert(1)",  # ...and tabs inside the scheme
        "java\nscript:alert(1)",  # ...and newlines
        "jav\rascript:alert(1)",
        "data:text/html,<script>alert(1)</script>",
        "vbscript:msgbox(1)",
        "file:///etc/passwd",
        "//evil.example.com/x",  # no scheme: inherits ours, leaves origin
        "",
        "   ",
        None,
    ],
)
def test_anything_else_becomes_an_empty_href(url):
    assert safe_url(url) == ""


def test_only_http_and_https_are_linkable():
    assert LINKABLE_URL_SCHEMES == frozenset({"http", "https"})


def test_a_token_with_an_unusable_url_still_renders():
    """Returning "" rather than raising keeps the page up, just without a link."""
    assert safe_url("javascript:alert(1)") == ""
