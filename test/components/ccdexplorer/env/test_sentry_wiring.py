"""Every service that reports to Sentry scrubs what it sends, once.

The site initialised Sentry twice -- the second call won, and the two had to
be kept in step by hand. The bot, which handles Telegram messages, now scrubs
too.
"""

import ast
from pathlib import Path


def _inits(path):
    tree = ast.parse(Path(path).read_text())
    return [
        {kw.arg: kw.value for kw in node.keywords}
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "init"
        and getattr(node.func.value, "id", "") == "sentry_sdk"
    ]


def test_the_site_initialises_sentry_exactly_once():
    assert len(_inits("bases/ccdexplorer/ccdexplorer_site/app/factory.py")) == 1


def test_the_site_traces_a_fraction_scrubbed_and_unprofiled():
    (kw,) = _inits("bases/ccdexplorer/ccdexplorer_site/app/factory.py")
    assert isinstance(kw["traces_sample_rate"], ast.Call)
    assert getattr(kw["before_send"], "id", "") == "scrub_sentry_event"
    assert getattr(kw["before_send_transaction"], "id", "") == "scrub_sentry_event"
    assert "_experiments" not in kw


def test_the_bot_scrubs_what_it_sends():
    (kw,) = _inits("bases/ccdexplorer/ccdexplorer_bot/__main__.py")
    assert getattr(kw["before_send"], "id", "") == "scrub_sentry_event"
