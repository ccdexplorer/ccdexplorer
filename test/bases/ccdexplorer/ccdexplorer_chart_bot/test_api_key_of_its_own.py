"""The chart bot answers strangers, so it carries a key of its own.

direct.py sent the site's shared CCDEXPLORER_API_KEY. CHART_BOT_API_KEY is used
when it is set -- a low-tier key that can be revoked without touching anything
else -- and the shared one until then, so setting it is a change of
configuration, not of code.
"""

from ccdexplorer.ccdexplorer_chart_bot import direct
from ccdexplorer.env import environment


def test_its_own_key_wins_when_it_is_set(monkeypatch):
    monkeypatch.setitem(environment, "CHART_BOT_API_KEY", "chart-key")
    monkeypatch.setitem(environment, "CCDEXPLORER_API_KEY", "shared-key")
    assert direct.api_headers() == {"x-ccdexplorer-key": "chart-key"}


def test_the_shared_key_is_the_fallback(monkeypatch):
    monkeypatch.delitem(environment, "CHART_BOT_API_KEY", raising=False)
    monkeypatch.setitem(environment, "CCDEXPLORER_API_KEY", "shared-key")
    assert direct.api_headers() == {"x-ccdexplorer-key": "shared-key"}


def test_an_empty_setting_counts_as_unset(monkeypatch):
    monkeypatch.setitem(environment, "CHART_BOT_API_KEY", "")
    monkeypatch.setitem(environment, "CCDEXPLORER_API_KEY", "shared-key")
    assert direct.api_headers() == {"x-ccdexplorer-key": "shared-key"}


def test_both_api_calls_use_the_helper():
    import inspect

    source = inspect.getsource(direct)
    assert 'environment["CCDEXPLORER_API_KEY"]},' not in source
    assert source.count("headers=api_headers()") == 2
