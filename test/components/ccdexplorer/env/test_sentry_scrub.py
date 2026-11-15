"""What Sentry is told about a request.

Identity tokens, reset and verification tokens, login codes and API keys all
travel in url paths here, so every span and request url carried one into a
third party's storage -- on every trace, because sampling was 100%.
"""

import pytest

from ccdexplorer.env.settings import (
    scrub_sentry_event,
    scrub_url,
    sentry_traces_sample_rate,
)

UUID = "528e6511-d55a-49d3-a4f1-fcce5eef03cc"


@pytest.mark.parametrize(
    "url",
    [
        f"https://api.ccdexplorer.io/site-auth/{UUID}/sessions",
        f"https://api.ccdexplorer.io/v2/site_user/{UUID}",
        f"https://api.ccdexplorer.io/auth/reset-password-action/{UUID}",
        f"https://ccdexplorer.io/auth/reset-password/{UUID}",
    ],
)
def test_a_uuid_in_a_path_is_masked(url):
    out = scrub_url(url)
    assert UUID not in out
    assert out.split("/")[2] == url.split("/")[2]  # host untouched


def test_a_login_code_after_token_is_masked():
    assert "ABCD-EFGH" not in scrub_url("https://ccdexplorer.io/token/ABCD-EFGH")


def test_a_verification_link_is_masked_whatever_shape_the_token_has():
    assert "s3cretvalue" not in scrub_url(
        "https://api.ccdexplorer.io/site-auth/verify-email/s3cretvalue"
    )


@pytest.mark.parametrize("name", ["token", "code", "key", "api_key", "password", "access_token"])
def test_a_secret_query_parameter_is_masked(name):
    out = scrub_url(f"https://x.example/path?{name}=hunter2&page=3")
    assert "hunter2" not in out
    assert "page=3" in out


def test_an_ordinary_url_is_left_alone():
    url = "https://api.ccdexplorer.io/v2/mainnet/block/last_final?limit=10"
    assert scrub_url(url) == url


def test_account_addresses_are_not_mistaken_for_secrets():
    url = "https://api.ccdexplorer.io/v2/mainnet/account/3sPmDZ5vYUP6pjqjt4zWTyv1wTMkfbFPxkqvJNYhWnvAZBUiQm/info"
    assert scrub_url(url) == url


def test_the_event_is_scrubbed_in_every_place_a_url_lives():
    event = {
        "request": {
            "url": f"https://api.ccdexplorer.io/site-auth/{UUID}/sessions",
            "query_string": "token=hunter2",
        },
        "spans": [
            {
                "description": f"GET https://api.ccdexplorer.io/v2/site_user/{UUID}",
                "data": {"http.url": f"https://api.ccdexplorer.io/v2/site_user/{UUID}"},
            }
        ],
        "breadcrumbs": {
            "values": [{"data": {"url": f"https://api.ccdexplorer.io/site-auth/{UUID}/x"}}]
        },
    }
    out = scrub_sentry_event(event, None)
    assert UUID not in repr(out)
    assert "hunter2" not in repr(out)


def test_an_event_without_those_parts_passes_through():
    assert scrub_sentry_event({"message": "hi"}, None) == {"message": "hi"}


def test_sampling_defaults_to_a_fraction(monkeypatch):
    monkeypatch.delenv("SENTRY_TRACES_SAMPLE_RATE", raising=False)
    assert 0 < sentry_traces_sample_rate() <= 0.25


def test_sampling_can_be_set_from_the_environment(monkeypatch):
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.5")
    assert sentry_traces_sample_rate() == 0.5


@pytest.mark.parametrize("bad", ["", "lots", "-1", "7"])
def test_a_nonsense_rate_falls_back_rather_than_crashing_startup(monkeypatch, bad):
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", bad)
    assert 0 <= sentry_traces_sample_rate() <= 1


# --- the api is wired to it ------------------------------------------------


def _init_call(path):
    import ast
    from pathlib import Path

    tree = ast.parse(Path(path).read_text())
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "init"
            and getattr(node.func.value, "id", "") == "sentry_sdk"
        ):
            return {kw.arg: kw.value for kw in node.keywords}
    raise AssertionError(f"no sentry_sdk.init in {path}")


def test_the_api_traces_a_fraction_and_scrubs_what_it_sends():
    import ast

    kw = _init_call("bases/ccdexplorer/ccdexplorer_api/app/factory.py")
    assert isinstance(kw["traces_sample_rate"], ast.Call)  # from the env, not a literal 1.0
    assert getattr(kw["before_send"], "id", "") == "scrub_sentry_event"
    assert getattr(kw["before_send_transaction"], "id", "") == "scrub_sentry_event"
    assert "_experiments" not in kw  # no continuous profiling
