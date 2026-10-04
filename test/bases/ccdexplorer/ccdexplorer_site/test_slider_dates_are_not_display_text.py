"""A slider date the server cannot read is a bad request, not a crash.

Production was sent `{"start_date": "Juin 2021", "end_date": "Septembre
2026"}` and answered with an unhandled ParserError -- 500, twice, from a
French Chrome (CCDEXPLORER-IO-2Q9). The cause was on the client: the slider
wrote "Jun 2021" into a hidden span, Chrome's page translation rewrote the
text node, and hx-vals read the translated text straight back out and
posted it.

The client sends a machine-readable value now, which is the fix. These
handlers are the other half: nothing stops a bot, a stale cached page or a
hand-rolled request sending anything at all, and a string a caller got
wrong is a bad request rather than a server error.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

PROJECT = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"

#: The endpoint, and a body it accepts with the dates left to each test.
#: Every one of these parses its slider dates before it touches mongo.
_GROUPED = {
    "theme": "dark",
    "group_by_selection": "daily",
    "trace_selection": "",
    "filename": "x.csv",
}

ENDPOINTS = [
    (
        "/mainnet/ajax_statistics_standalone/statistics_holders",
        {"theme": "dark", "amount_chosen": "500K", "filename": "x.csv"},
    ),
    ("/mainnet/ajax_statistics_standalone/statistics_active_addresses", _GROUPED),
    ("/mainnet/ajax_statistics_standalone/agent_registries", _GROUPED),
    (
        "/mainnet/ajax_statistics_standalone/plt_transfers",
        {**_GROUPED, "dropdown_values_fancy": "EUR"},
    ),
    (
        "/mainnet/ajax_statistics_standalone/statistics_network_summary_accounts_per_day",
        _GROUPED,
    ),
    ("/mainnet/ajax_transaction_types_reporting", _GROUPED),
]

#: What a translated page, a non-English datepicker or a bot actually sent.
UNREADABLE = ["Juin 2021", "Septembre 2026", "", "not a date", "../../etc/passwd"]


@pytest.fixture(scope="module")
def client():
    app = create_app(
        AppSettings(
            static_dir=PROJECT / "static",
            templates_dir=PROJECT / "templates",
            node_modules_dir=PROJECT / "node_modules",
            addresses_dir=PROJECT / "addresses",
        )
    )
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("value", UNREADABLE)
@pytest.mark.parametrize("path,body", ENDPOINTS, ids=[e[0].split("/")[-1] for e in ENDPOINTS])
def test_a_date_the_server_cannot_read_is_a_bad_request(client, path, body, value):
    response = client.post(path, json={**body, "start_date": value, "end_date": value})
    assert response.status_code != 500, response.text
    assert response.status_code == 422


@pytest.mark.parametrize("path,body", ENDPOINTS, ids=[e[0].split("/")[-1] for e in ENDPOINTS])
def test_a_date_it_can_read_is_not_refused(client, path, body):
    """The guard must not swallow the dates the slider really sends."""
    response = client.post(
        path, json={**body, "start_date": "2021-06-01", "end_date": "2026-09-30"}
    )
    assert response.status_code != 422, response.text
