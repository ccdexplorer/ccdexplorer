"""Credentialed CORS must not be offered over plaintext http.

Both apps allowlisted http://api.ccdexplorer.io and its dev twin alongside the
https forms, with allow_credentials=True. The allowlist itself was always
explicit -- no wildcard -- so this is about the scheme, not the hosts.
"""

import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[4] / "bases" / "ccdexplorer"
FACTORIES = {
    "site": ROOT / "ccdexplorer_site" / "app" / "factory.py",
    "api": ROOT / "ccdexplorer_api" / "app" / "factory.py",
}


@pytest.mark.parametrize("name", sorted(FACTORIES))
def test_no_plaintext_production_origin_is_allowed(name):
    source = FACTORIES[name].read_text()

    assert '"http://api.ccdexplorer.io"' not in source
    assert '"http://dev-api.ccdexplorer.io"' not in source


@pytest.mark.parametrize("name", sorted(FACTORIES))
def test_the_https_origins_are_still_there(name):
    """Removing the scheme, not the hosts."""
    source = FACTORIES[name].read_text()

    assert '"https://api.ccdexplorer.io"' in source


@pytest.mark.parametrize("name", sorted(FACTORIES))
def test_credentials_are_never_paired_with_a_wildcard(name):
    source = FACTORIES[name].read_text()

    assert 'allow_origins=["*"]' not in source
    assert 'allow_origins="*"' not in source
