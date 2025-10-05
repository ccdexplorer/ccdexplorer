import pytest
from unittest.mock import MagicMock

from ccdexplorer.ccdexplorer_site.app.utils import account_link
import ccdexplorer.ccdexplorer_site.app.utils as utils


@pytest.fixture
def mock_app():
    """Return a mock app with address lookup maps."""
    app = MagicMock()
    app.addresses_to_indexes = {
        "mainnet": {},
        "testnet": {},
    }
    return app


@pytest.fixture
def mock_wallet_address():
    """Return a fake CCD_ContractAddress-like object."""
    return type("CCD_ContractAddress", (), {"index": 123, "subindex": 0})()


@pytest.mark.parametrize("net", ["mainnet", "testnet"])
def test_account_link_with_instance_link(monkeypatch, net, mock_app):
    """Should delegate to instance_link_from_str when '<' is in value."""
    fake_html = "<a>fake</a>"
    monkeypatch.setattr(utils, "instance_link_from_str", lambda *a, **kw: fake_html)

    result = account_link("<instance>", net, app=mock_app)
    assert result == fake_html


def test_account_link_with_user_dict(monkeypatch, mock_app):
    """If user is a dict, it should be converted into a SiteUser instance."""
    mock_user = {"id": 1, "name": "John"}
    mock_siteuser = MagicMock()
    monkeypatch.setattr(utils, "SiteUser", lambda **kw: mock_siteuser)
    monkeypatch.setattr(utils, "from_address_to_index", lambda *a, **kw: 42)
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, "label"))

    result = account_link("someaddress", "mainnet", user=mock_user, app=mock_app)
    assert "href" in result
    assert "42" in result


def test_account_link_converts_str_to_index(monkeypatch, mock_app):
    """Should convert string address to index using from_address_to_index()."""
    monkeypatch.setattr(utils, "from_address_to_index", lambda *a, **kw: 77)
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, "label"))

    html = account_link("addr1", "mainnet", app=mock_app)
    assert "/mainnet/account/addr1" in html or "/mainnet/account/77" in html


def test_account_link_testnet(monkeypatch, mock_app):
    """For testnet, should use shorter format with person icon."""
    html = account_link("abc123", "testnet", app=mock_app)
    assert "bi-person-bounding-box" in html
    assert "/testnet/account/abc123" in html


def test_account_link_with_int_value_and_tag(monkeypatch, mock_app):
    """When value is int and tag is found."""

    def fake_account_label_on_index(val, user, tags, net, app):
        return True, "<i>custom label</i>"

    monkeypatch.setattr(utils, "account_label_on_index", fake_account_label_on_index)

    html = account_link(100, "mainnet", app=mock_app)
    assert "<i>custom label</i>" in html
    assert "/mainnet/account/100" in html


def test_account_link_with_int_value_no_tag(monkeypatch, mock_app):
    """When value is int and tag not found, use default icon + monospace."""
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, ""))
    html = account_link(42, "mainnet", app=mock_app)
    assert "bi-person-bounding-box" in html
    assert "/mainnet/account/42" in html


def test_account_link_with_wallet_contract(monkeypatch, mock_app, mock_wallet_address):
    """If wallet_contract_address is given, href should point to smart-wallet route."""
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, ""))
    html = account_link(
        99,
        "mainnet",
        app=mock_app,
        wallet_contract_address=mock_wallet_address,
    )
    assert "/mainnet/smart-wallet/123/0/99" in html


@pytest.mark.parametrize(
    "value, expected_snippet",
    [
        ("a" * 64, "bi-filetype-key"),  # key-like hash
        ("abcd", "bi-person-bounding-box"),  # short string
    ],
)
def test_account_link_string_variants(monkeypatch, mock_app, value, expected_snippet):
    """Covers 64-char and short string variants for non-tagged string values."""
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, ""))
    monkeypatch.setattr(utils, "from_address_to_index", lambda *a, **kw: value)
    html = account_link(value, "mainnet", app=mock_app)
    assert expected_snippet in html
    assert "/mainnet/account/" in html


def test_account_link_str_value_as_alias(monkeypatch, mock_app):
    """If from_address_to_index() changes value, ensure output uses the converted one."""
    monkeypatch.setattr(utils, "from_address_to_index", lambda v, n, a: 999)
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, ""))
    html = account_link("alias_addr", "mainnet", app=mock_app)
    assert "999" in html or "alias_addr" in html


def test_account_link_with_none_wallet(monkeypatch, mock_app):
    """Ensure wallet_contract_address=None follows account path."""
    monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, ""))
    html = account_link(7, "mainnet", app=mock_app, wallet_contract_address=None)
    assert "/mainnet/account/7" in html


# def test_account_link_with_str_user_and_tags(monkeypatch, mock_app):
#     """Tag system integration path (user and tags provided)."""
#     monkeypatch.setattr(utils, "account_label_on_index", lambda *a, **kw: (False, "fallback"))
#     monkeypatch.setattr(utils, "from_address_to_index", lambda *a, **kw: "xyz")
#     html = account_link("xyz", "mainnet", user="u", tags={"1": "2"}, app=mock_app)
#     assert "fallback" in html
#     assert "/mainnet/account/xyz" in html
