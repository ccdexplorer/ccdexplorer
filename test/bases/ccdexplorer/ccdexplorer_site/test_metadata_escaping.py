"""Token metadata must not reach the browser as markup.

The tabulator tables declare their columns `formatter: "html"`, so whatever
these functions build is inserted as markup rather than text. The values going
in come from token metadata, which is written by whoever deployed the contract
-- permissionless, and already carrying markup: token descriptions in
production contain <p> tags today.
"""

from ccdexplorer.ccdexplorer_site.app.utils import (
    create_dict_for_tabulator_display_for_nft_tokens,
    create_dict_for_tabulator_display_for_unverified_token,
    h,
    token_display_name,
)

PAYLOAD = "<img src=x onerror=alert(document.domain)>"


def test_h_escapes_a_tag():
    assert h(PAYLOAD) == "&lt;img src=x onerror=alert(document.domain)&gt;"


def test_h_escapes_an_attribute_break_out():
    assert '"' not in h('" onmouseover="alert(1)')


def test_h_renders_none_as_empty_rather_than_the_word_none():
    assert h(None) == ""


def test_token_display_name_still_returns_the_raw_value():
    """It feeds CSV export too, so escaping belongs at the HTML boundary."""
    assert token_display_name({"token_metadata": {"name": PAYLOAD}}, "tok") == PAYLOAD


def test_a_malicious_token_name_is_escaped_in_the_nft_table():
    row = {
        "token_metadata": {"name": PAYLOAD},
        "token_id": "tok1",
        "contract": "<7260,0>",
        "last_height_processed": 1,
    }

    cell = create_dict_for_tabulator_display_for_nft_tokens("mainnet", None, None, {}, row)[
        "token_id"
    ]

    assert "<img" not in cell
    assert "&lt;img" in cell
    # The markup this function builds itself is untouched.
    assert cell.startswith('<a href="/mainnet/token/')


def test_the_download_column_keeps_the_raw_name():
    """It is exported as CSV, where an escaped ampersand is corruption."""
    row = {
        "token_metadata": {"name": "Tom & Jerry"},
        "token_id": "tok1",
        "contract": "<7260,0>",
        "last_height_processed": 1,
    }

    cells = create_dict_for_tabulator_display_for_nft_tokens("mainnet", None, None, {}, row)

    assert cells["token_id_download"] == "Tom & Jerry"


def test_a_malicious_name_is_escaped_in_the_unverified_token_table():
    row = {
        "address_information": {
            "_id": "<7260,0>-tok1",
            "token_id": "tok1",
            "token_metadata": {"name": PAYLOAD},
        },
        "token_address": "<7260,0>-tok1",
        "token_id": "tok1",
        "contract": "<7260,0>",
        "token_amount": "1",
    }

    cells = create_dict_for_tabulator_display_for_unverified_token("mainnet", row)

    assert "<img" not in cells["token"]
    assert "&lt;img" in cells["token"]
    # The contract cell is built from a value that also comes off-chain.
    assert "&lt;7260,0&gt;" in cells["issuer"]
