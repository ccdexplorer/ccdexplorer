"""The admin chat is told that keys changed, not what they are.

The notification printed the keys themselves, and the documents they head --
which repeat the key as _id -- into Telegram, where it stays for good.
"""

from ccdexplorer.ccdexplorer_api.app.state_getters import get_dict_diff, mask_key

KEY_A = "528e6511-d55a-49d3-a4f1-fcce5eef03cc"
KEY_B = "b1f2c3d4-0000-4111-8222-333344445555"


def test_an_added_key_is_shown_by_its_prefix_only():
    out = get_dict_diff({}, {KEY_A: {"_id": KEY_A, "api_group": "free"}})
    assert "528e6511" in out
    assert KEY_A not in out


def test_a_removed_key_is_masked_too():
    out = get_dict_diff({KEY_B: {"_id": KEY_B}}, {})
    assert "Removed keys" in out
    assert KEY_B not in out


def test_a_changed_key_names_the_fields_and_never_the_documents():
    old = {KEY_A: {"_id": KEY_A, "api_group": "free"}}
    new = {KEY_A: {"_id": KEY_A, "api_group": "pro"}}
    out = get_dict_diff(old, new)
    assert "api_group" in out
    assert KEY_A not in out
    assert "free" not in out and "pro" not in out


def test_nothing_changed_says_so():
    d = {KEY_A: {"_id": KEY_A}}
    assert get_dict_diff(d, dict(d)) == "No differences"


def test_a_short_value_does_not_leak_whole():
    assert mask_key("abc") == "…"
    assert mask_key(KEY_A) == "528e6511…"
