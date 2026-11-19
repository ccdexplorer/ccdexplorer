"""The transactions filter body may name any of its filters, or none.

Sentry (API-CCDEXPLORER-IO-AR, 2026-11-16) caught a 500 from
POST /v2/{net}/transactions/{count}/{skip}: `KeyError: 'sponsored'`. The route
read `filters_dict["type"]` and `filters_dict["sponsored"]` straight out of
whatever filters the caller sent, so a body that mentioned only one of them --
or an empty list, which the request model allows -- raised before a single
query ran. A filter that was not given is a filter that is off.
"""

import pytest

from ccdexplorer.ccdexplorer_api.app.routers.v2.transactions_v2 import (
    FilterItem,
    filter_values,
)


def _item(field, value, type_="string"):
    return FilterItem(field=field, type=type_, value=value)


def test_no_filters_means_no_type_and_not_sponsored():
    assert filter_values([]) == ("no_tx", False)


def test_only_a_type_is_not_sponsored():
    assert filter_values([_item("type", "transfer")]) == ("transfer", False)


def test_only_sponsored_has_no_type():
    assert filter_values([_item("sponsored", True, "bool")]) == ("no_tx", True)


def test_both_are_read():
    assert filter_values([_item("type", "transfer"), _item("sponsored", True, "bool")]) == (
        "transfer",
        True,
    )


def test_an_empty_type_is_no_type():
    """The page sends type "" for 'all'; that is how it has always read."""
    assert filter_values([_item("type", "")]) == ("no_tx", False)


@pytest.mark.parametrize("value", [False, "true", "yes"])
def test_only_a_real_true_turns_sponsored_on(value):
    """`is True`, as before: the string "true" is not the boolean."""
    assert filter_values([_item("sponsored", value, "bool")])[1] is False
