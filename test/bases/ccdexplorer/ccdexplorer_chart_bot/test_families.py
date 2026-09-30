"""A group says what a chart is about; a period says it is one of a series.

Eighteen charts now share the group `other`. siblings() used to key off group,
which was safe while `kraken` was the only one -- with `other` it would hand
each of those eighteen seventeen siblings, and both keyboard_for and
photo_result attach a keyboard whenever there is more than one.
"""

from ccdexplorer.ccdexplorer_chart_bot.catalogue import (
    BY_NAME,
    CHARTS,
    family_default,
    siblings,
)

GROUPS = {"price", "txs", "agents", "tvl", "other"}


def _families() -> set[str]:
    """Groups that are families, read off the catalogue rather than hardcoded.

    New families arrive in later tasks; this stays true throughout.
    """
    return {c.group for c in CHARTS if c.period}


def test_every_chart_has_one_of_the_five_groups():
    assert {c.group for c in CHARTS} <= GROUPS
    assert all(c.group for c in CHARTS), "a chart with no group is invisible to the menu"


def test_kraken_is_now_the_price_family():
    assert BY_NAME["ccd_kraken_4h"].group == "price"
    assert len(siblings(BY_NAME["ccd_kraken_4h"])) == 7


def test_a_chart_in_other_has_no_siblings():
    """The regression that would give eighteen charts seventeen buttons each."""
    chart = BY_NAME["accounts_per_day"]

    assert chart.group == "other"
    assert siblings(chart) == []


def test_only_family_members_carry_a_period():
    for chart in CHARTS:
        if chart.group == "other":
            assert not chart.period, f"{chart.name} is in other but has a period"
        else:
            assert chart.period, f"{chart.name} is in a family but has no period"


def test_each_family_has_exactly_one_default():
    for group in _families():
        members = [c for c in CHARTS if c.group == group and c.default]
        assert len(members) == 1, f"{group} has {len(members)} defaults"


def test_nothing_in_other_is_a_default():
    assert not any(c.default for c in CHARTS if c.group == "other")


def test_the_price_default_is_the_four_hour_chart():
    assert family_default("price").name == "ccd_kraken_4h"


def test_family_default_of_other_is_none():
    assert family_default("other") is None


def test_family_default_of_an_unknown_group_is_none():
    assert family_default("nonsense") is None
