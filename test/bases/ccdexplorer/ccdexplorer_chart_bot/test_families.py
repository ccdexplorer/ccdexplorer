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

GROUPS = {"price", "txs", "plt", "agents", "other"}


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
    chart = BY_NAME["accounts_growth"]

    assert chart.group == "other"
    assert siblings(chart) == []


def test_only_interval_family_members_carry_a_period():
    """Three shapes now. `other` is standalone charts; a spec-backed chart
    carries its windows on its spec, not as a period; and what is left is the
    interval families, where the period is the button's label."""
    for chart in CHARTS:
        if chart.group == "other" or chart.spec_name:
            assert not chart.period, f"{chart.name} should carry no period"
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


# --- resolving a list of matches to what should actually be sent ------------
#
# Review finding 1 and 2, which are the same bug at two call sites. A query
# matching several members of one family must collapse to that family once --
# not send its default repeatedly, and not vanish.


def test_a_family_collapses_to_one_entry():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families

    price = [c for c in CHARTS if c.group == "price"]

    assert [c.name for c in resolve_families(price)] == ["ccd_kraken_4h"]


def test_a_non_default_member_resolves_instead_of_disappearing():
    """`1d`, `90d`, `minute` and `hourly` match only non-default members."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families, search

    # 90d and 365d used to be chart names; they are window_words now, and
    # must still reach the chart whose button they name.
    for word in ("1d", "minute", "hourly", "90d", "365d"):
        assert resolve_families(search(word)), f"{word!r} resolved to nothing"


def test_charts_outside_a_family_are_kept_as_themselves():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families

    others = [c for c in CHARTS if c.group == "other"]

    assert resolve_families(others) == others


def test_order_is_preserved():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families

    resolved = resolve_families(list(CHARTS))
    order = [c.name for c in CHARTS]

    assert [order.index(c.name) for c in resolved] == sorted(order.index(c.name) for c in resolved)


def test_the_whole_catalogue_resolves_to_one_per_family_plus_other():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import resolve_families

    # A group is a family only when its members carry a period -- `chain`,
    # `plt` and `agents` hold one chart each and collapse to themselves, the
    # same as everything in `other`. Counting spec-backed charts separately
    # double-counted nearly the whole catalogue once most of them had a spec.
    families = {c.group for c in CHARTS if c.period}
    standalone = [c for c in CHARTS if c.group not in families]

    assert len(resolve_families(list(CHARTS))) == len(families) + len(standalone)


# --- a claimed word answers with one chart ---------------------------------
#
# `claims` exists so that one family can "claim the word outright rather than
# win it by accident of naming" -- its own words. Ranking put the claimant
# first but still returned the accidents, so /c price sent the Kraken chart,
# realized prices and fee stabilization, three charts for an unambiguous word.


def test_a_claimed_word_returns_only_the_claimant():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import search

    names = [c.name for c in search("price")]

    assert "realized_prices" not in names
    assert "fee_stabilization" not in names
    assert names[0] == "ccd_kraken_4h"


def test_a_claimed_word_still_offers_the_whole_family():
    """Claiming narrows to the family, not to a single chart -- the picker and
    the interval buttons both still need its members."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import search

    assert {c.group for c in search("price")} == {"price"}


def test_an_unclaimed_word_is_unaffected():
    """ "staking" is claimed by nobody and must still return everything it matches."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import search

    assert len(search("staking")) > 1


def test_a_word_only_some_of_which_is_claimed_does_not_narrow():
    """ "realized price" is two words; the price family claims "price" but not
    "realized", so the claim must not swallow the more specific query."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import search

    assert search("realized price")[0].name == "realized_prices"


def test_tvl_claims_its_word_too():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import search

    assert {c.group for c in search("tvl")} == {"plt"}


def test_no_chart_carries_a_word_another_family_claims():
    """A claim is exclusive now, so a keyword another family claims is a
    keyword that can never reach its own chart."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS

    claimed: dict[str, set[str]] = {}
    for chart in CHARTS:
        for word in chart.claims:
            claimed.setdefault(word, set()).add(chart.group)

    stranded = [
        f"{c.name} carries {w!r}, claimed by {sorted(claimed[w])}"
        for c in CHARTS
        for w in c.keywords
        if w in claimed and c.group not in claimed[w]
    ]
    assert not stranded, "unreachable keywords:\n  " + "\n  ".join(stranded)


# --- a chart's description must match the bars it draws ---------------------
#
# The image routes group by week beyond thirty days, but the catalogue
# descriptions still said "per day" for every window, so the 90d, 180d and 365d
# charts described themselves wrongly in the picker and in every caption.


def test_no_description_claims_a_period_the_chart_does_not_draw():
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS

    wrong = []
    for chart in CHARTS:
        if not chart.period or not chart.period.endswith("d"):
            continue
        days = int(chart.period[:-1])
        text = chart.description.lower()
        if days <= 30 and "per week" in text:
            wrong.append(f"{chart.name}: daily bars described as weekly")
        if days > 30 and "per day" in text:
            wrong.append(f"{chart.name}: weekly bars described as daily")

    assert not wrong, "\n  " + "\n  ".join(wrong)


def test_a_spec_backed_chart_claims_no_fixed_period():
    """These used to be four charts whose descriptions each named the bar
    width their window implied -- "per day" for 30d, "per week" for 365d.

    One chart with a grouping button cannot say that: the bar width is
    whatever the reader last tapped, and a description insisting otherwise
    would be wrong three times out of four. The figure's own title carries it
    instead, which is the only place it can stay true."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS

    for chart in CHARTS:
        if not chart.spec_name:
            continue
        text = chart.description.lower()
        assert "per day" not in text, chart.name
        assert "per week" not in text, chart.name
        assert "per month" not in text, chart.name


def test_the_description_agrees_with_the_site_that_draws_it():
    """The bot's words and the site's grouping come from the same threshold."""
    from ccdexplorer.ccdexplorer_chart_bot.catalogue import CHARTS
    from ccdexplorer.ccdexplorer_site.app.routers.statistics import image_period

    for chart in CHARTS:
        if not chart.period or not chart.period.endswith("d"):
            continue
        expected = image_period(int(chart.period[:-1])).lower()
        other = "week" if expected == "day" else "day"
        assert f"per {other}" not in chart.description.lower(), (
            f"{chart.name} says per {other}, site draws per {expected}"
        )
