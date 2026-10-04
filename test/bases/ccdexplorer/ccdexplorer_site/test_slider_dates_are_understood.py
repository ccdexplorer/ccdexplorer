"""The slider says "Jun 2023", and the handler has to understand that.

Its tooltips are formatted for a reader -- month and year, because it steps
by a month -- and that is what the page posts back. Fed to
date.fromisoformat it raises, the range is dropped, and the chart redraws at
the default: dragging the slider appears to do nothing at all.

Months also happen to be exactly the precision the urls carry, so a dragged
range and a typed one mean the same thing.
"""

import datetime as dt

import pytest

from ccdexplorer.ccdexplorer_site.app.routers.charts.images import parse_slider_range


def test_the_sliders_own_format_is_understood():
    start, end = parse_slider_range("Jun 2023", "Apr 2026")
    assert start == dt.date(2023, 6, 1)
    assert end == dt.date(2026, 4, 30)


def test_a_to_month_covers_the_whole_month():
    _start, end = parse_slider_range("Jan 2026", "Feb 2026")
    assert end == dt.date(2026, 2, 28)


def test_iso_dates_still_work():
    """What the tests and any direct caller send."""
    start, end = parse_slider_range("2026-01-01", "2026-03-31")
    assert start == dt.date(2026, 1, 1)
    assert end == dt.date(2026, 3, 31)


def test_a_single_month_is_a_valid_range():
    start, end = parse_slider_range("Mar 2026", "Mar 2026")
    assert start == dt.date(2026, 3, 1)
    assert end == dt.date(2026, 3, 31)


@pytest.mark.parametrize("bad", ["", "nonsense", "Smurf 2026"])
def test_nonsense_yields_nothing_rather_than_a_wrong_range(bad):
    assert parse_slider_range(bad, "Apr 2026") is None
    assert parse_slider_range("Jun 2023", bad) is None


def test_a_backwards_range_yields_nothing():
    assert parse_slider_range("Apr 2026", "Jun 2023") is None
