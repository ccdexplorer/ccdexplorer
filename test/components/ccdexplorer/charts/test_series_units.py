"""Some fields are not stored in the unit the chart shows.

fee_for_day is microCCD. The handwritten chart divided by a million before
drawing; the generic one did not, so a week of fees read as 227 billion
rather than 227 thousand. A number that large is obviously wrong, which is
the only lucky thing about it -- the same mistake on a smaller field would
just be a wrong chart.
"""

from ccdexplorer.charts.registry import ALL_SPECS, BY_NAME

MICRO = 1_000_000


def test_fees_are_shown_in_ccd_not_microccd():
    series = BY_NAME["transaction_fees"].series[0]
    assert series.scale == MICRO


def test_most_series_are_not_scaled():
    """A scale is the exception; the default must be no change at all."""
    unscaled = [s for spec in ALL_SPECS for s in spec.series if s.scale == 1]
    assert len(unscaled) > 20


def test_no_scale_is_zero():
    """Dividing by zero would be a blank chart with no explanation."""
    for spec in ALL_SPECS:
        for series in spec.series:
            assert series.scale > 0, f"{spec.name}.{series.key}"


def test_the_label_says_which_unit_it_is_in():
    """A scaled series that still said 'Fees' would be ambiguous."""
    assert "CCD" in BY_NAME["transaction_fees"].series[0].label
