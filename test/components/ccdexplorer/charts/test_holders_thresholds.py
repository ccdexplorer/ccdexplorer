"""Holders draws every threshold worth drawing, and is on the gallery.

It went into the chart migration as a configurable page whose dropdown
offered eleven balance thresholds and came out as one unlisted line at one
million. The data never changed -- the nightly job writes a count at
thirteen thresholds and always has -- so the chart was the only thing lost,
and the reason given for unlisting it was that the accounts category already
covered the distribution. No chart in that category does.
"""

import re
from pathlib import Path

import pytest

from ccdexplorer.charts import Agg
from ccdexplorer.charts.registry import BY_NAME

ROOT = Path(__file__).resolve().parents[4]
PRODUCER = (
    ROOT
    / "bases"
    / "ccdexplorer"
    / "dagster_nightrunner"
    / "nightrunner"
    / "update_daily_holders.py"
)

#: The thresholds the chart draws, smallest first. Not all thirteen the job
#: writes: 0 and 100 are "every account that exists", and thirteen lines on
#: one axis is a colour key rather than a chart.
DRAWN = (1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000)


def _spec():
    return BY_NAME["holders"]


def _produced_thresholds():
    """The limits the nightly job actually writes a count for.

    Read out of the producer's source rather than imported: importing the
    dagster brick opens a Mongo connection, and this only needs the list of
    numbers in the loop that builds the documents.
    """
    source = PRODUCER.read_text()
    block = re.search(r"for limit in \[(.*?)\]", source, re.DOTALL)
    assert block, "the producer no longer has a literal list of limits"
    return {int(n.replace("_", "")) for n in re.findall(r"[\d_]+", block.group(1))}


# --- the thresholds it draws ----------------------------------------------


def test_it_draws_more_than_one_threshold():
    """The whole complaint: five thresholds became one."""
    assert len(_spec().series) > 1


def test_it_draws_the_thresholds_it_means_to():
    assert [s.key for s in _spec().series] == [f"count_>={t}" for t in DRAWN]


@pytest.mark.parametrize("threshold", DRAWN)
def test_every_drawn_threshold_is_one_the_job_writes(threshold):
    """A series naming a field nothing produces draws a flat line at zero,
    which looks like an answer."""
    assert threshold in _produced_thresholds()


def test_the_producer_still_writes_all_of_them():
    """Guards the other direction: if a threshold is dropped from the nightly
    job, the chart asking for it should fail here rather than in a render."""
    assert set(DRAWN) <= _produced_thresholds()


def test_the_series_read_smallest_threshold_first():
    """Each threshold is a subset of the one below it, so ascending order
    puts the largest count at the top of the legend and the smallest at the
    bottom -- the same order as the lines themselves."""
    thresholds = [int(s.key.removeprefix("count_>=")) for s in _spec().series]

    assert thresholds == sorted(thresholds)


# --- how each one is labelled and aggregated ------------------------------


def test_every_series_is_a_level():
    """A count of accounts over a threshold is a snapshot, never a tally.
    Summing a week of them would multiply the answer by seven."""
    for series in _spec().series:
        assert series.agg is Agg.LAST, series.key


def test_the_labels_name_their_threshold():
    labels = [s.label for s in _spec().series]

    assert labels == ["> 1M CCD", "> 5M CCD", "> 10M CCD", "> 50M CCD", "> 100M CCD"]


def test_the_url_names_are_unique():
    """They go in the query string; two series sharing one cannot be
    toggled independently."""
    names = [s.url_name for s in _spec().series]

    assert len(names) == len(set(names)), names


def test_the_url_names_are_safe_in_a_query_string():
    for series in _spec().series:
        assert re.fullmatch(r"[a-z0-9]+", series.url_name), series.url_name


def test_the_one_million_series_keeps_the_name_its_links_used():
    """It was the only series, so any saved link selecting a series selects
    this one by name."""
    assert _spec().series[0].url_name == "above1m"


def test_the_colours_are_all_different():
    """Five lines of one colour is one line."""
    colours = [s.colour for s in _spec().series]

    assert len(colours) == len(set(colours)), colours


# --- back on the gallery ---------------------------------------------------


def test_it_is_listed():
    """listed=False is what made it invisible: the gallery filters on it, so
    the chart had a page and an image and no way to reach either."""
    assert _spec().listed


def test_it_still_has_its_page_and_image():
    assert _spec().has_page
    assert _spec().has_image


def test_it_is_still_an_accounts_chart():
    assert _spec().category == "accounts"


def test_the_docs_describe_more_than_the_one_threshold():
    """The page links to these docs, which described a single-threshold
    chart because that is what it was."""
    docs = (ROOT / "docs" / "charts" / "holders.md").read_text()

    for label in ("5M", "10M", "50M", "100M"):
        assert label in docs or f"count_>={label}" in docs, label
