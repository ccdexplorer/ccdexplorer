"""Traces in the path: /charts/daily-limits/weekly/202306/202604/top100-top250

A hyphen separates them, so no trace name may contain one. The raw mongo
field is not usable either -- two of them have spaces in, and
`amount_to_make_top_100` is not something to put in an address -- so each
series carries a short name for the url.

Selecting every trace is the default, and the default is not spelled out: the
segment is absent when nothing has been deselected.
"""

from ccdexplorer.charts import ChartState, Grouping
from ccdexplorer.charts.paths import chart_path, parse_traces, state_from_path
from ccdexplorer.charts.registry import ALL_SPECS, BY_NAME

LIMITS = BY_NAME["daily_limits"]
TXS = BY_NAME["transactions_count"]


def test_every_series_has_a_url_name():
    for spec in ALL_SPECS:
        for series in spec.series:
            assert series.url_name, f"{spec.name}.{series.key}"


def test_no_url_name_contains_the_delimiter():
    """It would split into two traces that do not exist."""
    for spec in ALL_SPECS:
        for series in spec.series:
            assert "-" not in series.url_name, f"{spec.name}.{series.url_name}"


def test_url_names_are_url_safe():
    for spec in ALL_SPECS:
        for series in spec.series:
            assert series.url_name.isalnum(), f"{spec.name}.{series.url_name}"
            assert series.url_name.islower() or series.url_name.isdigit(), series.url_name


def test_url_names_are_unique_within_a_chart():
    """Two traces sharing a name makes one of them unreachable."""
    for spec in ALL_SPECS:
        names = [s.url_name for s in spec.series]
        assert len(names) == len(set(names)), spec.name


def test_the_awkward_keys_got_readable_names():
    names = {s.key: s.url_name for s in TXS.series}
    assert names["smart ctr"] == "contracts"
    assert names["register data"] == "data"
    assert {s.url_name for s in LIMITS.series} == {"top100", "top250"}


def test_a_trace_segment_parses_to_keys():
    assert parse_traces(LIMITS, "top100-top250") == (
        "amount_to_make_top_100",
        "amount_to_make_top_250",
    )


def test_one_trace_needs_no_delimiter():
    assert parse_traces(LIMITS, "top100") == ("amount_to_make_top_100",)


def test_an_unknown_trace_name_is_refused():
    """The path names something the chart does not have, so it names nothing."""
    assert parse_traces(LIMITS, "top100-nonsense") is None
    assert parse_traces(LIMITS, "") is None


def test_a_repeated_trace_is_refused():
    assert parse_traces(LIMITS, "top100-top100") is None


def test_the_path_omits_the_segment_when_everything_is_selected():
    """All of them is the default, and a default does not need spelling out."""
    state = state_from_path(LIMITS, "weekly", "202306", "202604")
    assert chart_path(LIMITS, state) == "/charts/daily-limits/weekly/202306/202604"


def test_the_path_carries_the_segment_when_a_trace_is_dropped():
    state = state_from_path(LIMITS, "weekly", "202306", "202604", "top100")
    assert state.traces == ("amount_to_make_top_100",)
    assert chart_path(LIMITS, state) == "/charts/daily-limits/weekly/202306/202604/top100"


def test_the_segment_keeps_the_charts_own_order():
    """Not the order they were typed, so one selection has one url."""
    a = state_from_path(TXS, "weekly", "202306", "202604", "data-account")
    b = state_from_path(TXS, "weekly", "202306", "202604", "account-data")
    assert chart_path(TXS, a) == chart_path(TXS, b)


def test_a_bad_trace_segment_names_no_state():
    assert state_from_path(LIMITS, "weekly", "202306", "202604", "nonsense") is None


def test_a_state_from_a_window_still_omits_the_default_traces():
    state = ChartState.from_query(LIMITS, {"window": "90d"})
    path = chart_path(LIMITS, state, net="mainnet")
    assert path.count("/") == 6
    # Daily rather than weekly: daily_limits is two closing values, so it
    # picks its resolution from the span, and ninety days is dense enough
    # to draw day by day.
    assert Grouping.DAILY.value in path
