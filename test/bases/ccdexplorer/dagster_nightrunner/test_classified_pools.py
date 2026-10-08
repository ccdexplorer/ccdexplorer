"""The average delegators per pool, which used to read about twice what
it should.

Pure arithmetic over fields the asset has already stored, so these need no
database: the point is which people and which pools belong in the figure.
"""

from types import SimpleNamespace

import pytest

from ccdexplorer.dagster_nightrunner.nightrunner.update_classified_pools import (
    average_delegators_per_pool,
)


def test_passive_delegators_are_not_spread_across_pools():
    """They are in no pool. Counting them per pool counts people who are
    not there: on 2025-03-18, 900 of 1718 delegators were passive."""
    assert average_delegators_per_pool(1718, 900, 83, 1) == pytest.approx(818 / 84)


def test_a_pool_closed_to_new_delegators_still_holds_its_own():
    """closedForNew is closed to new delegators, not emptied of existing
    ones, so it belongs in the denominator."""
    assert average_delegators_per_pool(110, 10, 5, 5) == 10
    assert average_delegators_per_pool(110, 10, 10, 0) == 10


def test_without_the_passive_count_there_is_no_figure():
    """The old answer was the bug; repeating it where the passive count is
    missing would keep publishing it."""
    assert average_delegators_per_pool(1718, None, 83, 1) is None


def test_no_pools_is_not_a_division():
    assert average_delegators_per_pool(0, 0, 0, 0) is None


# --- the day the average is unknowable -------------------------------------


def _one_pool_with_two_delegators():
    import pandas as pd

    return pd.DataFrame(
        {
            "pool_status": ["openForAll", None, None],
            "delegation_target": [None, "1", "1"],
            "staked_amount": [1000.0, 10.0, 20.0],
        }
    )


def _run_without_grpc(monkeypatch):
    """The asset over one day, with no node to ask for the passive count."""
    from ccdexplorer.dagster_nightrunner.nightrunner import update_classified_pools as m

    monkeypatch.setattr(m, "get_df_from_git", lambda _: _one_pool_with_two_delegators())
    written = []
    monkeypatch.setattr(m, "write_queue_to_collection", lambda *a: written.append(a))
    logged = []
    context = SimpleNamespace(
        log=SimpleNamespace(info=logged.append, error=logged.append),
    )

    result = m.perform_data_for_classified_pools(
        context, "2024-02-12", {"2024-02-12": "deadbeef"}, mongodb=None
    )
    return result, written, logged


def test_the_day_without_a_passive_count_still_records_the_rest(monkeypatch):
    """A gap in one field is not a reason to lose the pool counts -- and the
    log line used to format the average with :.2f, so a None crashed the
    partition outright."""
    result, written, _ = _run_without_grpc(monkeypatch)

    assert "avg_per_pool: unknown" in result["message"]
    assert written, "the day's document was not queued"


def test_an_unknown_average_is_stored_as_unknown_not_as_the_old_answer(monkeypatch):
    """Writing a figure computed the old way would republish the bug."""
    _, written, _ = _run_without_grpc(monkeypatch)

    doc = written[0][1][0]._doc
    assert doc["delegator_avg_count_per_pool"] is None
    assert doc["delegator_count"] == 2
    assert doc["open_pool_count"] == 1
    assert "passive_delegator_count" not in doc
