from types import SimpleNamespace

from ccdexplorer.dagster_recurring.recurring.update_validators_missed import (
    _counts_for_epoch,
)


def round_won_by(winner: int, present: bool):
    return SimpleNamespace(round=0, winner=winner, present=present)


def test_counts_for_epoch_keeps_missed_and_adds_denominator():
    winning_bakers = [
        round_won_by(11, True),
        round_won_by(11, True),
        round_won_by(11, False),
        round_won_by(22, True),
        round_won_by(33, False),
        round_won_by(33, False),
    ]

    counts = _counts_for_epoch(winning_bakers)

    # Unchanged meaning: rounds whose winner produced no finalized block.
    assert counts["missed_rounds_count"] == {"33": 2, "11": 1}
    # The denominator: every round won, missed or not.
    assert counts["rounds_won_count"] == {"11": 3, "33": 2, "22": 1}
    assert counts["rounds_total"] == 6
    assert counts["rounds_missed_total"] == 3


def test_counts_for_epoch_baked_is_won_minus_missed():
    """Blocks baked is derived, not stored, so it has to hold by construction."""
    winning_bakers = [
        round_won_by(11, True),
        round_won_by(11, False),
        round_won_by(22, True),
    ]

    counts = _counts_for_epoch(winning_bakers)

    baked = {
        baker: won - counts["missed_rounds_count"].get(baker, 0)
        for baker, won in counts["rounds_won_count"].items()
    }
    assert baked == {"11": 1, "22": 1}
    assert sum(baked.values()) == counts["rounds_total"] - counts["rounds_missed_total"]


def test_counts_for_epoch_perfect_epoch():
    winning_bakers = [round_won_by(11, True), round_won_by(22, True)]

    counts = _counts_for_epoch(winning_bakers)

    assert counts["missed_rounds_count"] == {}
    assert counts["rounds_missed_total"] == 0
    assert counts["rounds_won_count"] == {"11": 1, "22": 1}


def test_counts_for_epoch_empty_epoch():
    counts = _counts_for_epoch([])

    assert counts == {
        "missed_rounds_count": {},
        "rounds_won_count": {},
        "rounds_total": 0,
        "rounds_missed_total": 0,
    }


def test_counts_are_sorted_by_count_descending():
    """The existing missed dict is stored sorted; keep that for both dicts."""
    winning_bakers = (
        [round_won_by(11, False)]
        + [round_won_by(22, False) for _ in range(3)]
        + [round_won_by(33, False) for _ in range(2)]
    )

    counts = _counts_for_epoch(winning_bakers)

    assert list(counts["missed_rounds_count"]) == ["22", "33", "11"]
    assert list(counts["rounds_won_count"]) == ["22", "33", "11"]
