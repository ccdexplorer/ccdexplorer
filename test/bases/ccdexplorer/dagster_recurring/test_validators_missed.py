from types import SimpleNamespace

from ccdexplorer.dagster_recurring.recurring.update_validators_missed import (
    _counts_for_epoch,
    epochs_to_record,
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


def test_epochs_to_record_stops_one_short_of_the_chain():
    """The current epoch is in progress; GetWinningBakersEpoch rejects it."""
    assert epochs_to_record(highest_stored=100, payday_epoch=90, chain_epoch=104) == range(101, 104)


def test_epochs_to_record_resumes_from_storage_not_the_payday():
    """The bug this replaces: a payday-anchored lower bound skipped an epoch.

    A payday running epochs 100..123 rolls over to one starting at 124. With the
    old logic the lower bound jumped to 124 while the upper bound had only ever
    reached 122, so epoch 123 was never recorded by either window.
    """
    assert epochs_to_record(highest_stored=122, payday_epoch=124, chain_epoch=126) == range(
        123, 126
    )


def test_epochs_to_record_is_empty_when_caught_up():
    # Caught up means the highest stored epoch is the frontier itself.
    assert len(epochs_to_record(highest_stored=123, payday_epoch=100, chain_epoch=124)) == 0
    # And a store somehow ahead of the chain must not produce a backwards range.
    assert len(epochs_to_record(highest_stored=200, payday_epoch=100, chain_epoch=124)) == 0


def test_epochs_to_record_catches_up_after_downtime():
    assert epochs_to_record(highest_stored=50, payday_epoch=190, chain_epoch=200) == range(51, 200)


def test_epochs_to_record_falls_back_to_the_payday_on_an_empty_genesis():
    assert epochs_to_record(highest_stored=None, payday_epoch=90, chain_epoch=104) == range(90, 104)


def test_epochs_to_record_falls_back_to_one_after_a_protocol_update():
    """The last known payday block still belongs to the previous genesis, so its
    epoch number is meaningless (and far ahead) in the new one."""
    assert epochs_to_record(highest_stored=None, payday_epoch=4176, chain_epoch=6) == range(1, 6)


def test_epochs_to_record_is_contiguous_with_the_previous_run():
    """Successive runs must not leave a hole between them."""
    chain, highest, seen = 200, 100, []
    while chain <= 210:
        epochs = epochs_to_record(highest, payday_epoch=100, chain_epoch=chain)
        seen.extend(epochs)
        if epochs:
            highest = epochs[-1]
        chain += 1
    assert seen == list(range(101, 210))
