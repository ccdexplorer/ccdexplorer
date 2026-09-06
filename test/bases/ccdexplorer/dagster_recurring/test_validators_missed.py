from types import SimpleNamespace

from ccdexplorer.dagster_recurring.recurring.update_validators_missed import (
    _counts_for_epoch,
    epochs_to_record,
)


def _filled(highest: int, lookback: int = 48) -> set[int]:
    """Every epoch in the lookback window already stored, so no gaps to sweep."""
    return set(range(max(1, highest - lookback + 1), highest + 1))


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
    assert epochs_to_record(100, _filled(100), payday_epoch=90, chain_epoch=104) == [101, 102, 103]


def test_epochs_to_record_resumes_from_storage_not_the_payday():
    """The bug this replaces: a payday-anchored lower bound skipped an epoch.

    A payday running epochs 100..123 rolls over to one starting at 124. With the
    old logic the lower bound jumped to 124 while the upper bound had only ever
    reached 122, so epoch 123 was never recorded by either window.
    """
    assert epochs_to_record(122, _filled(122), payday_epoch=124, chain_epoch=126) == [123, 124, 125]


def test_epochs_to_record_is_empty_when_caught_up():
    # Caught up means the highest stored epoch is the frontier itself.
    assert epochs_to_record(123, _filled(123), payday_epoch=100, chain_epoch=124) == []
    # And a store somehow ahead of the chain must not produce a backwards range.
    assert epochs_to_record(200, _filled(200), payday_epoch=100, chain_epoch=124) == []


def test_epochs_to_record_catches_up_after_downtime():
    assert epochs_to_record(50, _filled(50), payday_epoch=190, chain_epoch=200) == list(range(51, 200))


def test_epochs_to_record_falls_back_to_the_payday_on_an_empty_genesis():
    assert epochs_to_record(None, set(), payday_epoch=90, chain_epoch=104) == list(range(90, 104))


def test_epochs_to_record_falls_back_to_one_after_a_protocol_update():
    """The last known payday block still belongs to the previous genesis, so its
    epoch number is meaningless (and far ahead) in the new one."""
    assert epochs_to_record(None, set(), payday_epoch=4176, chain_epoch=6) == list(range(1, 6))


def test_epochs_to_record_is_contiguous_with_the_previous_run():
    """Successive runs must not leave a hole between them."""
    chain, highest, seen = 200, 100, []
    while chain <= 210:
        epochs = epochs_to_record(highest, _filled(highest), payday_epoch=100, chain_epoch=chain)
        seen.extend(epochs)
        if epochs:
            highest = epochs[-1]
        chain += 1
    assert seen == list(range(101, 210))


def test_epochs_to_record_sweeps_up_a_hole_below_the_highest():
    """Appending alone never revisits a hole; the bounded sweep has to."""
    stored = _filled(120) - {117}

    assert epochs_to_record(120, stored, payday_epoch=100, chain_epoch=123) == [117, 121, 122]


def test_epochs_to_record_sweeps_the_seam_a_concurrent_rebuild_left():
    """The real incident: a rebuild stopped at 4318 while the job resumed at
    4320, orphaning 4319. A run must now pick it up without being told."""
    stored = _filled(4321) - {4319}

    assert epochs_to_record(4321, stored, payday_epoch=4320, chain_epoch=4323) == [4319, 4322]


def test_epochs_to_record_sweep_is_bounded_by_the_lookback():
    """Holes older than the window are the rebuild script's job, not this one."""
    stored = _filled(200, lookback=48) - {160}
    stored.discard(100)  # far older than the 48-epoch window

    epochs = epochs_to_record(200, stored, payday_epoch=180, chain_epoch=202, lookback=48)

    assert 160 in epochs
    assert 100 not in epochs


def test_epochs_to_record_returns_oldest_first():
    """Gaps are recorded before new epochs, so a partial run stays sensible."""
    stored = _filled(120) - {110, 118}

    assert epochs_to_record(120, stored, payday_epoch=100, chain_epoch=124) == [
        110,
        118,
        121,
        122,
        123,
    ]


def test_epochs_to_record_sweep_does_not_duplicate_new_epochs():
    stored = _filled(120) - {119}

    epochs = epochs_to_record(120, stored, payday_epoch=100, chain_epoch=124)

    assert epochs == sorted(set(epochs))
