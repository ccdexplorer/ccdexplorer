"""Carrying each account's cost basis from one day's snapshot to the next.

The join underneath this is a full outer one, and which accounts survive it
matters: an account in yesterday's state but absent from today's snapshot
still has a basis, and an account new today has none yet. Polars renamed
how="outer" to how="full" and, in the same move, stopped coalescing the
join key by default -- so the obvious rename is a data bug rather than a
rename.
"""

import polars as pl

from ccdexplorer.dagster_nightrunner.nightrunner.update_realized_prices import (
    merge_snapshot_with_state,
)


# Schemas are spelled out because an empty frame otherwise gets dtype Null
# for every column and the join refuses to match it against a str key --
# which is why perform_realized_prices declares its opening state too.
def _snapshot(**balances):
    return pl.DataFrame(
        {"account": list(balances), "balance": [float(v) for v in balances.values()]},
        schema={"account": pl.String, "balance": pl.Float64},
    ).with_columns(pl.lit(0.02).alias("fx"))


def _state(**rows):
    return pl.DataFrame(
        {
            "account": list(rows),
            "balance_prev": [float(b) for b, _ in rows.values()],
            "basis_prev": [float(c) for _, c in rows.values()],
        },
        schema={"account": pl.String, "balance_prev": pl.Float64, "basis_prev": pl.Float64},
    )


def test_an_account_in_both_keeps_its_name():
    merged = merge_snapshot_with_state(_snapshot(alice=100), _state(alice=(80, 0.01)))

    assert merged["account"].to_list() == ["alice"]
    assert merged["balance"].to_list() == [100.0]
    assert merged["balance_prev"].to_list() == [80.0]


def test_an_account_that_left_the_snapshot_is_not_nameless():
    """It is in the state and not in today's snapshot. Without coalescing,
    polars puts its name in account_right and leaves account null, so the
    row survives the join with no account at all."""
    merged = merge_snapshot_with_state(_snapshot(alice=100), _state(bob=(50, 0.03)))

    accounts = set(merged["account"].to_list())
    assert accounts == {"alice", "bob"}, accounts
    assert None not in accounts


def test_the_join_leaves_no_duplicate_key_column():
    merged = merge_snapshot_with_state(_snapshot(alice=100), _state(bob=(50, 0.03)))

    assert "account_right" not in merged.columns, merged.columns


def test_a_brand_new_account_starts_from_zero():
    """fill_null(0) is what makes the basis formula's balance_prev == 0
    branch fire for an account seen for the first time."""
    merged = merge_snapshot_with_state(_snapshot(alice=100), _state())

    row = merged.row(by_predicate=pl.col("account") == "alice", named=True)
    assert row["balance_prev"] == 0.0
    assert row["basis_prev"] == 0.0
