from ccdexplorer.mongodb import MongoDB, CollectionsUtilities
from pymongo import ReplaceOne

from ..nightrunner.utils import AnalysisType, write_queue_to_collection, get_polars_df_from_git

import polars as pl
from pathlib import Path

BALANCES_DIR = Path("data/balances")
FX_FILE = Path("data/fx/ccd_usd.csv")
OUT_FILE = Path("data/realised/realised_prices.csv")


def get_historical_fx_rate_for(date: str, mongodb: MongoDB) -> float:
    fx_rate = mongodb.utilities[CollectionsUtilities.exchange_rates_historical].find_one(
        {"token": "CCD", "date": date}
    )
    if fx_rate and "rate" in fx_rate:
        return fx_rate["rate"]
    return 0.01703143 / 0.91  # EUR RATE * AVG EUR/USD rate for June 2021-June 2022 (roughly)


def merge_snapshot_with_state(snap: pl.DataFrame, state: pl.DataFrame) -> pl.DataFrame:
    """Today's balances beside yesterday's basis, keeping every account.

    Both sides matter: an account missing from today's snapshot still holds
    a basis, and an account appearing for the first time has none, which is
    what fill_null(0) turns into the formula's balance_prev == 0 branch.

    coalesce=True is not decoration. Polars renamed how="outer" to
    how="full" and made non-coalescing the default in the same move, so the
    bare rename keeps the rows but splits the key: an account only in the
    state lands in account_right with account left null, and every later
    pl.col("account") reads that null instead of the name.
    """
    return snap.join(state, on="account", how="full", coalesce=True).fill_null(0)


def perform_realized_prices(context, commits_by_day: dict, mongodb: MongoDB) -> dict:
    analysis = AnalysisType.statistics_realized_prices
    state = pl.DataFrame(
        {"account": [], "balance": [], "basis": []},
        schema_overrides={"account": pl.String, "balance": pl.Float64, "basis": pl.Float64},
    )

    rows = []

    for date, commit in reversed(commits_by_day.items()):
        fx_today = get_historical_fx_rate_for(date, mongodb)
        snap = get_polars_df_from_git(commits_by_day[date])

        if snap is None:
            continue

        snap = (
            snap.select(["account", "total_balance"])
            .rename({"total_balance": "balance"})
            .with_columns(pl.lit(fx_today).alias("fx"))
        )

        state = state.rename({"balance": "balance_prev", "basis": "basis_prev"})
        merged = merge_snapshot_with_state(snap, state)

        merged = merged.with_columns((pl.col("balance") - pl.col("balance_prev")).alias("delta"))

        merged = merged.with_columns(
            pl.when(pl.col("balance_prev") == 0)
            .then(pl.col("fx"))
            .when(pl.col("delta") > 0)
            .then(
                ((pl.col("balance_prev") * pl.col("basis_prev")) + (pl.col("delta") * pl.col("fx")))
                / pl.col("balance")
            )
            .otherwise(pl.col("basis_prev"))
            .alias("basis")
        )

        realised_cap = (merged["balance"] * merged["basis"]).sum()
        total_supply = merged["balance"].sum()
        realised_price = realised_cap / total_supply if total_supply > 0 else None

        rows.append({"date": date, "realised_cap": realised_cap, "realised_price": realised_price})

        state = merged.select(["account", "balance", "basis"])
        pass

        # pl.DataFrame(rows).write_csv(OUT_FILE)
        # print(f"Wrote realised prices → {OUT_FILE}")

        _id = f"{date}-{analysis.value}"
        context.log.info(_id)

        dct = {
            "_id": _id,
            "date": date,
            "type": analysis.value,
            "realised_cap": realised_cap,
            "realised_price": realised_price,
        }

        # queue.append(
        #     ReplaceOne(
        #         {"_id": _id},
        #         replacement=dct,
        #         upsert=True,
        #     )
        # )

        write_queue_to_collection(
            mongodb,
            [
                ReplaceOne(
                    {"_id": _id},
                    replacement=dct,
                    upsert=True,
                )
            ],
            analysis,
        )
        # queue = []
        context.log.info(
            {"message": "Realized prices updated", "date": date, "realised_price": realised_price}
        )
    return {"message": {}}
