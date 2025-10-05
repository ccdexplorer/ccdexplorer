## Goal
The goal of this script is to calculate, on a daily basis, the *realized market price* of CCD based on on-chain account balances and historical exchange rates.  
By combining daily snapshots of account balances with historical CCD-USD exchange rates, we can derive both the **realized capitalization** (sum of balances weighted by their acquisition basis) and the corresponding **realized price** (realized capitalization divided by total supply).

This metric reflects the *average cost basis* of all CCD in circulation — an indicator of how much capital has been effectively “invested” in the chain, adjusted for each account’s entry price over time.

## Data used
The calculation uses three main data sources:

1. **Account balance snapshots** — stored as CSV snapshots under `data/balances/`.  
   Each file represents a full ledger state for a given date and is fetched via Git commits (`get_polars_df_from_git(commits_by_day[date])`).

2. **Historical CCD exchange rates** — stored in the MongoDB collection  
   `utilities.exchange_rates_historical`.  
   Each document contains the fields `token`, `date`, and `rate`.  
   If no matching rate is found for a given day, a default fallback value is used:
   ```
   0.01703143 / 0.91  # CCD/EUR × EUR/USD average rate (June 2021–June 2022)
   ```

   This fallback is a rough estimate of purchase levels in the PP sale.

## Calculation steps
The script iterates chronologically (oldest → newest) through all available balance snapshots, performing the following steps per day:

1. **Load FX rate**  
   Retrieve the CCD/USD exchange rate for that date from MongoDB, or use the fallback.

2. **Load balance snapshot**  
   Convert the snapshot CSV to a Polars DataFrame, selecting only `account` and `total_balance` columns.  
   Rename `total_balance` → `balance` and attach the day’s FX rate as a column `fx`.

3. **Merge with previous state**  
   Join the new snapshot with the previous day’s state (`account`, `balance_prev`, `basis_prev`) via an outer join to ensure that all accounts are retained.  
   Missing values are filled with 0.

4. **Calculate balance delta**  
   ```
   delta = balance - balance_prev
   ```

5. **Update acquisition basis**  
   For each account:
   - If the account is new (`balance_prev == 0`), set `basis = fx` (the day’s rate).  
   - If the balance increased, compute a weighted average:
     ```
     basis = ((balance_prev * basis_prev) + (delta * fx)) / balance
     ```
   - Otherwise, retain the previous basis.

6. **Compute realized metrics**
   - Realized capitalization = Σ(balance × basis)
   - Total supply = Σ(balance)
   - Realized price = realized_cap / total_supply


8. **Update in-memory state**
   Replace `state` with the merged DataFrame (`account`, `balance`, `basis`) to serve as the previous day’s state for the next iteration.

## Example
For 2025-11-01, the following document is stored:

```json
{
  "_id": "2025-11-01-statistics_realized_prices",
  "date": "2025-11-01",
  "type": "statistics_realized_prices",
  "realised_cap": 173928821.82226622,
  "realised_price": 0.012279167038748411
}
```

## Notes
!!! Note
    This script treats each balance increase as a “purchase” at that day’s FX rate, and decreases (or exits) do not alter the historical basis. This yields an approximate cost-basis average for the circulating supply, not a true realized profit metric.


## Where is this data used?
The realized price is used by in the Realized Price Graph. 

## Location
`nightrunner – realized_prices`