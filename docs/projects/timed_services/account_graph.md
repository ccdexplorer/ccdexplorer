## Goal
The goal of this service is to pre-calculate data points to display Account Graphs for every account. This graph displays both the CCD balance as well as the USD value of that CCD balance over time. 

## Data used
All balances are taken from the nightly file that is created in (Accounts)[accounts.md] and is stored in the repo. All historical rates are taken from the rates collection.

## Calculations
The graph shows data points only when the balance has changed on that day. In order to calculate this, we take, for every day for every account present on that day, a snapshot and compare the CCD balance to its value from yesterday. If the value if different, we add the data point to the collection. 

## Example
```
{
  "_id": "2025-03-11-account_graph-3EctbG8WaQkTqZb1NTJPAFnqmuhvW",
  "type": "account_graph",
  "account_address_canonical": "3EctbG8WaQkTqZb1NTJPAFnqmuhvW",
  "account_index": 4,
  "date": "2025-03-11",
  "ccd_balance": 350103097.878814,
  "rate": 0.0035252313701634275,
  "ccd_balance_in_USD": 1234194.423433792
}
```
## Location

`nightrunner - AccountGraph`

!!! Note
    We need to make a decision between pre-calculating and storing data vs computing data on rerequest. Doing the latter will lead to unacceptable wait times for the user, as we need to request 1300+ balances from the node for an account to view.

    On the other hand, pre-computing all balances for all accounts for every day is also wasteful, as we have many accounts that have acquired a CCD balance early on, but that balance has never changed. Pre-computing for every day would store the same balance on all these days. 
