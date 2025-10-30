import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    from ccdexplorer.mongodb import MongoDB, Collections
    from git import Repo
    from pydantic import BaseModel
    import dateutil
    import io
    import pandas as pd
    from io import StringIO
    from sortedcontainers import SortedList, SortedSet
    from rich import print
    from bisect import bisect_right, bisect_left
    return (
        BaseModel,
        Collections,
        MongoDB,
        Repo,
        SortedSet,
        StringIO,
        bisect_left,
        dateutil,
        io,
        pd,
        print,
    )


@app.cell
def _(MongoDB):
    mongodb = MongoDB(None)
    return (mongodb,)


@app.cell
def _(BaseModel):
    class CommitResponse(BaseModel):
        date: str
        nightly_accounts: dict
    return (CommitResponse,)


@app.cell
def _(BaseModel):
    class RealizedPriceRequest(BaseModel):
        date: str
        nightly_accounts: dict
        transaction_dates_by_account: dict
        exchange_rates_by_currency: dict    

    return (RealizedPriceRequest,)


@app.cell
def _(Collections, mongodb, print):
    pp = [
        {
            '$match': {
            
                'effect_type': {"$in":['transferred_with_schedule', 'account_transfer']}
            }
        }
    ]
    result_pp = list(mongodb.mainnet[Collections.impacted_addresses].aggregate(pp))
    print (len(result_pp))
    return (result_pp,)


@app.cell
def _(Collections, mongodb):
    nightly_accounts_dict = {x["account"]:x["total_balance"] for x in mongodb.mainnet[Collections.nightly_accounts].find({}) if x.get("account")}
    return


@app.cell
def _(mongodb):
    result = mongodb.utilities_db["exchange_rates_historical"].find({})
    exchange_rates_by_currency = dict({})
    for x in result:
        if not exchange_rates_by_currency.get(x["token"]):
            exchange_rates_by_currency[x["token"]] = {}
            exchange_rates_by_currency[x["token"]][x["date"]] = x["rate"]
        else:
            exchange_rates_by_currency[x["token"]][x["date"]] = x["rate"]
    return (exchange_rates_by_currency,)


@app.cell
def _(SortedSet, result_pp):
    transaction_dates_by_account = {}
    for entry in result_pp:
        impacted_address = entry['impacted_address']
        if not transaction_dates_by_account.get(impacted_address):
            transaction_dates_by_account[impacted_address] = SortedSet()
 
        transaction_dates_by_account[impacted_address].add(entry["date"])
    return (transaction_dates_by_account,)


@app.cell
def _(RealizedPriceRequest, bisect_left):
    def calculate_realized_price_for_date(rpr: RealizedPriceRequest):
        sum_of_balances = 0
        sum_of_multiplied_balances = 0
        sum_zero_balance = 0
        account_count = 0
        account_count_zero = 0
        # rpr.date="2022-07-31"
        for account_id in list(rpr.nightly_accounts.keys()):
            if rpr.transaction_dates_by_account.get(account_id):
                account_count+=1
                tx_dates_for_account = list(rpr.transaction_dates_by_account.get(account_id))
            
                found_index = bisect_left(tx_dates_for_account, rpr.date)
                if found_index == 0: # only tx after commit date, so ignore in calc
                    tx_date_to_use = None
                else:
                    tx_date_to_use = tx_dates_for_account[found_index-1]
            
                # print (account_id, tx_dates_for_account, tx_date_to_use)
                if tx_date_to_use: 
                    if rpr.exchange_rates_by_currency["CCD"].get(tx_date_to_use):
                        CCD_price_to_use = rpr.exchange_rates_by_currency["CCD"].get(tx_date_to_use) 
                    else:
                        CCD_price_to_use = 0.01703143
                    
                    sum_of_multiplied_balances += CCD_price_to_use * rpr.nightly_accounts[account_id]
                    sum_of_balances += rpr.nightly_accounts[account_id]
            else:
                sum_zero_balance += rpr.nightly_accounts[account_id]
                if rpr.nightly_accounts[account_id] == 0:
                    account_count_zero +=1
                # else:
                #     print (account_id)
        realized_price = sum_of_multiplied_balances /sum_of_balances 
        # print (realized_price, account_count, len(rpr.nightly_accounts), sum_zero_balance, account_count_zero)
        # print (len(rpr.nightly_accounts)-account_count-account_count_zero)
        return realized_price
    return (calculate_realized_price_for_date,)


@app.cell
def _(CommitResponse, StringIO, dateutil, io, pd):
    def get_nightly_accounts_from_commit(commit):
        targetfile = commit.tree / 'accounts.csv'
        timestamp = f"{dateutil.parser.parse(commit.message):%Y-%m-%d}"
        # print (timestamp)
        with io.BytesIO(targetfile.data_stream.read()) as f:
            my_file = f.read().decode('utf-8')
        data = StringIO(my_file) 
        df=pd.read_csv(data)
        df= df[['account', 'total_balance']]
        nightly_accounts = {x['account']:x['total_balance'] for x in df.to_dict(orient='records')}
        return CommitResponse(date= timestamp, nightly_accounts=nightly_accounts)
    return (get_nightly_accounts_from_commit,)


@app.cell
def _(
    RealizedPriceRequest,
    Repo,
    calculate_realized_price_for_date,
    exchange_rates_by_currency,
    get_nightly_accounts_from_commit,
    print,
    transaction_dates_by_account,
):
    dir = "/Users/sander/Developer/open_source/ccdexplorer-accounts"
    repo = Repo(dir)
    commits = list(repo.iter_commits('main'))
    ddd = {}
    for commit in commits[:2]:
    
        commit_response = get_nightly_accounts_from_commit(commit)
        # print (f"Working on {commit_response.date}")
        rpr = RealizedPriceRequest(
            date=commit_response.date, 
            nightly_accounts=commit_response.nightly_accounts, 
            transaction_dates_by_account=transaction_dates_by_account, 
            exchange_rates_by_currency=exchange_rates_by_currency
            )
        rp = calculate_realized_price_for_date(rpr)
        ddd[commit_response.date] = rp
    print (ddd)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
