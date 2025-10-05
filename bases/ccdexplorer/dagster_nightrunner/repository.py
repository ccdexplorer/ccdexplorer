import dagster as dg
import ccdexplorer.dagster_nightrunner.src.accounts_repo as accounts_repo
import ccdexplorer.dagster_nightrunner.src.account_graph_data as account_graph_data
import ccdexplorer.dagster_nightrunner.src.tx_types as tx_types
import ccdexplorer.dagster_nightrunner.src.daily_holders as daily_holders
import ccdexplorer.dagster_nightrunner.src.daily_limits as daily_limits
import ccdexplorer.dagster_nightrunner.src.tx_fees as tx_fees
import ccdexplorer.dagster_nightrunner.src.exchange_volume as exchange_volume
import ccdexplorer.dagster_nightrunner.src.microccd as microccd
import ccdexplorer.dagster_nightrunner.src.classified_pools as classified_pools
import ccdexplorer.dagster_nightrunner.src.exchange_wallets as exchange_wallets
import ccdexplorer.dagster_nightrunner.src.ccd_classified as ccd_classified
import ccdexplorer.dagster_nightrunner.src.unique_addresses as unique_addresses
import ccdexplorer.dagster_nightrunner.src.historical_rates as historical_rates
import ccdexplorer.dagster_nightrunner.src.network_summary as network_summary
import ccdexplorer.dagster_nightrunner.src.network_activity as network_activity
import ccdexplorer.dagster_nightrunner.src.release_amounts as release_amounts
import ccdexplorer.dagster_nightrunner.src.tx_by_project as tx_by_project
import ccdexplorer.dagster_nightrunner.src.projects_json as projects_json
import ccdexplorer.dagster_nightrunner.src.mongo_transactions as mongo_transactions
import ccdexplorer.dagster_nightrunner.src.plt_statistics as plt_statistics
import ccdexplorer.dagster_nightrunner.src.forex as forex
import ccdexplorer.dagster_nightrunner.src._jobs as _jobs

defs = dg.Definitions.merge(
    accounts_repo.defs,
    account_graph_data.defs,
    tx_types.defs,
    daily_holders.defs,
    daily_limits.defs,
    tx_fees.defs,
    exchange_volume.defs,
    microccd.defs,
    classified_pools.defs,
    exchange_wallets.defs,
    ccd_classified.defs,
    unique_addresses.defs,
    historical_rates.defs,
    network_summary.defs,
    network_activity.defs,
    release_amounts.defs,
    tx_by_project.defs,
    projects_json.defs,
    mongo_transactions.defs,
    plt_statistics.defs,
    forex.defs,
    _jobs.defs,
)
pass
