import dagster as dg
import ccdexplorer.dagster_paydays.src.paydays as src_paydays
import ccdexplorer.dagster_paydays.src.paydays_day_info as paydays_day_info
import ccdexplorer.dagster_paydays.src.paydays_averages as paydays_averages
import ccdexplorer.dagster_paydays.src._jobs as _jobs


defs = dg.Definitions.merge(
    src_paydays.defs,
    paydays_day_info.defs,
    paydays_averages.defs,
    _jobs.defs,
)
