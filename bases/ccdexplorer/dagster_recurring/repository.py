import dagster as dg
import ccdexplorer.dagster_recurring.src.dashboard_nodes as dashboard_nodes
import ccdexplorer.dagster_recurring.src.market_position as market_position
import ccdexplorer.dagster_recurring.src.memos as memos
import ccdexplorer.dagster_recurring.src.top_impacted_addresses as top_impacted_addresses
import ccdexplorer.dagster_recurring.src.validators_missed as validators_missed
import ccdexplorer.dagster_recurring.src.metadata as metadata
import ccdexplorer.dagster_recurring.src.spot_retrieval as spot_retrieval
import ccdexplorer.dagster_recurring.src.redis_failures as redis_failures
import ccdexplorer.dagster_recurring.src.tx_types_count_v2 as tx_types_count_v2
import ccdexplorer.dagster_recurring.src.instance_maintenance as instance_maintenance

# Every job in this code location materialises a single asset (market_position
# also runs its asset check, which reads that asset and so runs after it either
# way). The default multiprocess executor therefore only adds step subprocesses
# to each run -- and each one re-imports this whole module, rebuilding anything
# created at import time. The in-process executor runs the steps inside the run
# worker instead.
defs = dg.Definitions.merge(
    market_position.defs,
    dashboard_nodes.defs,
    memos.defs,
    top_impacted_addresses.defs,
    validators_missed.defs,
    metadata.defs,
    spot_retrieval.defs,
    redis_failures.defs,
    tx_types_count_v2.defs,
    instance_maintenance.defs,
    dg.Definitions(executor=dg.in_process_executor),
)
