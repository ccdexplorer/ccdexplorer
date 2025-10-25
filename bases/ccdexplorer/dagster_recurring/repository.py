import dagster as dg
import ccdexplorer.dagster_recurring.src.dashboard_nodes as dashboard_nodes
import ccdexplorer.dagster_recurring.src.market_position as market_position
import ccdexplorer.dagster_recurring.src.memos as memos
import ccdexplorer.dagster_recurring.src.top_impacted_addresses as top_impacted_addresses
import ccdexplorer.dagster_recurring.src.validators_missed as validators_missed
import ccdexplorer.dagster_recurring.src.tx_types_count as tx_types_count
import ccdexplorer.dagster_recurring.src.metadata as metadata
import ccdexplorer.dagster_recurring.src.spot_retrieval as spot_retrieval

defs = dg.Definitions.merge(
    market_position.defs,
    dashboard_nodes.defs,
    memos.defs,
    top_impacted_addresses.defs,
    validators_missed.defs,
    tx_types_count.defs,
    metadata.defs,
    spot_retrieval.defs,
)
