"""A net with nothing indexed is a normal state, not a job failure.

This job is partitioned by net and every partition is scheduled every five
minutes, whether or not that net has any data. A devnet is replaced from time
to time and starts empty, so both helper documents are legitimately missing --
which used to raise, producing a failed Dagster run and a Sentry event every
five minutes indefinitely.
"""

from unittest.mock import MagicMock

from ccdexplorer.dagster_recurring.recurring.update_top_impacted_addresses import (
    update_top_impacted_addresses,
)
from ccdexplorer.mongodb import Collections


def _mongodb_with(helpers: dict):
    """A MongoDB stand-in whose `helpers` collection holds `helpers` by _id."""
    helpers_collection = MagicMock()
    helpers_collection.find_one.side_effect = lambda query: helpers.get(query["_id"])

    db = {collection: MagicMock() for collection in Collections}
    db[Collections.helpers] = helpers_collection
    # No impacted addresses and no previous top list.
    db[Collections.impacted_addresses].aggregate.return_value = []
    db[Collections.impacted_addresses_all_top_list].find.return_value = []

    mongodb = MagicMock()
    mongodb.mainnet = db
    mongodb.testnet = db
    mongodb.devnet = db
    return mongodb


def test_unindexed_net_is_skipped_not_failed():
    mongodb = _mongodb_with({})
    context = MagicMock()

    result = update_top_impacted_addresses(context, mongodb, "devnet")

    assert result["sorted_totals"] == []
    assert result["skipped"] == "net not indexed"


def test_missing_top_list_marker_seeds_instead_of_failing():
    """An indexed net whose marker is absent is on its first run.

    Raising there was self-defeating: the marker is only written once the job
    completes, so a job that refused to run without one could never create it.
    """
    mongodb = _mongodb_with({"heartbeat_last_processed_block": {"height": 51_517_071}})
    context = MagicMock()

    result = update_top_impacted_addresses(context, mongodb, "devnet")

    assert "skipped" not in result
    assert (
        result["heartbeat_last_block_processed_impacted_addresses_all_top_list"] == 51_517_071
    )
