"""The hourly tx type counts, for every net the partition knows.

tx_types_count is the only source of the counts on /{net}/transactions-by-type:
the dropdown labels come from it, and so does total_for_type in the api's
POST /v2/{net}/transactions/{count}/{skip}. On devnet every one of them read
zero, because the schedule asked for runs like this:

    for net in ["mainnet", "testnet"]:

while the asset's own partition definition has carried devnet all along, and
every other recurring asset -- stable_address_info, metadata, memos,
top_impacted_addresses, redis_failures -- drives its requests from
net_partition.get_partition_keys(). This one hardcoded the list and dropped
the third net, so concordium_devnet.tx_types_count was never written. The
transactions themselves were always there; only the counts were missing,
which left a dropdown where every type looked equally empty.

Asserting against net_partition rather than a literal list is the point: a
fourth net added to the partition should not need this schedule edited again.
"""

import datetime as dt

import dagster as dg
import pytest

from ccdexplorer.dagster_recurring.src import tx_types_count_v2 as src
from ccdexplorer.dagster_recurring.src._partitions import net_partition

SCHEDULED_AT = dt.datetime(2026, 11, 11, 14, 1, tzinfo=dt.timezone.utc)
# The schedule runs the last fully completed hour, so 14:01 asks for 13:00.
EXPECTED_HOUR = "2026-11-11-13"


@pytest.fixture
def requests():
    with dg.instance_for_test() as instance:
        context = dg.build_schedule_context(
            instance=instance, scheduled_execution_time=SCHEDULED_AT
        )
        yield list(src.schedule(context))


def dimensions(partition_key: str) -> dict:
    """A multi-partition key as the schedule writes it: "<hour>|<net>"."""
    datetimestamp, net = partition_key.split("|")
    return {"datetime": datetimestamp, "net": net}


def nets_asked_for(requests) -> set:
    return {dimensions(r.partition_key)["net"] for r in requests}


def test_every_net_in_the_partition_is_asked_for(requests):
    """The bug. devnet is a partition key and was never requested."""
    assert nets_asked_for(requests) == set(net_partition.get_partition_keys())


def test_devnet_is_one_of_them(requests):
    """Named on its own, because it is the one that was missing and the one
    the empty dropdown was reported on."""
    assert "devnet" in nets_asked_for(requests)


def test_one_run_per_net(requests):
    assert len(requests) == len(net_partition.get_partition_keys())


def test_each_run_covers_the_last_completed_hour(requests):
    """Unchanged: 14:01 asks for the 13:00 partition, not the one in progress."""
    for request in requests:
        assert dimensions(request.partition_key)["datetime"] == EXPECTED_HOUR


def test_each_net_keeps_its_own_run_key(requests):
    """The run key is what stops a net's run being deduplicated against
    another net's for the same hour."""
    assert len({r.run_key for r in requests}) == len(requests)
