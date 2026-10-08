"""What goes into statistics_cooldowns, and what it costs to find out.

Cooldown is where stake sits after a validator or delegator reduces it:
locked, earning nothing, until it expires. Nothing stored it, so the site
could show what is in cooldown now and never what it had been.

The node is asked which accounts are in cooldown rather than being asked
about every account -- there are tens of them against a hundred thousand
accounts -- and it is asked at each day's final block rather than at
last_final, which is what makes the series backfillable.
"""

from types import SimpleNamespace


from ccdexplorer.dagster_nightrunner.nightrunner.update_cooldowns import (
    FIELD_FOR_STATUS,
    accounts_in_any_cooldown,
    document_for_date,
    totals_for_block,
)
from ccdexplorer.grpc_client.CCD_Types import CoolDownStatus

BLOCK = "a-block-hash"


def _entry(status, amount):
    return SimpleNamespace(status=status, amount=amount)


class _Node:
    """Enough of the gRPC client to answer the three streams and lookups."""

    def __init__(self, cooldown=(), pre=(), pre_pre=(), per_account=None):
        self._streams = {"cooldown": cooldown, "pre": pre, "pre_pre": pre_pre}
        self._per_account = per_account or {}
        self.account_lookups = 0

    def get_cooldown_accounts(self, block, net):
        return list(self._streams["cooldown"])

    def get_pre_cooldown_accounts(self, block, net):
        return list(self._streams["pre"])

    def get_pre_pre_cooldown_accounts(self, block, net):
        return list(self._streams["pre_pre"])

    def get_account_info(self, block, account_index, net):
        self.account_lookups += 1
        return SimpleNamespace(cooldowns=self._per_account.get(account_index, []))


# --- which accounts get looked at -----------------------------------------


def test_the_three_streams_are_unioned():
    """They are not nested. Measured against mainnet on 2025-10-07, six
    accounts were in pre-pre-cooldown and none of them appeared in the
    cooldown stream, so reading only that one undercounts."""
    node = _Node(cooldown=[1, 2], pre=[3], pre_pre=[4, 2])

    assert accounts_in_any_cooldown(node, BLOCK, None) == {1, 2, 3, 4}


def test_a_stream_of_objects_reads_the_same_as_a_stream_of_indexes():
    """One of the three yields AccountPending and the others bare ints."""
    node = _Node(cooldown=[SimpleNamespace(account_index=7)], pre=[8])

    assert accounts_in_any_cooldown(node, BLOCK, None) == {7, 8}


def test_only_accounts_in_cooldown_are_looked_up():
    """The cost of this asset is the thing that decides whether it can run
    nightly. It must never become a walk of every account."""
    node = _Node(cooldown=[1, 2, 3], per_account={})

    totals_for_block(node, BLOCK, None)

    assert node.account_lookups == 3


# --- what the totals say ---------------------------------------------------


def test_amounts_are_summed_per_status():
    node = _Node(
        cooldown=[1, 2],
        per_account={
            1: [_entry(CoolDownStatus.COOLDOWN, 100)],
            2: [_entry(CoolDownStatus.COOLDOWN, 50), _entry(CoolDownStatus.PRE_COOLDOWN, 7)],
        },
    )

    totals = totals_for_block(node, BLOCK, None)

    assert totals["cooldown"]["amount"] == 150
    assert totals["pre_cooldown"]["amount"] == 7


def test_an_account_with_two_cooldowns_counts_once():
    """P7's whole point is that stake can be reduced again while a
    reduction is already in cooldown, so one account can hold several. The
    count is accounts, not entries."""
    node = _Node(
        cooldown=[1],
        per_account={1: [_entry(CoolDownStatus.COOLDOWN, 10), _entry(CoolDownStatus.COOLDOWN, 20)]},
    )

    totals = totals_for_block(node, BLOCK, None)

    assert totals["cooldown"]["amount"] == 30
    assert totals["cooldown"]["accounts"] == 1


def test_an_account_in_two_states_counts_in_both():
    node = _Node(
        cooldown=[1],
        per_account={
            1: [
                _entry(CoolDownStatus.COOLDOWN, 10),
                _entry(CoolDownStatus.PRE_PRE_COOLDOWN, 5),
            ]
        },
    )

    totals = totals_for_block(node, BLOCK, None)

    assert totals["cooldown"]["accounts"] == 1
    assert totals["pre_pre_cooldown"]["accounts"] == 1


def test_every_status_the_chain_has_is_stored():
    """A state with no field would be silently dropped from the total."""
    assert set(FIELD_FOR_STATUS) == set(CoolDownStatus)


# --- the document ----------------------------------------------------------


def test_the_document_carries_an_amount_and_a_count_per_state():
    node = _Node(cooldown=[1], per_account={1: [_entry(CoolDownStatus.COOLDOWN, 100)]})

    doc = document_for_date("2026-10-06", BLOCK, node, None)

    for field in FIELD_FOR_STATUS.values():
        assert f"{field}_amount" in doc
        assert f"{field}_accounts" in doc


def test_the_total_is_the_whole_queue():
    """A reader asking how much is locked means all three states: none of
    them is spendable, and they are one journey."""
    node = _Node(
        cooldown=[1, 2],
        per_account={
            1: [_entry(CoolDownStatus.COOLDOWN, 100)],
            2: [_entry(CoolDownStatus.PRE_PRE_COOLDOWN, 25)],
        },
    )

    doc = document_for_date("2026-10-06", BLOCK, node, None)

    assert doc["total_amount"] == 125


def test_the_id_and_type_match_the_analysis():
    node = _Node()

    doc = document_for_date("2026-10-06", BLOCK, node, None)

    assert doc["_id"] == "2026-10-06-statistics_cooldowns"
    assert doc["type"] == "statistics_cooldowns"
    assert doc["date"] == "2026-10-06"


def test_a_day_with_nothing_in_cooldown_is_still_a_document():
    """Zero is a reading. Skipping the day would leave a gap the chart
    would fill by drawing a line straight across it."""
    doc = document_for_date("2024-10-30", BLOCK, _Node(), None)

    assert doc["total_amount"] == 0
    assert doc["cooldown_accounts"] == 0


def test_amounts_stay_in_the_unit_the_chain_gives_them_in():
    """microCCD. The chart scales them, the same way transaction fees do."""
    node = _Node(cooldown=[1], per_account={1: [_entry(CoolDownStatus.COOLDOWN, 1_000_000)]})

    doc = document_for_date("2026-10-06", BLOCK, node, None)

    assert doc["cooldown_amount"] == 1_000_000


# --- the wiring ------------------------------------------------------------


def test_the_partitions_start_at_protocol_version_7():
    """Cooldowns in this form are a P7 feature, and P7 reached mainnet on
    30 October 2024. From genesis this would be about 1200 partitions
    measuring a number the node defines as zero."""
    from ccdexplorer.dagster_nightrunner.src._partitions import partitions_def_from_cooldowns

    assert partitions_def_from_cooldowns.start.strftime("%Y-%m-%d") == "2024-10-30"


def test_the_asset_and_its_job_are_in_the_repository():
    from ccdexplorer.dagster_nightrunner.repository import defs

    assets = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    jobs = {j.name for j in defs.resolve_all_job_defs()}

    assert "cooldowns" in assets
    assert "j_from_cooldowns" in jobs


def test_the_job_runs_nightly():
    """Defining the job is not enough: nothing runs it unless the sensor
    that fires after accounts_repo asks for it."""
    import inspect

    from ccdexplorer.dagster_nightrunner.src import accounts_repo

    source = inspect.getsource(accounts_repo)
    # Once in the sensor's jobs=, once in the list it builds run requests from.
    assert source.count("job_from_cooldowns") >= 2
