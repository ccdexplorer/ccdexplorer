"""How much stake is locked in cooldown, one document per day.

Cooldown is where stake sits after a validator or delegator reduces or
removes it: locked, earning nothing, until it expires. Nothing stored it,
so the site could show what is in cooldown now and never what it had been.

Protocol version 7 is where this begins. It reached mainnet on 30 October
2024, and before it the stream is empty by definition -- the node answers
with nothing rather than failing, which is why the partition set starts
there rather than at genesis.

Read at each day's final block rather than at last_final, so the series can
be backfilled: the node still answers for blocks back to P7.
"""

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CoolDownStatus
from ccdexplorer.mongodb import Collections, MongoDB

from ..nightrunner.utils import AnalysisType

#: What each status is called in the stored document. The chain's three
#: states are a queue: stake enters pre-pre-cooldown, moves to pre-cooldown
#: at the snapshot epoch, and to cooldown at the following payday.
FIELD_FOR_STATUS = {
    CoolDownStatus.COOLDOWN: "cooldown",
    CoolDownStatus.PRE_COOLDOWN: "pre_cooldown",
    CoolDownStatus.PRE_PRE_COOLDOWN: "pre_pre_cooldown",
}


def get_hash_from_date(date: str, mongodb: MongoDB) -> str:
    result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": date})
    return result["hash_for_last_block"] if result else ""


def accounts_in_any_cooldown(grpcclient: GRPCClient, block_hash: str, net) -> set[int]:
    """Every account with stake anywhere in the queue.

    The three streams are not nested: measured against mainnet on
    2025-10-07, six accounts were in pre-pre-cooldown and none of them
    appeared in the cooldown stream. Reading only that one undercounts.
    """

    def indexes(pending) -> set[int]:
        # The streams differ: one yields AccountPending, another bare indexes.
        return {x if isinstance(x, int) else x.account_index for x in pending}

    return (
        indexes(grpcclient.get_cooldown_accounts(block_hash, net))
        | indexes(grpcclient.get_pre_cooldown_accounts(block_hash, net))
        | indexes(grpcclient.get_pre_pre_cooldown_accounts(block_hash, net))
    )


def totals_for_block(grpcclient: GRPCClient, block_hash: str, net) -> dict:
    """Amount and account count per status, as at `block_hash`.

    Amounts stay in microCCD, the unit the chain gives them in; the chart
    scales them. Counts are accounts, not entries: one account can hold
    several cooldowns at once, which is the whole point of the P7 queue.
    """
    totals = {field: {"amount": 0, "accounts": 0} for field in FIELD_FOR_STATUS.values()}

    for index in accounts_in_any_cooldown(grpcclient, block_hash, net):
        info = grpcclient.get_account_info(block_hash, account_index=index, net=net)
        seen = set()
        for entry in info.cooldowns or []:
            field = FIELD_FOR_STATUS.get(entry.status)
            if field is None:
                continue
            totals[field]["amount"] += entry.amount
            seen.add(field)
        for field in seen:
            totals[field]["accounts"] += 1

    return totals


def document_for_date(d_date: str, block_hash: str, grpcclient: GRPCClient, net) -> dict:
    """The day's document, built but not stored.

    Separate from the write so it can be looked at and tested without a
    database.
    """
    analysis = AnalysisType.statistics_cooldowns
    totals = totals_for_block(grpcclient, block_hash, net)

    dct = {
        "_id": f"{d_date}-{analysis.value}",
        "type": analysis.value,
        "date": d_date,
    }
    for field, measured in totals.items():
        dct[f"{field}_amount"] = measured["amount"]
        dct[f"{field}_accounts"] = measured["accounts"]
    # Summed here rather than in the chart: a reader asking "how much is
    # locked" means the whole queue, and the three states are one journey.
    dct["total_amount"] = sum(m["amount"] for m in totals.values())
    return dct


def perform_data_for_cooldowns(
    context, d_date: str, mongodb: MongoDB, grpcclient: GRPCClient, net=None
) -> dict:
    """One document for `d_date`, or none if that day has no final block."""
    from ccdexplorer.domain.generic import NET

    net = net or NET.MAINNET
    _id = f"{d_date}-{AnalysisType.statistics_cooldowns.value}"
    context.log.info(_id)

    block_hash = get_hash_from_date(d_date, mongodb)
    if not block_hash:
        # A day with no recorded final block is a day the chain data is not
        # there for; writing a zero would read as "nothing was in cooldown".
        context.log.info(f"no final block recorded for {d_date}, skipping")
        return {"dct": {}}

    dct = document_for_date(d_date, block_hash, grpcclient, net)
    mongodb.mainnet[Collections.statistics].replace_one({"_id": _id}, dct, upsert=True)
    context.log.info(f"info: {dct}")
    return {"dct": dct}
