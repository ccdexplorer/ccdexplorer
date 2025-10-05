from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import ProtocolVersions
from ccdexplorer.mongodb import MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    get_df_from_git,
    get_hash_from_date,
    write_queue_to_collection,
)


def perform_data_for_network_summary(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB, grpcclient: GRPCClient
) -> dict:
    analysis = AnalysisType.statistics_network_summary
    _id = f"{d_date}-{analysis.value}"
    queue = []
    df = get_df_from_git(commits_by_day[d_date])
    if df is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    block_hash = get_hash_from_date(d_date, mongodb)
    s = grpcclient.get_tokenomics_info(block_hash)
    versioned_object = None
    if s.v0:
        versioned_object = s.v0
    if s.v1:
        versioned_object = s.v1

    if not versioned_object:
        return {}

    protocol_version = ProtocolVersions(versioned_object.protocol_version).name
    total_amount = int(versioned_object.total_amount) / 1_000_000
    total_encrypted_amount = int(versioned_object.total_encrypted_amount) / 1_000_000
    baking_reward_account = int(versioned_object.baking_reward_account) / 1_000_000
    finalization_reward_account = int(versioned_object.finalization_reward_account) / 1_000_000
    gas_account = int(versioned_object.gas_account) / 1_000_000

    accounts_count = len(df)
    f_no_bakers = df["baker_id"].isna()
    bakers_count = len(df[~f_no_bakers])
    suspended_count = df["is_suspended"].sum() if "is_suspended" in df.columns else None

    # New row to add, as a dictionary.
    dct = {
        "_id": _id,
        "type": analysis.value,
        "date": d_date,
        "protocol_version": protocol_version,
        "total_amount": total_amount,
        "total_encrypted_amount": total_encrypted_amount,
        "baking_reward_account": baking_reward_account,
        "finalization_reward_account": finalization_reward_account,
        "gas_account": gas_account,
        "account_count": accounts_count,
        "validator_count": bakers_count,
    }

    if suspended_count is not None:
        dct.update({"suspended_count": suspended_count})
    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )

    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(f"info: {dct}")
    return {"dct": dct}
