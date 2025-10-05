import datetime as dt
import math


from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountAddress,
    CCD_BakerId,
    CCD_BlockHash,
    CCD_BlockInfo,
    CCD_BlockSpecialEvent,
    CCD_BlockSpecialEvent_PaydayAccountReward,
    CCD_BlockSpecialEvent_PaydayPoolReward,
    CCD_DelegatorRewardPeriodInfo,
    CCD_PassiveDelegationInfo,
    CCD_PoolInfo,
)
from ccdexplorer.mongodb import Collections, MongoDB
from dateutil import parser
from pydantic import BaseModel


class PaydayInfo(BaseModel):
    date: str
    hash: CCD_BlockHash
    block_info: CCD_BlockInfo
    height_for_first_block: int | None = None
    height_for_last_block: int | None = None
    hash_for_first_block: CCD_BlockHash | None = None
    hash_for_last_block: CCD_BlockHash | None = None
    payday_duration_in_seconds: float | None = None
    payday_block_slot_time: dt.datetime | None = None
    seconds_per_year: float = 31_556_952  # 60 * 60 * 24 * 365.2425
    bakers_with_delegation_information: dict[str, list[CCD_DelegatorRewardPeriodInfo]] | None = None
    bakers_with_delegation_information_current_payday: dict | None = None
    baker_account_ids: dict | None = None
    pool_status_for_bakers: dict | None = None
    special_events_with_rewards: dict[str, CCD_BlockSpecialEvent] | None = None
    passive_delegation_info: CCD_PassiveDelegationInfo | None = None
    pool_info_by_baker_id: dict[str, CCD_PoolInfo] | None = None
    pool_info_by_account_id: dict[str, CCD_PoolInfo] | None = None
    pool_info_by_baker_id_current_payday: dict[str, CCD_PoolInfo] | None = None
    bakers_that_need_APY: list | None = None
    accounts_that_need_APY: list | None = None
    list_of_delegators: list[CCD_AccountAddress] | None = None
    account_with_stake_by_account_id: dict | None = None
    baker_account_ids_by_account_id: dict[str, CCD_BakerId] | None = None
    account_rewards: dict[str, CCD_BlockSpecialEvent_PaydayAccountReward] | None = None
    pool_rewards: dict[str, CCD_BlockSpecialEvent_PaydayPoolReward] | None = None
    restake_info_validators: dict[str, bool] | None = None
    restake_info_delegators: dict[str, bool] | None = None
    metadata: dict = {}


def calc_apy_for_period(daily_apy: list) -> float:
    daily_ln = [math.log(1 + x) for x in daily_apy]
    avg_ln = sum(daily_ln) / len(daily_ln)
    expp = math.exp(avg_ln)
    apy = expp - 1
    return apy


def calc_apy(rewards: float, staked_amount: float, payday_info: PaydayInfo) -> float:
    """
    Calculate the annual percentage yield (APY) based on rewards and staked amount.
    """
    if staked_amount == 0:
        return 0.0
    return (
        math.pow(
            1 + (rewards / staked_amount),
            payday_info.seconds_per_year / payday_info.payday_duration_in_seconds,  # type: ignore
        )
        - 1
    )


# def read_payday_block_info(
#     hash: CCD_BlockHash, grpcclient: GRPCClient
# ) -> PaydayBlockInfo:
#     """
#     read the PaydayBlockInfo with the block information from the GRPCClient.
#     """
#     block_info = grpcclient.get_block_info(hash)
#     return PaydayBlockInfo(
#         date=block_info.slot_time.strftime("%Y-%m-%d"),
#         hash=block_info.hash,
#         height=block_info.height,
#         slot_time=block_info.slot_time,
#     )


def get_historical_payday_information_entry(
    payday_date_string: str, mongodb: MongoDB
) -> dict | None:
    return mongodb.mainnet[Collections.paydays_v2].find_one({"date": payday_date_string})


def get_previous_payday_information_entry(payday_date_string: str, mongodb: MongoDB) -> dict | None:
    payday_date = parser.parse(payday_date_string)
    previous_payday_date = payday_date - dt.timedelta(days=1)
    previous_payday_date_string = f"{previous_payday_date:%Y-%m-%d}"

    return mongodb.mainnet[Collections.paydays_v2].find_one({"date": previous_payday_date_string})


# def reverse_search_from_dictionary(dictionary: dict, keyword: str | int):
#     return next((key for key, values in dictionary.items() if keyword in values), None)


def reverse_search_from_dictionary(dictionary: dict, keyword: str | int):
    for key, values in dictionary.items():
        account_ids_list = [x["account"] for x in values]
        if keyword in account_ids_list:
            return key
    return None
