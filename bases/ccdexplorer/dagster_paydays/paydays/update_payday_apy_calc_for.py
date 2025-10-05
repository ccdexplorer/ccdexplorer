import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    MongoDB,
)

from .utils import PaydayInfo


def perform_payday_apy_calc_for(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
) -> PaydayInfo:
    """
    This method determines for which accounts and baker_ids we need
    to calculate APY.
    """

    list_of_lists_of_delegators = [
        x
        for x in payday_info.bakers_with_delegation_information.values()  # type: ignore
    ]
    flat_list_of_delegators = [item for sublist in list_of_lists_of_delegators for item in sublist]
    payday_info.list_of_delegators = [x["account"] for x in flat_list_of_delegators]  # type: ignore
    # dict of all delegators accounts...
    payday_info.account_with_stake_by_account_id = {
        x["account"]: x["stake"]  # type: ignore
        for x in flat_list_of_delegators  # type: ignore
    }

    # add the account_ids of bakers...
    for account_id, pool_info in payday_info.pool_info_by_account_id.items():  # type: ignore
        payday_info.account_with_stake_by_account_id[account_id] = (
            pool_info.current_payday_info.baker_equity_capital  # type: ignore
        )

    baker_account_ids = payday_info.baker_account_ids.values()  # type: ignore

    payday_info.accounts_that_need_APY = list(
        set(payday_info.list_of_delegators) | set(baker_account_ids)
    )
    payday_info.bakers_that_need_APY = list(
        payday_info.bakers_with_delegation_information.keys()  # type: ignore
    )
    return payday_info
