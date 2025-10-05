import pytest


from ccdexplorer.grpc_client import GRPCClient

from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def test_delegators_reward(grpcclient: GRPCClient):
    block_hash = "ee6f396d82bd3615fb74e53681dbacb1f409fba22eaa12fba60941bc3d387f2b"
    pool_id = 72723
    dr = grpcclient.get_delegators_for_pool_in_reward_period(pool_id, block_hash)

    # print (dr.dict(exclude_none=True))
    print(dr)
    # assert dr.all_pool_total_capital == 8663567331383744
    # assert dr.delegated_capital == 141046997927499
    # assert dr.current_payday_delegated_capital == 139569794466750
    # assert dr.current_payday_transaction_fees_earned == 237025826
    # assert dr.commission_rates.baking == 0.12


def test_delegators_for_pool_first_payday(grpcclient: GRPCClient):
    block_hash = "4b4d397ea2320fc754d7345fc66a3c169c0c3ac9839ed462453d206dde8edeaf"
    pool_id = 815
    try:
        dr = grpcclient.get_delegators_for_pool_in_reward_period(pool_id, block_hash)
    except:  # noqa: E722
        dr = None

    assert dr is None

    # print (dr.dict(exclude_none=True))
    print(dr)


def test_delegators_for_pool_830_midday(grpcclient: GRPCClient):
    block_hash = "8d14cda4a71edba5507e50fd1e78dadfb91c1585750e795c1d66298a5f8fa8c1"
    pool_id = 830
    dr = grpcclient.get_delegators_for_pool_in_reward_period(pool_id, block_hash)
    found = False
    for d in dr:
        if d.account == "4XrWuDridR7oYAdECLJkEFv1NUysE6xtNsCVCgijPcPBiuXEg3":
            found = True

    assert found is True


def test_delegators_for_pool_830_last_block_in_payday(grpcclient: GRPCClient):
    block_hash = "4169d2adc7ef71af81afa279acc2a961a92d19b213550e10555cec66174f61b1"
    pool_id = 830

    dr = grpcclient.get_delegators_for_pool_in_reward_period(pool_id, block_hash)
    found = False
    for d in dr:
        if d.account == "4XrWuDridR7oYAdECLJkEFv1NUysE6xtNsCVCgijPcPBiuXEg3":
            found = True

    assert found is True
