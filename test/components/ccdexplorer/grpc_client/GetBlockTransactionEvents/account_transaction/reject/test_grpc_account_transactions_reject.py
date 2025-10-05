# pyright: reportOptionalMemberAccess=false
import pytest


from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient():
    return GRPCClient()


def tx_at_index_from(tx_index: int, block_hash: str, grpcclient: GRPCClient):
    block = grpcclient.get_block_transaction_events(block_hash)
    if tx_index == -1:
        return None
    else:
        return block.transaction_summaries[tx_index]


def test_tx_effect_none(grpcclient: GRPCClient):
    block_hash = "2673b1676b131754c3c119a210044ef00648ffa64e274e2d654d33d98a913c33"
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.account_transaction.outcome == "reject"


#  'AmountTooLarge': 1404965,
def test_tx_effect_none_AmountTooLarge(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1404965)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)

    assert tx.account_transaction.outcome == "reject"
    assert tx.account_transaction.effects.none.reject_reason.amount_too_large.amount == 20000000
    assert tx.type.contents == "amount_too_large"


# {'DuplicateCredIDs': 256356,
def test_tx_effect_none_DupCredIds(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(256356)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert type(tx.account_transaction.effects.none.reject_reason.duplicate_cred_ids.ids) is list
    assert tx.type.contents == "duplicate_cred_ids"


#  'InvalidAccountReference': 600725,
def test_tx_effect_none_InvalidAccountReference(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(600725)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "invalid_account_reference"


#  'InvalidContractAddress': 772189,
def test_tx_effect_none_InvalidContractAddress(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(772189)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "invalid_contract_address"


#  'InvalidCredentials': 1358880,
def test_tx_effect_none_InvalidCredentials(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1358880)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "invalid_credentials"


#  'RejectedReceive': 1371616,
def test_tx_effect_none_RejectedReceive(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1371616)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "rejected_receive"


#  'OutOfEnergy': 1388774,
def test_tx_effect_none_OutOfEnergy(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1388774)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "out_of_energy"


#  'StakeUnderMinimumThresholdForBaking': 1650436,
def test_tx_effect_none_StakeUnderMinimumThresholdForBaking(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1650436)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "stake_under_minimum_threshold_for_baking"


#  'InsufficientBalanceForBakerStake': 1846167,
def test_tx_effect_none_InsufficientBalanceForBakerStake(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1846167)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "insufficient_balance_for_baker_stake"


#  'AlreadyABaker': 1848790,
def test_tx_effect_none_AlreadyABaker(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1848790)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "already_a_baker"


#  'BakerInCooldown': 1961535,
def test_tx_effect_none_BakerInCooldown(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(1961535)[0]
    tx = tx_at_index_from(1, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "baker_in_cooldown"


#  'InvalidReceiveMethod': 2113024,
def test_tx_effect_none_InvalidReceiveMethod(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(2113024)[0]
    tx = tx_at_index_from(1, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "invalid_receive_method"


#  'DuplicateAggregationKey': 2248705,
def test_tx_effect_none_DuplicateAggregationKey(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(2248705)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "duplicate_aggregation_key"


#  'ModuleHashAlreadyExists': 2537993,
def test_tx_effect_none_ModuleHashAlreadyExists(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(2537993)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "module_hash_already_exists"


#  'NotABaker': 2681676,
def test_tx_effect_none_NotABaker(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(2681676)[0]
    tx = tx_at_index_from(1, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "not_a_baker"


#  'SerializationFailure': 3134135,
def test_tx_effect_none_SerializationFailure(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3134135)[0]
    tx = tx_at_index_from(7, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "serialization_failure"


#  'ModuleNotWF': 3134149,
def test_tx_effect_none_ModuleNotWF(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3134149)[0]
    tx = tx_at_index_from(1, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "module_not_wf"


#  'TransactionFeeCommissionNotInRange': 3233120,
def test_tx_effect_none_TransactionFeeCommissionNotInRange(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3233120)[0]
    tx = tx_at_index_from(8, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "transaction_fee_commission_not_in_range"


#  'InsufficientBalanceForDelegationStake': 3276658,
def test_tx_effect_none_InsufficientBalanceForDelegationStake(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3276658)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "insufficient_balance_for_delegation_stake"


#  'StakeOverMaximumThresholdForPool': 3277313,
def test_tx_effect_none_StakeOverMaximumThresholdForPool(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3277313)[0]
    tx = tx_at_index_from(2, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "stake_over_maximum_threshold_for_pool"


#  'PoolClosed': 3890427,
def test_tx_effect_none_PoolClosed(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(3890427)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "pool_closed"


#  'FirstScheduledReleaseExpired': 4384498,
def test_tx_effect_none_FirstScheduledReleaseExpired(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(4384498)[0]
    tx = tx_at_index_from(6, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "first_scheduled_release_expired"


#  'RuntimeFailure': 4591212,
def test_tx_effect_none_RuntimeFailure(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(4591212)[0]
    tx = tx_at_index_from(10, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "runtime_failure"


#  'InvalidInitMethod': 4644652,
def test_tx_effect_none_InvalidInitMethod(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(4644652)[0]
    tx = tx_at_index_from(5, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "invalid_init_method"


#  'InvalidModuleReference': 4914657,
def test_tx_effect_none_InvalidModuleReference(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(4914657)[0]
    tx = tx_at_index_from(25, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "invalid_module_reference"


#  'DelegatorInCooldown': 4962924}
def test_tx_effect_none_DelegatorInCooldown(grpcclient: GRPCClient):
    block_hash = grpcclient.get_blocks_at_height(4962924)[0]
    tx = tx_at_index_from(0, block_hash, grpcclient)
    print(tx)
    assert tx.account_transaction.outcome == "reject"
    assert tx.type.contents == "delegator_in_cooldown"
