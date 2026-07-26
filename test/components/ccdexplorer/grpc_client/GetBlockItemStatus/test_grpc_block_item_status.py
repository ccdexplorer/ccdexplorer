import pytest

from ccdexplorer.grpc_client import GRPCClient
from rich import print


@pytest.fixture
def grpcclient_devnet():
    return GRPCClient(devnet=True)


def test_block_item_status_devnet_lock_create(grpcclient_devnet: GRPCClient):
    """A real devnet transaction that creates a PLT lock, looked up by tx hash alone."""
    tx_hash = "1e21c8af2dc081703a775a26cbbeb2f29e27c36f624c4fc3a99553fd6133d440"
    status = grpcclient_devnet.get_block_item_status(tx_hash)
    print(status)

    assert status.received is False
    assert status.committed is None
    assert status.finalized is not None
    assert (
        status.finalized.block_hash
        == "088f3c69fb54128dc825242507c0dcb972052ee9282752b2374d143e0f09ddb7"
    )

    outcome = status.finalized.outcome
    assert outcome.hash == tx_hash
    assert outcome.energy_cost == 715
    assert outcome.type.type == "account_transaction"
    assert outcome.type.contents == "meta_update_effect"

    account_transaction = outcome.account_transaction
    assert account_transaction is not None
    assert account_transaction.cost == 3071522
    assert account_transaction.sender == "4bbprb6pgUdUTJPaYva52smhdUnoZmU1FvZCfvarfarTtLxDXj"
    assert account_transaction.outcome == "success"

    events = account_transaction.effects.meta_update_effect.events
    assert len(events) == 1

    lock_create_event = events[0].lock_create_event
    assert lock_create_event is not None
    assert lock_create_event.lock_id.account_index == 75
    assert lock_create_event.lock_id.sequence_number == 1
    assert lock_create_event.lock_id.creation_order == 0
    assert lock_create_event.lock_config

    # cross-check: the lock_config CBOR blob should decode cleanly too
    decoded = grpcclient_devnet.decode_lock_config(lock_create_event.lock_config)
    assert decoded.recipients == ["4bbprb6pgUdUTJPaYva52smhdUnoZmU1FvZCfvarfarTtLxDXj"]
    assert decoded.controller.tokens == ["IMT"]
    assert sorted(decoded.controller.grants[0].roles) == ["cancel", "fund", "return", "send"]
