import pytest

from ccdexplorer.grpc_client.CCD_Types import CCD_LockId


def test_to_str_matches_node_sdk_test_vector():
    # From Concordium/concordium-node-sdk-js (feature/p11-locks),
    # packages/sdk/test/ci/plt/LockId.test.ts: LockId.create(1n, 2n, 3n).toString()
    lock_id = CCD_LockId(account_index=1, sequence_number=2, creation_order=3)
    assert lock_id.to_str() == "W9EXVYXZJq"


def test_from_str_matches_node_sdk_test_vector():
    assert CCD_LockId.from_str("W9EXVYXZJq") == CCD_LockId(
        account_index=1, sequence_number=2, creation_order=3
    )


@pytest.mark.parametrize(
    "account_index,sequence_number,creation_order",
    [
        (0, 0, 0),
        (17, 42, 0),
        (300, 999999999999, 5),
        (2**64 - 1, 2**64 - 1, 2**64 - 1),
    ],
)
def test_to_str_from_str_roundtrip(account_index, sequence_number, creation_order):
    lock_id = CCD_LockId(
        account_index=account_index,
        sequence_number=sequence_number,
        creation_order=creation_order,
    )
    assert CCD_LockId.from_str(lock_id.to_str()) == lock_id


def test_from_str_rejects_invalid_string():
    with pytest.raises(ValueError):
        CCD_LockId.from_str("not-a-valid-lock-id")
