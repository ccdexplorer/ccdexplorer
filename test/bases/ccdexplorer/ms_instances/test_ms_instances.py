from ccdexplorer.tooter.core import Tooter
from pymongo import ReplaceOne
import pytest
from unittest.mock import patch, MagicMock

from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET
from ccdexplorer.ms_instances.subscriber import Subscriber


@pytest.mark.asyncio
async def test_new_contract(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    mock_bulk_write = MagicMock()
    mock_collection = MagicMock()
    mock_collection.bulk_write = mock_bulk_write
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {Collections.instances: mock_collection},
            clear=False,
        ),
    ):
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
        net = NET.MAINNET
        block_height = 37030223
        await subscriber.process_block_for_instances({"height": block_height}, NET(net))
        mock_bulk_write.assert_called_once()
        args, _ = mock_bulk_write.call_args
        ops = args[0]
        op = ops[0]
        assert isinstance(op, ReplaceOne)

        # Resolved when the instance is created, so the API never has to invoke
        # `supports` on the request path. This module exports no `supports`
        # entrypoint, so the answer is settled from what ms_modules parsed out
        # of the wasm -- no gRPC call was made to reach it.
        cis_support = op._doc.pop("cis_support")
        assert cis_support["has_supports_entrypoint"] is False
        assert cis_support["standards"] == []
        assert (
            cis_support["source_module"]
            == "7f64dfb3555d6f24afce8f157e6dce0c0823226f1775a26360b9294e54f7ec9f"
        )
        assert cis_support["checked_at"] is not None

        assert op._doc == {
            "_id": "<9958,0>",
            "v1": {
                "owner": "3mukXwSoSrsqPVMphmdGVoHxuDHM4HWxmJsqQNmRfPi7nXZ4Fh",
                "amount": 0,
                "methods": [
                    "ccd_contract.edit_game_room",
                    "ccd_contract.edit_lineup",
                    "ccd_contract.fund_game_room",
                    "ccd_contract.join_game_room",
                    "ccd_contract.process_runnerup_payout",
                    "ccd_contract.process_winner_payout",
                    "ccd_contract.upgrade",
                    "ccd_contract.view",
                    "ccd_contract.view_participants",
                    "ccd_contract.view_runnerups",
                ],
                "name": "init_ccd_contract",
                "source_module": "7f64dfb3555d6f24afce8f157e6dce0c0823226f1775a26360b9294e54f7ec9f",
            },
            "source_module": "7f64dfb3555d6f24afce8f157e6dce0c0823226f1775a26360b9294e54f7ec9f",
        }


@pytest.mark.asyncio
async def test_new_contract2(
    grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
):
    mock_bulk_write = MagicMock()
    mock_collection = MagicMock()
    mock_collection.bulk_write = mock_bulk_write
    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {Collections.instances: mock_collection},
            clear=False,
        ),
    ):
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
        net = NET.MAINNET
        block_height = 37030223
        await subscriber.process_block_for_instances({"height": block_height}, NET(net))
        mock_bulk_write.assert_called_once()
        args, _ = mock_bulk_write.call_args
        ops = args[0]
        op = ops[0]
        assert isinstance(op, ReplaceOne)

        # Resolved when the instance is created, so the API never has to invoke
        # `supports` on the request path. This module exports no `supports`
        # entrypoint, so the answer is settled from what ms_modules parsed out
        # of the wasm -- no gRPC call was made to reach it.
        cis_support = op._doc.pop("cis_support")
        assert cis_support["has_supports_entrypoint"] is False
        assert cis_support["standards"] == []
        assert (
            cis_support["source_module"]
            == "7f64dfb3555d6f24afce8f157e6dce0c0823226f1775a26360b9294e54f7ec9f"
        )
        assert cis_support["checked_at"] is not None

        assert op._doc == {
            "_id": "<9958,0>",
            "v1": {
                "owner": "3mukXwSoSrsqPVMphmdGVoHxuDHM4HWxmJsqQNmRfPi7nXZ4Fh",
                "amount": 0,
                "methods": [
                    "ccd_contract.edit_game_room",
                    "ccd_contract.edit_lineup",
                    "ccd_contract.fund_game_room",
                    "ccd_contract.join_game_room",
                    "ccd_contract.process_runnerup_payout",
                    "ccd_contract.process_winner_payout",
                    "ccd_contract.upgrade",
                    "ccd_contract.view",
                    "ccd_contract.view_participants",
                    "ccd_contract.view_runnerups",
                ],
                "name": "init_ccd_contract",
                "source_module": "7f64dfb3555d6f24afce8f157e6dce0c0823226f1775a26360b9294e54f7ec9f",
            },
            "source_module": "7f64dfb3555d6f24afce8f157e6dce0c0823226f1775a26360b9294e54f7ec9f",
        }
