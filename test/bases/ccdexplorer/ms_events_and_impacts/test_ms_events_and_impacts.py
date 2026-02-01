# from ccdexplorer.tooter.core import Tooter
# from pymongo import ReplaceOne
# import pytest
# from unittest.mock import patch, MagicMock

# from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
# from ccdexplorer.grpc_client import GRPCClient
# from ccdexplorer.domain.generic import NET
# from ccdexplorer.ms_events_and_impacts.subscriber import Subscriber


# @pytest.mark.asyncio
# async def test_sponsored_tx(
#     grpcclient: GRPCClient, tooter: Tooter, motormongo: MongoMotor, mongodb: MongoDB
# ):
#     mock_bulk_write = MagicMock()
#     mock_collection = MagicMock()
#     mock_collection.bulk_write = mock_bulk_write
#     with (
#         patch.object(tooter, "send_to_tooter") as _,
#         patch.dict(
#             mongodb.mainnet,
#             {Collections.stable_address_info: mock_collection},
#             clear=False,
#         ),
#     ):
#         subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
#         impacted_addresses = await subscriber.extract_impacted_addresses_from_tx(tx)
#         mock_bulk_write.assert_called_once()
#
