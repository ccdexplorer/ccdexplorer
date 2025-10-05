from unittest.mock import MagicMock, patch

import httpx
import pytest
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.ms_metadata.subscriber import Subscriber
from ccdexplorer.tooter.core import Tooter


@pytest.mark.asyncio
async def test_metadata_can_read_status_200(
    grpcclient: GRPCClient,
    tooter: Tooter,
    motormongo: MongoMotor,
    mongodb: MongoDB,
    httpx_client: httpx.Client,
):
    """Test fetching and storing token metadata, fully mocked (so no external influences ever)"""
    mock_replace_one = MagicMock()
    mock_collection = MagicMock()
    mock_collection.replace_one = mock_replace_one
    mock_collection.find_one.return_value = {
        "_id": "<9403,0>-da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831",
        "contract": "<9403,0>",
        "token_id": "da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831",
        "token_amount": "0",
        "metadata_url": "https://nft.ptags.io/DA79C2405F888E6511CDF0E16149902A7AA8EE21368B55300A7682CDBA82D831",
        "last_height_processed": 36841773,
        "hidden": False,
    }

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "name": "Traveler Leather Logo",
        "unique": True,
        "description": "Traveler Leather Logo",
        "attributes": [
            {
                "type": "string",
                "name": "nfc_id",
                "value": "DA79C2405F888E6511CDF0E16149902A7AA8EE21368B55300A7682CDBA82D831",
            }
        ],
        "display": {
            "url": "https://storage.googleapis.com/provenance_images/307_793ff9b8-159d-41f8-b26b-dcc970160784.png",
            "hash": None,
        },
        "thumbnail": {
            "url": "https://storage.googleapis.com/provenance_images/307_793ff9b8-159d-41f8-b26b-dcc970160784.png",
            "hash": None,
        },
    }

    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {Collections.tokens_token_addresses_v2: mock_collection},
            clear=False,
        ),
        patch.object(httpx_client, "get", return_value=mock_resp) as mock_get,
    ):
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
        await subscriber.init_sessions()
        net = NET.MAINNET
        token_address: str = (
            "<9403,0>-da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831"
        )
        subscriber.fetch_token_metadata(NET(net), token_address, httpx_client)
        mock_replace_one.assert_called_once()
        mock_get.assert_called_once_with(mock_collection.find_one.return_value["metadata_url"])
        _, kwargs = mock_replace_one.call_args
        replacement = kwargs["replacement"]
        token_metadata = replacement["token_metadata"]
        assert "failed_attempt" not in replacement
        assert token_metadata == {
            "name": "Traveler Leather Logo",
            "unique": True,
            "description": "Traveler Leather Logo",
            "thumbnail": {
                "url": "https://storage.googleapis.com/provenance_images/307_793ff9b8-159d-41f8-b26b-dcc970160784.png"
            },
            "display": {
                "url": "https://storage.googleapis.com/provenance_images/307_793ff9b8-159d-41f8-b26b-dcc970160784.png"
            },
            "attributes": [
                {
                    "type": "string",
                    "name": "nfc_id",
                    "value": "DA79C2405F888E6511CDF0E16149902A7AA8EE21368B55300A7682CDBA82D831",
                }
            ],
        }


@pytest.mark.asyncio
async def test_metadata_fail_to_read_status_404(
    grpcclient: GRPCClient,
    tooter: Tooter,
    motormongo: MongoMotor,
    mongodb: MongoDB,
    httpx_client: httpx.Client,
):
    """Test fetching and storing token metadata, fully mocked (so no external influences ever)"""
    mock_replace_one = MagicMock()
    mock_collection = MagicMock()
    mock_collection.replace_one = mock_replace_one
    mock_collection.find_one.return_value = {
        "_id": "<9403,0>-da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831",
        "contract": "<9403,0>",
        "token_id": "da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831",
        "token_amount": "0",
        "metadata_url": "https://nft.ptags.io/DA79C2405F888E6511CDF0E16149902A7AA8EE21368B55300A7682CDBA82D831",
        "last_height_processed": 36841773,
        "hidden": False,
    }

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.status_code = 404
    mock_resp.json.return_value = None

    with (
        patch.object(tooter, "send_to_tooter") as _,
        patch.dict(
            mongodb.mainnet,
            {Collections.tokens_token_addresses_v2: mock_collection},
            clear=False,
        ),
        patch.object(httpx_client, "get", return_value=mock_resp) as mock_get,
    ):
        subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
        await subscriber.init_sessions()
        net = NET.MAINNET
        token_address: str = (
            "<9403,0>-da79c2405f888e6511cdf0e16149902a7aa8ee21368b55300a7682cdba82d831"
        )
        subscriber.fetch_token_metadata(NET(net), token_address, httpx_client)
        mock_replace_one.assert_called_once()
        mock_get.assert_called_once_with(mock_collection.find_one.return_value["metadata_url"])
        _, kwargs = mock_replace_one.call_args
        replacement = kwargs["replacement"]
        assert "token_metadata" not in replacement
        assert "failed_attempt" in replacement
