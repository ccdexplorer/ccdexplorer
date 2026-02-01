from __future__ import annotations

from ccdexplorer.domain.credential import Credentials
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from ccdexplorer.grpc_client.CCD_Types import (
        CCD_AccountInfo,
        CCD_Policy,
    )


class NET(Enum):
    MAINNET = "mainnet"
    TESTNET = "testnet"


class StandardIdentifiers(Enum):
    """
    Enum class representing standard identifiers for CIS (Common Identifier System).

    Attributes:
        CIS_0 (str): Represents the identifier "CIS-0".
        CIS_1 (str): Represents the identifier "CIS-1".
        CIS_2 (str): Represents the identifier "CIS-2".
        CIS_3 (str): Represents the identifier "CIS-3".
        CIS_4 (str): Represents the identifier "CIS-4".
        CIS_5 (str): Represents the identifier "CIS-5".
        CIS_6 (str): Represents the identifier "CIS-6".
    """

    CIS_0 = "CIS-0"
    CIS_1 = "CIS-1"
    CIS_2 = "CIS-2"
    CIS_3 = "CIS-3"
    CIS_4 = "CIS-4"
    CIS_5 = "CIS-5"
    CIS_6 = "CIS-6"


class CredentialShort(BaseModel):
    created_at: str
    valid_to: str
    ip_id: int


class AccountInfoStable(BaseModel):
    account_address: str
    account_index: int
    account_threshold: int
    credential_count: int
    credentials: list

    @classmethod
    def from_account_info(cls, ai: CCD_AccountInfo) -> "AccountInfoStable":
        credentials = Credentials().determine_id_providers(ai.credentials)

        return cls(
            account_address=ai.address,
            account_index=ai.index,
            account_threshold=ai.threshold,
            credentials=credentials,
            credential_count=len(ai.credentials.keys()),
        )

    def to_collection(self) -> dict:
        md = self.model_dump()
        md["_id"] = md["account_address"][:29]
        return md
