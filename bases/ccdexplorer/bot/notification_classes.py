# ruff: noqa: F403, F405

from typing import Optional, Union

from pydantic import BaseModel, ConfigDict
from ccdexplorer.domain.cis import (
    burnEvent,
    mintEvent,
    tokenMetadataEvent,
    transferEvent,
)
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import *  # type: ignore
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter


class MessageResponse(BaseModel):
    title_telegram: str
    title_email: str
    message_telegram: str
    message_email: str
    event_type_str: Optional[str] = None


class CCD_LoweredStake(BaseModel):
    baker_stake_decreased: Optional[CCD_BakerStakeDecreased] = None
    baker_removed: Optional[bool] = None
    unstaked_amount: microCCD
    new_stake: microCCD
    percentage_unstaked: float


class CCD_Pool_Commission_Changed(BaseModel):
    validator_id: CCD_AccountIndex
    events: list[CCD_BakerEvent]
    delegators: Optional[list[CCD_AccountIndex]] = None


class IndexLookUp(int, Enum):
    account_index = 0
    contract_index = 1
    delegator_id = 2


class TokenEvent(BaseModel):
    result: Union[mintEvent, transferEvent, burnEvent, tokenMetadataEvent]
    token_address: str
    token_name: Optional[str] = None
    # tag_info: Optional[MongoTypeTokensTag] = None


class Connections(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    tooter: Tooter
    mongodb: MongoDB
    mongomoter: Optional[MongoMotor] = None
    grpcclient: GRPCClient


class EventTypeContract(BaseModel):
    module_deployed: Optional[CCD_ModuleRef] = None
    contract_initialized: Optional[CCD_ContractInitializedEvent] = None
    contract_update_issued: Optional[CCD_ContractUpdateIssued] = None
    # these properties are helpers to facilitate the notifications
    receive_name: Optional[str] = None


class EventTypeAccount(BaseModel):
    module_deployed: Optional[CCD_ModuleRef] = None
    contract_initialized: Optional[CCD_ContractInitializedEvent] = None
    # contract_update_issued: Optional[CCD_ContractUpdateIssued] = None
    account_transfer: Optional[CCD_AccountTransfer] = None
    encrypted_amount_transferred: Optional[
        CCD_AccountTransactionEffects_EncryptedAmountTransferred
    ] = None
    transferred_to_encrypted: Optional[CCD_EncryptedSelfAmountAddedEvent] = None
    transferred_to_public: Optional[CCD_AccountTransactionEffects_TransferredToPublic] = None
    transferred_with_schedule: Optional[CCD_TransferredWithSchedule] = None
    credential_keys_updated: Optional[CCD_CredentialRegistrationId] = None
    credentials_updated: Optional[CCD_AccountTransactionEffects_CredentialsUpdated] = None
    data_registered: Optional[CCD_RegisteredData] = None
    # note this is the account itself performing a delegation action
    delegation_configured: Optional[CCD_DelegationConfigured] = None
    # if the account is delegating, get notified if your validator configures
    baker_configured: Optional[CCD_BakerConfigured] = None
    validator_commission_changed: Optional[CCD_Pool_Commission_Changed] = None
    # Note this contains, if available, the delegator info when event_type == delegation_configured
    previous_block_validator_info: Optional[CCD_AccountStakingInfo_Baker] = None
    previous_block_account_info: Optional[CCD_AccountStakingInfo_Delegator] = None
    payday_account_reward: Optional[CCD_BlockSpecialEvent_PaydayAccountReward] = None
    token_event: Optional[TokenEvent] = None
    validator_suspended: Optional[int] = None
    validator_primed_for_suspension: Optional[int] = None
    token_update_effect: Optional[CCD_TokenEvent] = None


class EventTypeValidator(BaseModel):
    # these events are ready for notification
    block_validated: Optional[bool] = None
    block_baked_by_baker: Optional[bool] = None
    payday_pool_reward: Optional[CCD_BlockSpecialEvent_PaydayPoolReward] = None

    validator_configured: Optional[CCD_BakerConfigured] = None
    baker_configured: Optional[CCD_BakerConfigured] = None
    payday_pool_reward: Optional[CCD_BlockSpecialEvent_PaydayPoolReward] = None
    # note this is activated when the validator was/is/becomes the target.
    delegation_configured: Optional[CCD_DelegationConfigured] = None
    # this is the amount of blocks its running behind
    validator_running_behind: Optional[int] = None
    # these properties are helpers to facilitate the notifications
    corresponding_account_reward: Optional[CCD_BlockSpecialEvent_PaydayAccountReward] = None
    pool_info: Optional[CCD_PoolInfo] = None
    previous_block_validator_info: Optional[CCD_AccountStakingInfo_Baker] = None
    previous_block_account_info: Optional[CCD_AccountStakingInfo_Delegator] = None
    current_block_pool_info: Optional[CCD_PoolInfo] = None

    earliest_win_time: Optional[dt.datetime] = None
    validator_suspended: Optional[int] = None
    validator_primed_for_suspension: Optional[int] = None
    validator_missed_rounds: Optional[dict] = None  # {"current_missed_rounds": missed_rounds}


class EventTypeOther(BaseModel):
    validator_lowered_stake: Optional[CCD_LoweredStake] = None
    protocol_update: Optional[CCD_ProtocolUpdate] = None
    add_anonymity_revoker_update: Optional[CCD_ArInfo] = None
    add_identity_provider_update: Optional[CCD_IpInfo] = None
    # used for lowered stake and commission changed
    baker_configured: Optional[CCD_BakerConfigured] = None
    account_transfer: Optional[CCD_AccountTransfer] = None
    transferred_with_schedule: Optional[CCD_TransferredWithSchedule] = None
    domain_name_minted: Optional[str] = None
    account_created: Optional[CCD_AccountAddress] = None
    validator_commission_changed: Optional[CCD_Pool_Commission_Changed] = None
    validator_suspended: Optional[int] = None
    validator_primed_for_suspension: Optional[int] = None
    validator_node_version_upgrade: Optional[str] = None
    token_creation: Optional[CCD_CreatePLT] = None

    # Note these are also listed under EventTypeAccount.
    # There they are events for the account itself.
    # Here it's events in general.
    module_deployed: Optional[CCD_ModuleRef] = None
    contract_initialized: Optional[CCD_ContractInitializedEvent] = None
    # these properties are helpers to facilitate the notifications
    previous_block_validator_info: Optional[CCD_AccountStakingInfo_Baker] = None


class EventType(BaseModel):
    account: Optional[EventTypeAccount] = None
    validator: Optional[EventTypeValidator] = None
    contract: Optional[EventTypeContract] = None
    other: Optional[EventTypeOther] = None


class AddressType(str, Enum):
    account = "Account"
    sender = "Sender"
    receiver = "Receiver"
    delegator = "Delegator"
    validator = "Validator"
    contract = "Contract"


class CCD_AccountAddress_Complete(BaseModel):
    id: Optional[CCD_AccountAddress] = None
    index: Optional[CCD_AccountIndex] = None


class CCD_Address_Complete(BaseModel):
    account: Optional[CCD_AccountAddress_Complete] = None
    contract: Optional[CCD_ContractAddress] = None


class ImpactedAddress(BaseModel):
    label: Optional[str] = None
    address: Optional[CCD_Address_Complete] = None
    address_type: Optional[AddressType] = None


class NotificationEvent(BaseModel):
    event_type: EventType
    block_height: Optional[int] = None
    block_hash: Optional[str] = None
    block_slot_time: Optional[dt.datetime] = None
    tx_hash: Optional[str] = None
    impacted_addresses: Optional[list[ImpactedAddress]] = None
