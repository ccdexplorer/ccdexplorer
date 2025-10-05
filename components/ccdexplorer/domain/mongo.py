from __future__ import annotations
import datetime as dt
from enum import Enum
from typing import Any, Optional, Union
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountIndex,
    CCD_InstanceInfo_V0,
    CCD_InstanceInfo_V1,
    CCD_PoolInfo,
    CCD_TokenId,
    CCD_TokenSupplyUpdateEvent,
    CCD_TokenTransferEvent,
    microCCD,
)
from ccdexplorer.domain.cis import (
    mintEvent,
    transferEvent,
    burnEvent,
    updateOperatorEvent,
    registerCredentialEvent,
    revokeCredentialEvent,
    issuerMetadataEvent,
    credentialMetadataEvent,
    tokenMetadataEvent,
    credentialSchemaRefEvent,
    revocationKeyEvent,
    itemCreatedEvent,
    itemStatusChangedEvent,
    nonceEventCIS3,
    nonceEventCIS5,
    depositCCDEvent,
    depositCIS2TokensEvent,
    transferCCDEvent,
    transferCIS2TokensEvent,
    withdrawCCDEvent,
    withdrawCIS2TokensEvent,
    fiveStarsRegisterAccessEvent,
)
from pydantic import BaseModel, ConfigDict, Field


class TokenAttribute(BaseModel):
    """
    TokenAttribute is a data model representing an attribute of a token.

    Attributes:
        type (Optional[str]): The type of the token attribute.
        name (Optional[str]): The name of the token attribute.
        value (Optional[str]): The value of the token attribute.

    Config:
        model_config (ConfigDict): Configuration for the model, with coercion of numbers to strings enabled.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)
    type: Optional[str] = None
    name: Optional[str] = None
    value: Optional[str] = None


class TokenURLJSON(BaseModel):
    """
    TokenURLJSON is a data model representing a JSON object with a URL and a hash.

    Attributes:
        url (Optional[str]): The URL as a string. Defaults to None.
        hash (Optional[str]): The hash value as a string. Defaults to None.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)
    url: Optional[str] = None
    hash: Optional[str] = None


class TokenMetaData(BaseModel):
    """
    TokenMetaData is a model representing metadata for a token.

    Attributes:
        name (Optional[str]): The name of the token.
        symbol (Optional[str]): The symbol of the token.
        unique (Optional[bool]): Indicates if the token is unique.
        decimals (Optional[int]): The number of decimal places for the token.
        description (Optional[str]): A description of the token.
        thumbnail (Optional[TokenURLJSON]): A URL to the token's thumbnail image.
        display (Optional[TokenURLJSON]): A URL to the token's display image.
        artifact (Optional[TokenURLJSON]): A URL to the token's artifact.
        assets (Optional[list[TokenMetaData]]): A list of associated token metadata.
        attributes (Optional[list[TokenAttribute]]): A list of attributes for the token.
        localization (Optional[dict[str, TokenURLJSON]]): A dictionary for localization with language codes as keys and TokenURLJSON as values.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)
    name: Optional[str] = None
    symbol: Optional[str] = None
    unique: Optional[bool] = None
    decimals: Optional[int] = None
    description: Optional[str] = None
    thumbnail: Optional[TokenURLJSON] = None
    display: Optional[TokenURLJSON] = None
    artifact: Optional[TokenURLJSON] = None
    assets: Optional[list[TokenMetaData]] = None
    attributes: Optional[list[TokenAttribute]] = None
    localization: Optional[dict[str, TokenURLJSON]] = None


class LoggedEvents(Enum):
    transfer_event = 255
    mint_event = 254
    burn_event = 253
    operator_event = 252
    metadata_event = 251
    nonce_event = 250
    register_credential_event = 249
    revoke_credential_event = 248
    issuer_metadata_event = 247
    credential_metadata_event = 246
    credential_schemaref_event = 245
    recovation_key_event = 244
    item_created_event = 237
    item_status_changed = 236


class LEEventInfo(BaseModel):
    """
    LEEventInfo is a model representing information about a logged event.

    Attributes:
        contract (Optional[str]): The contract associated with the event.
        standard (Optional[str]): The standard associated with the event.
        logged_event (str): The name or identifier of the logged event.
        effect_index (int): The index of the effect within the event.
        event_index (int): The index of the event.
        event_type (Optional[str]): The type of the event.
        token_address (Optional[str]): The address of the token involved in the event.
    """

    contract: Optional[str] = None
    standard: Optional[str] = None
    logged_event: str
    effect_index: int
    event_index: int
    event_type: Optional[str] = None
    token_address: Optional[str] = None


class LETxInfo(BaseModel):
    """
    LETxInfo is a data model representing transaction information.

    Attributes:
        date (str): The date of the transaction.
        tx_hash (str): The hash of the transaction.
        tx_index (int): The index of the transaction.
        block_height (int): The height of the block containing the transaction.
    """

    date: str
    tx_hash: str
    tx_index: int
    block_height: int


class MongoTypeLoggedEventV2(BaseModel):
    """
    MongoTypeLoggedEventV2 is a Pydantic model representing a logged event in MongoDB.

    Attributes:
        id (str): The unique identifier for the event, aliased as "_id".
        event_info (LEEventInfo): Information about the logged event.
        tx_info (LETxInfo): Transaction information related to the event.
        recognized_event (Optional[Union[
            mintEvent, transferEvent, burnEvent, updateOperatorEvent, tokenMetadataEvent,
            registerCredentialEvent, revokeCredentialEvent, issuerMetadataEvent,
            credentialMetadataEvent, credentialSchemaRefEvent, revocationKeyEvent,
            itemCreatedEvent, itemStatusChangedEvent, nonceEvent, depositCCDEvent,
            depositCIS2TokensEvent, transferCCDEvent, transferCIS2TokensEvent,
            withdrawCCDEvent, withdrawCIS2TokensEvent, fiveStarsRegisterAccessEvent
        ]]): The recognized event type, which can be one of several event types, or None.
        to_address_canonical (Optional[str]): The canonical address to which the event is related, if applicable.
        from_address_canonical (Optional[str]): The canonical address from which the event is related, if applicable.
    """

    id: str = Field(..., alias="_id")
    event_info: LEEventInfo
    tx_info: LETxInfo
    recognized_event: Optional[
        mintEvent
        | transferEvent
        | burnEvent
        | updateOperatorEvent
        | tokenMetadataEvent
        | registerCredentialEvent
        | revokeCredentialEvent
        | issuerMetadataEvent
        | credentialMetadataEvent
        | credentialSchemaRefEvent
        | revocationKeyEvent
        | itemCreatedEvent
        | itemStatusChangedEvent
        | nonceEventCIS3
        | nonceEventCIS5
        | depositCCDEvent
        | depositCIS2TokensEvent
        | transferCCDEvent
        | transferCIS2TokensEvent
        | withdrawCCDEvent
        | withdrawCIS2TokensEvent
        | fiveStarsRegisterAccessEvent
    ] = None
    to_address_canonical: Optional[str] = None
    from_address_canonical: Optional[str] = None


class MongoTypeTokenForAddress(BaseModel):
    """
    MongoTypeTokenForAddress is a data model representing a token for an address in a MongoDB database.

    Attributes:
        token_address (str): The address of the token.
        contract (str): The contract associated with the token.
        token_id (str): The unique identifier of the token.
        token_amount (str): The amount of the token.
    """

    model_config = ConfigDict(coerce_numbers_to_str=True)
    token_address: str
    contract: str
    token_id: str
    token_amount: str


class MongoTypeTokenLink(BaseModel):
    """
    MongoTypeTokenLink is a model representing a link between a token and an account in a MongoDB database.

    Attributes:
        id (str): The unique identifier for the token link, mapped to the MongoDB "_id" field.
        account_address (Optional[str]): The address of the account holding the token.
        account_address_canonical (Optional[str]): The canonical form of the account address.
        token_holding (Optional[MongoTypeTokenForAddress]): The token holding details for the account.
    """

    id: str = Field(..., alias="_id")
    account_address: Optional[str] = None
    account_address_canonical: Optional[str] = None
    token_holding: Optional[MongoTypeTokenForAddress] = None


class MongoTypeTokenHolderAddress(BaseModel):
    """
    MongoTypeTokenHolderAddress is a model representing a token holder's address in a MongoDB collection.

    Attributes:
        id (str): The unique identifier for the token holder, mapped from the MongoDB "_id" field.
        account_address_canonical (Optional[str]): The canonical form of the account address. Defaults to None.
        tokens (dict[str, MongoTypeTokenForAddress]): A dictionary mapping token identifiers to their corresponding MongoTypeTokenForAddress objects.
    """

    id: str = Field(..., alias="_id")
    account_address_canonical: Optional[str] = None
    tokens: dict[str, MongoTypeTokenForAddress]


class MongoTypeLoggedEvent(BaseModel):
    """
    MongoTypeLoggedEvent represents a logged event in a MongoDB collection.

    Attributes:
        id (str): The unique identifier for the event, mapped from the MongoDB "_id" field.
        logged_event (str): The name or description of the logged event.
        result (dict): The result or payload of the logged event.
        tag (int): A tag associated with the event.
        event_type (str): The type or category of the event.
        block_height (int): The block height at which the event occurred.
        slot_time (Optional[dt.datetime]): The slot time when the event was logged, if available.
        tx_index (int): The transaction index within the block.
        ordering (int): The ordering of the event.
        tx_hash (str): The transaction hash associated with the event.
        token_address (str): The address of the token involved in the event.
        contract (str): The contract address related to the event.
        date (Optional[str]): The date when the event was logged, if available.
        to_address_canonical (Optional[str]): The canonical form of the recipient address, if available.
        from_address_canonical (Optional[str]): The canonical form of the sender address, if available.
    """

    id: str = Field(..., alias="_id")
    logged_event: str
    result: dict | Any
    tag: int
    event_type: str
    block_height: int
    slot_time: Optional[dt.datetime] = None
    tx_index: int  #####################################################################
    ordering: int
    tx_hash: str
    token_address: str
    contract: str
    date: Optional[str] = None
    to_address_canonical: Optional[str] = None
    from_address_canonical: Optional[str] = None


class MongoTypeTokensTag(BaseModel):
    """
    MongoTypeTokensTag is a Pydantic model representing a token tag in a MongoDB collection.

    Attributes:
        id (str): The unique identifier for the token tag, mapped from the MongoDB "_id" field.
        contracts (list[str]): A list of contract addresses associated with the token tag.
        tag_template (Optional[bool]): Indicates if the tag is a template.
        single_use_contract (Optional[bool]): Indicates if the contract is for single use.
        logo_url (Optional[str]): URL to the logo image of the token.
        decimals (Optional[int]): Number of decimal places for the token.
        exchange_rate (Optional[float]): Exchange rate of the token.
        get_price_from (Optional[str]): Source from which the price of the token is obtained.
        logged_events_count (Optional[int]): Number of logged events associated with the token.
        owner (Optional[str]): Owner of the token.
        module_name (Optional[str]): Name of the module associated with the token.
        token_type (Optional[str]): Type of the token.
        display_name (Optional[str]): Display name of the token.
        tvl_for_token_in_usd (Optional[float]): Total value locked for the token in USD.
        token_tag_id (Optional[str]): Identifier for the token tag.
    """

    id: str = Field(..., alias="_id")
    contracts: list[str]
    tag_template: Optional[bool] = None
    single_use_contract: Optional[bool] = None
    logo_url: Optional[str] = None
    decimals: Optional[int] = None
    exchange_rate: Optional[float] = None
    get_price_from: Optional[str] = None
    logged_events_count: Optional[int] = None
    owner: Optional[str] = None
    module_name: Optional[str] = None
    token_type: Optional[str] = None
    display_name: Optional[str] = None
    tvl_for_token_in_usd: Optional[float] = None
    token_tag_id: Optional[str] = None


class FailedAttempt(BaseModel):
    """
    A model representing a failed attempt to download and parse metadata.

    Attributes:
        attempts (int): The number of attempts made.
        do_not_try_before (datetime): The datetime before which no further attempts should be made.
        last_error (str): The error message from the last attempt.
    """

    attempts: int
    do_not_try_before: dt.datetime
    last_error: str


class MongoTypeTokenAddress(BaseModel):
    """
    MongoTypeTokenAddress represents the structure of a token address document stored in MongoDB.

    Attributes:
        id (str): The unique identifier for the token address, mapped from the MongoDB "_id" field.
        contract (str): The contract address associated with the token.
        token_id (str): The unique identifier for the token.
        token_amount (Optional[str]): The amount of tokens, if available.
        metadata_url (Optional[str]): The URL to the token's metadata, if available.
        last_height_processed (int): The last blockchain height that was processed for this token.
        token_holders (Optional[dict[str, str]]): A dictionary mapping token holder addresses to their respective amounts, if available.
        tag_information (Optional[MongoTypeTokensTag]): Additional tag information related to the token, if available.
        exchange_rate (Optional[float]): The exchange rate of the token, if available.
        domain_name (Optional[str]): The domain name associated with the token, if available.
        token_metadata (Optional[TokenMetaData]): Metadata related to the token, if available.
        failed_attempt (Optional[FailedAttempt]): Information about any failed attempts related to the token, if available.
        hidden (Optional[bool]): A flag indicating whether the token is hidden.
    """

    id: str = Field(..., alias="_id")
    contract: str
    token_id: str
    token_amount: Optional[str] = None
    metadata_url: Optional[str] = None
    last_height_processed: int
    token_holders: Optional[dict[str, str]] = None
    tag_information: Optional[MongoTypeTokensTag] = None
    exchange_rate: Optional[float] = None
    domain_name: Optional[str] = None
    token_metadata: Optional[TokenMetaData] = None
    failed_attempt: Optional[FailedAttempt] = None
    hidden: Optional[bool] = None


class MongoTypeTokenAddressV2(BaseModel):
    """
    MongoTypeTokenAddressV2 represents a MongoDB document model for token addresses.

    Attributes:
        id (str): The unique identifier for the document, mapped to the MongoDB "_id" field.
        contract (str): The contract address associated with the token.
        token_id (str): The unique identifier for the token.
        token_amount (Optional[str]): The amount of the token, if applicable.
        metadata_url (Optional[str]): The URL to the token's metadata.
        last_height_processed (int): The last blockchain height that was processed for this token.
        tag_information (Optional[MongoTypeTokensTag]): Additional tag information related to the token.
        exchange_rate (Optional[float]): The exchange rate of the token.
        token_metadata (Optional[TokenMetaData]): Metadata associated with the token.
        failed_attempt (Optional[FailedAttempt]): Information about any failed attempts related to the token.
        hidden (Optional[bool]): Indicates whether the token is hidden.
    """

    id: str = Field(..., alias="_id")
    contract: str
    token_id: str
    token_amount: Optional[str] = None
    metadata_url: Optional[str] = None
    last_height_processed: int
    tag_information: Optional[MongoTypeTokensTag] = None
    exchange_rate: Optional[float] = None
    # domain_name: Optional[str] = None
    token_metadata: Optional[TokenMetaData] = None
    failed_attempt: Optional[FailedAttempt] = None
    hidden: Optional[bool] = None


class MongoLabeledAccount(BaseModel):
    """
    MongoLabeledAccount represents a labeled account in a MongoDB collection.

    Attributes:
        id (str): The unique identifier for the account, mapped from the MongoDB "_id" field.
        account_index (Optional[CCD_AccountIndex]): The index of the account, if available.
        label (str): The label associated with the account.
        label_group (str): The group to which the label belongs.
    """

    id: str = Field(..., alias="_id")
    account_index: Optional[CCD_AccountIndex] = None
    label: str
    label_group: str


class AccountStatementEntry(BaseModel):
    """
    AccountStatementEntry represents an entry in an account statement.

    Attributes:
        block_height (int): The height of the block in the blockchain.
        slot_time (dt.datetime): The timestamp of the slot.
        entry_type (str): The type of the entry (e.g., transaction, reward).
        amount (microCCD): The amount involved in the entry.
        balance (microCCD): The balance after the entry.
    """

    block_height: int
    slot_time: dt.datetime
    entry_type: str
    amount: microCCD
    balance: microCCD


class AccountStatementTransferType(BaseModel):
    """
    AccountStatementTransferType represents a transfer type in an account statement.

    Attributes:
        amount (microCCD): The amount of the transfer in microCCD.
        counterparty (str): The counterparty involved in the transfer.
    """

    amount: microCCD
    counterparty: str


class PLTTransferType(BaseModel):
    """
    PLTTransferType represents a transfer type in an account statement.

    """

    event: CCD_TokenSupplyUpdateEvent | CCD_TokenTransferEvent
    token_id: CCD_TokenId


class AccountStatementEntryType(BaseModel):
    """
    AccountStatementEntryType represents the structure of an account statement entry.

    Attributes:
        amount_decrypted (Optional[microCCD]): The decrypted amount in the account statement entry.
        amount_encrypted (Optional[microCCD]): The encrypted amount in the account statement entry.
        baker_reward (Optional[microCCD]): The reward received from baking.
        finalization_reward (Optional[microCCD]): The reward received from finalization.
        foundation_reward (Optional[microCCD]): The reward received from the foundation.
        transaction_fee (Optional[microCCD]): The transaction fee associated with the account statement entry.
        transaction_fee_reward (Optional[microCCD]): The reward received from transaction fees.
        transfer_in (Optional[list[AccountStatementTransferType]]): List of incoming transfers.
        transfer_out (Optional[list[AccountStatementTransferType]]): List of outgoing transfers.
    """

    amount_decrypted: Optional[microCCD] = None
    amount_encrypted: Optional[microCCD] = None
    baker_reward: Optional[microCCD] = None
    finalization_reward: Optional[microCCD] = None
    foundation_reward: Optional[microCCD] = None
    transaction_fee: Optional[microCCD] = None
    transaction_fee_reward: Optional[microCCD] = None
    transfer_in: Optional[list[AccountStatementTransferType]] = None
    transfer_out: Optional[list[AccountStatementTransferType]] = None
    plt_transfer_in: Optional[list[PLTTransferType]] = None
    plt_transfer_out: Optional[list[PLTTransferType]] = None


class MongoImpactedAddress(BaseModel):
    """
    MongoImpactedAddress is a data model representing an impacted address in a MongoDB collection.

    Attributes:
        id (str): The unique identifier for the impacted address, mapped from the MongoDB "_id" field.
        tx_hash (Optional[str]): The transaction hash associated with the impacted address. Rewards do not have a transaction hash.
        impacted_address (str): The impacted address.
        impacted_address_canonical (str): The canonical form of the impacted address.
        effect_type (str): The type of effect on the impacted address.
        balance_movement (Optional[AccountStatementEntryType]): The balance movement associated with the impacted address.
        block_height (int): The block height at which the impact occurred.
        included_in_flow (Optional[bool]): Indicates whether the impacted address is included in the flow.
        date (Optional[str]): The date when the impact occurred.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: str = Field(..., alias="_id")
    tx_hash: Optional[str] = None  # Rewards do not have a tx
    impacted_address: str
    impacted_address_canonical: str
    effect_type: str
    balance_movement: Optional[AccountStatementEntryType] = None
    block_height: int
    included_in_flow: Optional[bool] = None
    date: Optional[str] = None
    plt_token_id: Optional[str] = None


class MongoTokensImpactedAddress(BaseModel):
    """
    MongoTokensImpactedAddress is a Pydantic model representing a MongoDB document for tokens impacted by an address.

    Attributes:
        id (str): The unique identifier for the document, mapped from MongoDB's "_id" field.
        tx_hash (str): The transaction hash associated with the impacted address.
        impacted_address (str): The address that was impacted.
        impacted_address_canonical (str): The canonical form of the impacted address.
        event_type (str): The type of event that impacted the address.
        token_address (Optional[str]): The address of the token, if applicable.
        plt_token_id (Optional[str]): The ID of the PLT token, if applicable.
        contract (Optional[str]): The contract associated with the token, if applicable.
        block_height (int): The block height at which the event occurred.
        date (str): The date when the event occurred.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: str = Field(..., alias="_id")
    tx_hash: str
    impacted_address: str
    impacted_address_canonical: str
    event_type: str
    token_address: Optional[str] = None
    plt_token_id: Optional[str] = None
    contract: Optional[str] = None
    block_height: int
    date: str


class MongoTypeBlockPerDay(BaseModel):
    """
    Block Per Day. This type is stored in the collection `blocks_per_day`.

    :Parameters:
    - `_id`: the date of the day that ended
    - `date`: the date of the day that ended
    - `height_for_first_block`: height of the first block in the day
    - `height_for_last_block`: height of the last block in the day
    - `slot_time_for_first_block`: time of the first block in the day
    - `slot_time_for_last_block`: time of the last block in the day
    - `hash_for_first_block`: hash of the first block in the day
    - `hash_for_last_block`: hash of the last block in the day

    """

    id: str = Field(..., alias="_id")
    date: str
    height_for_first_block: int
    height_for_last_block: int
    slot_time_for_first_block: dt.datetime
    slot_time_for_last_block: dt.datetime
    hash_for_first_block: str
    hash_for_last_block: str


class MongoTypeInvolvedAccount(BaseModel):
    """
    Involved Account. This type is stored in the collections `involved_accounts_all` and
    `involved_accounts_transfer`.

    :Parameters:
    - `_id`: the hash of the transaction
    - `sender`: the sender account address
    - `receiver`: the receiver account address, might be null
    - `sender_canonical`: the canonical sender account address
    - `receiver_canonical`: the  canonical receiver account address, might be null
    - `amount`: amount of the transaction, might be null
    - `type`: dict with transaction `type` and `contents`
    - `block_height`: height of the block in which the transaction is executed
    """

    id: str = Field(..., alias="_id")
    sender: str
    receiver: Optional[str] = None
    sender_canonical: str
    receiver_canonical: Optional[str] = None
    amount: Optional[int] = None
    type: dict[str, str]
    block_height: int
    memo: Optional[str] = None


class MongoTypeInvolvedContract(BaseModel):
    """
    Involved Contract. This type is stored in the collection `involved_contracts`.

    :Parameters:
    - `_id`: the hash of the transaction - the `str` representation of the contract.
    - `index`: contract index
    - `subindex`: contract subindex
    - `contract`: the `str` representation of the contract
    - `type`: dict with transaction `type` and `contents`
    - `block_height`: height of the block in which the transaction is executed
    - `source_module`: hash of the source module from which this contract is instanciated.
    """

    id: str = Field(..., alias="_id")
    index: int
    subindex: int
    contract: str
    type: dict[str, str]
    block_height: int
    source_module: str


class ModuleVerification(BaseModel):
    """
    ModuleVerification represents the verification details of a module.

    Attributes:
        verified (Optional[bool]): Indicates if the module is verified. Defaults to False.
        verification_status (str): The status of the verification process.
        verification_timestamp (Optional[dt.datetime]): The timestamp when the verification was performed. Defaults to None.
        explanation (Optional[str]): An optional explanation of the verification status. Defaults to None.
        build_image_used (Optional[str]): The build image used during verification. Defaults to None.
        build_command_used (Optional[str]): The build command used during verification. Defaults to None.
        archive_hash (Optional[str]): The hash of the archive used during verification. Defaults to None.
        link_to_source_code (Optional[str]): A link to the source code of the module. Defaults to None.
        source_code_at_verification_time (Optional[str]): The source code at the time of verification. Defaults to None.
    """

    verified: Optional[bool] = False
    verification_status: str
    verification_timestamp: Optional[dt.datetime] = None
    explanation: Optional[str] = None
    build_image_used: Optional[str] = None
    build_command_used: Optional[str] = None
    archive_hash: Optional[str] = None
    link_to_source_code: Optional[str] = None
    source_code_at_verification_time: Optional[str] = None


class MongoTypeModule(BaseModel):
    """
    MongoTypeModule represents a module stored in the `modules` collection.

    Attributes:
        id (str): The hex string identifier for the module, stored as `_id` in the database.
        module_name (str): The name of the module.
        methods (Optional[list[str]]): A list of method names associated with the module.
        contracts (Optional[list[str]]): A list of contract instances from this module.
        init_date (Optional[dt.datetime]): The initialization date of the module.
        verification (Optional[ModuleVerification]): The verification details of the module.
    """

    id: str = Field(..., alias="_id")
    module_name: Optional[str] = None
    methods: Optional[list[str]] = None
    contracts: Optional[list[str]] = None
    init_date: Optional[dt.datetime] = None
    verification: Optional[ModuleVerification] = None


class MongoTypeInstance(BaseModel):
    """
    Instance. This type is stored in the collection `instances`.

    :Parameters:
    - `_id`: the hex string
    - `module_name`: the name from the module
    - `methods`: list of method names
    - `contracts`: list of contract instances from this module
    """

    id: str = Field(..., alias="_id")
    v0: Optional[CCD_InstanceInfo_V0] = None  # noqa: F405
    v1: Optional[CCD_InstanceInfo_V1] = None  # noqa: F405
    source_module: Optional[str] = None
    module_verification: Optional[ModuleVerification] = None


class MongoTypeReward(BaseModel):
    """
    Module. This type is stored in the collection `payday_rewards`, property `reward`.

    """

    pool_owner: Optional[Union[int, str]] = None
    account_id: Optional[str] = None
    transaction_fees: int
    baker_reward: int
    finalization_reward: int


class MongoTypePoolReward(BaseModel):
    """
    Module. This type is stored in the collection `payday_rewards`.

    :Parameters:
    - `_id`: the hex string

    """

    id: str = Field(..., alias="_id")
    pool_owner: Union[int, str]
    pool_status: dict
    reward: MongoTypeReward
    date: str


class MongoTypeAccountReward(BaseModel):
    """
    Module. This type is stored in the collection `payday_rewards`.

    :Parameters:
    - `_id`: the hex string
    - `module_name`: the name from the module
    - `methods`: list of method names
    - `contracts`: list of contract instances from this module
    """

    id: str = Field(..., alias="_id")
    account_id: str
    staked_amount: int
    account_is_baker: Optional[bool] = None
    baker_id: Optional[int] = None
    reward: MongoTypeReward
    date: str


class Delegator(BaseModel):
    account: str
    stake: int


class MongoTypePayday(BaseModel):
    """
    Payday. This type is stored in collection `paydays`.

    :Parameters:
    - `_id`: hash of the block that contains payday information for
    this payday.
    - `date`: the payday date
    - `height_for_first_block`: height of the first block in the payday
    - `height_for_last_block`: height of the last block in the payday
    - `hash_for_first_block`: hash of the first block in the payday
    - `hash_for_last_block`: hash of the last block in the payday
    - `payday_duration_in_seconds`: duration of the payday in seconds (used for
    APY calculation)
    - `payday_block_slot_time`: time of payday reward block
    - `bakers_with_delegation_information`: bakers with delegators for reward period, retrieved
    from `get_delegators_for_pool_in_reward_period`, using the hash of the last block
    - `baker_account_ids`: mapping from baker_id to account_address
    - `pool_status_for_bakers`: dictionary, keyed on pool_status, value
    is a list of bakers, retrieved using the hash of the first block
    """

    id: str = Field(..., alias="_id")
    date: str
    height_for_first_block: int
    height_for_last_block: int
    hash_for_first_block: str
    hash_for_last_block: str
    payday_duration_in_seconds: float
    payday_block_slot_time: dt.datetime
    bakers_with_delegation_information: dict[str, list[Delegator]]
    baker_account_ids: dict[int, str]
    pool_status_for_bakers: Optional[dict[str, list[int]]] = None


class MongoTypePaydayV2(BaseModel):
    """
    Payday. This type is stored in collection `paydays_v2`.

    :Parameters:
    - `_id`: hash of the block that contains payday information for
    this payday.
    - `date`: the payday date
    - `height_for_first_block`: height of the first block in the payday
    - `height_for_last_block`: height of the last block in the payday
    - `hash_for_first_block`: hash of the first block in the payday
    - `hash_for_last_block`: hash of the last block in the payday
    - `payday_duration_in_seconds`: duration of the payday in seconds (used for
    APY calculation)
    - `payday_block_slot_time`: time of payday reward block
    """

    id: str = Field(..., alias="_id")
    date: str
    height_for_first_block: Optional[int] = None
    height_for_last_block: Optional[int] = None
    hash_for_first_block: Optional[str] = None
    hash_for_last_block: Optional[str] = None
    payday_duration_in_seconds: Optional[float] = None
    payday_block_slot_time: Optional[dt.datetime] = None


class MongoTypePaydayAPYIntermediate(BaseModel):
    """
    Payday APY Intermediate. This type is stored in collection `paydays_apy_intermediate`.

    :Parameters:
    - `_id`: baker_is or account address

    """

    id: str = Field(..., alias="_id")
    daily_apy_dict: dict
    d30_apy_dict: Optional[dict] = None
    d90_apy_dict: Optional[dict] = None
    d180_apy_dict: Optional[dict] = None


class MongoTypePaydaysPerformance(BaseModel):
    """
    Payday Performance. This is a collection that stores daily performance characteristics
    for bakers.


    :Parameters:
    - `_id`: unique id in the form of `date`-`baker_id`
    - `expectation`: the daily expected number of blocks for this baker in this payday.
    Calculated as the lottery power * 8640 (the expected number of blocks in a day)
    - `payday_block_slot_time`: Slot time of the payday block
    - `baker_id`: the baker_id
    - `pool_status`:

    """

    id: str = Field(..., alias="_id")
    pool_status: CCD_PoolInfo  # noqa: F405
    expectation: float
    date: str
    payday_block_slot_time: dt.datetime
    baker_id: str
