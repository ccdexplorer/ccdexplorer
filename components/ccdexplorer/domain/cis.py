from pydantic import BaseModel
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountAddress,
    CCD_ContractAddress,
    microCCD,
    CCD_BlockItemSummary,
)
from ccdexplorer.domain.generic import StandardIdentifiers
from typing import Optional, Any
import datetime as dt
# CIS-2 Logged Event Types


class CISProcessEventRequest(BaseModel):
    """
    CISProcessEventRequest represents a request to process an event in the CIS (Contract Initialization System).

    Attributes:
        tx (CCD_BlockItemSummary): The transaction block item summary.
        event_index (int): The index of events from either contract_initialized or contract_update_issued.
        standard (Optional[StandardIdentifiers]): The standard identifier, None for unrecognized events.
        instance_address (str): The address of the instance.
        event (str): The event string.
        event_name (Optional[str]): The name of the event, None for unrecognized events.
        tag (int): The tag associated with the event.
        recognized_event (Optional[Any]): The recognized event, None for unrecognized events.
        effect_type (Optional[str]): The effect type for contract_update_issued, either interrupted or updated.
        effect_index (int): The index of the effect in the list.
    """

    tx: CCD_BlockItemSummary
    event_index: int  # this is the index of events from either contract_initialized of contract_update_issued
    standard: Optional[StandardIdentifiers] = None  # None for unrecognized events
    instance_address: str
    event: str
    event_name: Optional[str] = None  # None for unrecognized events
    tag: int
    recognized_event: Optional[Any] = None  # None for unrecognized events
    effect_type: Optional[str] = (
        None  # for contract_update_issued, these are either interrupted or updated
    )
    effect_index: int = 0  # its index in the list


class MetadataUrl(BaseModel):
    """
    MetadataUrl represents a model for storing metadata URL information.

    Attributes:
        url (str): The URL of the metadata.
        checksum (Optional[str]): An optional checksum for the metadata URL.
    """

    url: str
    checksum: Optional[str] = None


class transferEvent(BaseModel):
    """A transfer event from a CIS-2 compliant smart contract.

    See: [transferEvent](http://proposals.concordium.software/CIS/cis-2.html#transferevent)

    Attributes:
        tag (int): The event tag (255 for transfer events).
        token_id (Optional[str]): The ID of the token being transferred.
        token_amount (Optional[int]): The amount of tokens being transferred.
        from_address (Optional[str]): The address tokens are being transferred from.
        to_address (Optional[str]): The address tokens are being transferred to.
    """

    tag: int
    token_id: Optional[str] = None
    token_amount: Optional[int] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None


class mintEvent(BaseModel):
    """A mint event from a CIS-2 compliant smart contract.

    See: [mintEvent](http://proposals.concordium.software/CIS/cis-2.html#mintevent)

    Attributes:
        tag (int): The event tag (254 for mint events).
        token_id (Optional[str]): The ID of the token being minted.
        token_amount (Optional[int]): The amount of tokens being minted.
        to_address (Optional[str]): The address receiving the minted tokens.
    """

    tag: int
    token_id: Optional[str] = None
    token_amount: Optional[int] = None
    to_address: Optional[str] = None


class burnEvent(BaseModel):
    """A burn event from a CIS-2 compliant smart contract.

    See: [burnEvent](http://proposals.concordium.software/CIS/cis-2.html#burnevent)

    Attributes:
        tag (int): The event tag (253 for burn events).
        token_id (Optional[str]): The ID of the token being burned.
        token_amount (Optional[int]): The amount of tokens being burned.
        from_address (Optional[str]): The address tokens are being burned from.
    """

    tag: int
    token_id: Optional[str] = None
    token_amount: Optional[int] = None
    from_address: Optional[str] = None


class updateOperatorEvent(BaseModel):
    """An operator update event from a CIS-2 compliant smart contract.

    See: [updateOperatorEvent](http://proposals.concordium.software/CIS/cis-2.html#updateoperatorevent)

    Attributes:
        tag (int): The event tag (252 for operator update events).
        operator_update (Optional[str]): The type of update ("Add operator" or "Remove operator").
        owner (Optional[str]): The address of the token owner.
        operator (Optional[str]): The address of the operator being updated.
    """

    tag: int
    operator_update: Optional[str] = None
    owner: Optional[str] = None
    operator: Optional[str] = None


class SchemaRef(BaseModel):
    """
    SchemaRef represents a reference to a schema with an optional checksum.

    Attributes:
        url (str): The URL of the schema.
        checksum (Optional[str]): An optional checksum for the schema.
    """

    url: str
    checksum: Optional[str] = None


class registerCredentialEvent(BaseModel):
    """A register credential event from a CIS-4 compliant smart contract.

    See: [registerCredentialEvent](http://proposals.concordium.software/CIS/cis-4.html#registercredentialevent)

    Attributes:
        tag (int): The event tag (249 for register credential events).
        credential_id (Optional[str]): The unique identifier of the credential.
        schema_ref (Optional[SchemaRef]): The reference to the schema definition.
        credential_type (Optional[str]): The type of the credential being registered.
    """

    tag: int
    credential_id: Optional[str] = None
    schema_ref: Optional[SchemaRef] = None
    credential_type: Optional[str] = None


class revokeCredentialEvent(BaseModel):
    """A revoke credential event from a CIS-4 compliant smart contract.

    See: [revokeCredentialEvent](http://proposals.concordium.software/CIS/cis-4.html#revokecredentialevent)

    Attributes:
        tag (int): The event tag (248 for revoke credential events).
        credential_id (Optional[str]): The unique identifier of the credential being revoked.
        revoker (Optional[str]): The entity revoking the credential (Issuer, Holder, or Other).
        reason (Optional[str]): The reason for revoking the credential.
    """

    tag: int
    credential_id: Optional[str] = None
    revoker: Optional[str] = None
    reason: Optional[str] = None


class issuerMetadataEvent(BaseModel):
    """An issuer metadata event from a CIS-4 compliant smart contract.

    See: [issuerMetadataEvent](http://proposals.concordium.software/CIS/cis-4.html#issuermetadataevent)

    Attributes:
        tag (int): The event tag (247 for issuer metadata events).
        metadata (MetadataUrl): The URL and optional hash of the issuer's metadata.
    """

    tag: int
    metadata: MetadataUrl


class credentialMetadataEvent(BaseModel):
    """A credential metadata event from a CIS-4 compliant smart contract.

    See: [credentialMetadataEvent](http://proposals.concordium.software/CIS/cis-4.html#credentialmetadataevent)

    Attributes:
        tag (int): The event tag (246 for credential metadata events).
        id (str): The credential holder identifier.
        metadata (MetadataUrl): The URL and optional hash of the credential metadata.
    """

    tag: int
    id: str  # credentialHolderId
    metadata: MetadataUrl


class credentialSchemaRefEvent(BaseModel):
    """A credential schema reference event from a CIS-4 compliant smart contract.

    See: [credentialSchemaRefEvent](http://proposals.concordium.software/CIS/cis-4.html#credentialschemarefevent)

    Attributes:
        tag (int): The event tag (245 for credential schema reference events).
        type (Optional[str]): The type of credential this schema is for.
        schema_ref (Optional[str]): The reference to the schema definition.
    """

    tag: int
    type: Optional[str] = None
    schema_ref: Optional[str] = None


class revocationKeyEvent(BaseModel):
    """A revocation key event from a CIS-4 compliant smart contract.

    See: [revocationKeyEvent](http://proposals.concordium.software/CIS/cis-4.html#revocationkeyevent)

    Attributes:
        tag (int): The event tag (244 for revocation key events).
        public_key_ed25519 (Optional[str]): The public key being registered or removed.
        action (Optional[str]): The action being performed ("Register" or "Remove").
    """

    tag: int
    public_key_ed25519: Optional[str] = None
    action: Optional[str] = None


class tokenMetadataEvent(BaseModel):
    """A metadata event from a CIS-2 compliant smart contract.

    See: [tokenMetadataEvent](http://proposals.concordium.software/CIS/cis-2.html#tokenmetadataevent)

    Attributes:
        tag (int): The event tag (251 for metadata events).
        token_id (str): The ID of the token whose metadata is being set.
        metadata (MetadataUrl): The URL and optional hash of the token's metadata.
    """

    tag: int
    token_id: str
    metadata: MetadataUrl


class itemCreatedEvent(BaseModel):
    """An item created event from a CIS-6 compliant smart contract.

    See: [itemCreatedEvent](http://proposals.concordium.software/CIS/cis-6.html#itemcreatedevent)

    Attributes:
        tag (int): The event tag (237 for item created events).
        item_id (str): The unique identifier of the item being created.
        metadata (MetadataUrl): The URL and optional hash of the item's metadata.
        initial_status (str | int): The initial status of the created item.
    """

    tag: int
    item_id: str
    metadata: MetadataUrl
    initial_status: str | int


class itemStatusChangedEvent(BaseModel):
    """An item status change event from a CIS-6 compliant smart contract.

    See: [itemStatusChangedEvent](http://proposals.concordium.software/CIS/cis-6.html#itemstatuschangedevent)

    Attributes:
        tag (int): The event tag (236 for item status changed events).
        item_id (str): The unique identifier of the item being updated.
        new_status (str | int): The new status of the item.
        additional_data (str): Additional data associated with the status change.
    """

    tag: int
    item_id: str
    new_status: str | int
    additional_data: str


class nonceEventCIS3(BaseModel):
    """A nonce event from a CIS-3 compliant smart contract.

    See: [nonceEvent](http://proposals.concordium.software/CIS/cis-3.html#nonceevent)

    Attributes:
        tag (int): The event tag (250 for nonce events).
        nonce (Optional[str]): The generated nonce value.
        sponsoree (Optional[str]): The address of the account being sponsored.
    """

    tag: int
    nonce: Optional[str] = None
    sponsoree: Optional[str] = None


class nonceEventCIS5(BaseModel):
    """A nonce event from a CIS-5 compliant smart contract.

    See: [nonceEvent](http://proposals.concordium.software/CIS/cis-5.html#nonceevent)

    Attributes:
        tag (int): The event tag (250 for nonce events).
        nonce (Optional[str]): The generated nonce value.
        sponsoree (Optional[str]): The public key being sponsored.
    """

    tag: int
    nonce: Optional[str] = None
    sponsoree: Optional[str] = None


class depositCCDEvent(BaseModel):
    """A CCD deposit event from a CIS-5 compliant smart contract.

    See: [depositCCDEvent](http://proposals.concordium.software/CIS/cis-5.html#depositccdevent)

    Attributes:
        tag (int): The event tag (249 for deposit CCD events).
        ccd_amount (Optional[microCCD]): The amount of CCD being deposited.
        from_address (Optional[str]): The address from which CCD is being deposited.
        to_public_key_ed25519 (Optional[str]): The public key of the recipient's account.
    """

    tag: int
    ccd_amount: Optional[microCCD] = None
    from_address: Optional[str] = None
    to_public_key_ed25519: Optional[str] = None


class depositCIS2TokensEvent(BaseModel):
    """A CIS-2 token deposit event from a CIS-5 compliant smart contract.

    See: [depositCIS2TokensEvent](http://proposals.concordium.software/CIS/cis-5.html#depositcis2tokenstevent)

    Attributes:
        tag (int): The event tag (248 for deposit CIS2 tokens events).
        token_amount (Optional[int]): The amount of CIS-2 tokens being deposited.
        token_id (Optional[str]): The ID of the token being deposited.
        cis2_token_contract_address (Optional[str]): The contract address of the CIS-2 token.
        from_address (Optional[str]): The address from which tokens are being deposited.
        to_public_key_ed25519 (Optional[str]): The public key of the recipient's account.
    """

    tag: int
    token_amount: Optional[int] = None
    token_id: Optional[str] = None
    cis2_token_contract_address: Optional[str] = None
    from_address: Optional[str] = None
    to_public_key_ed25519: Optional[str] = None


class withdrawCCDEvent(BaseModel):
    """A CCD withdraw event from a CIS-5 compliant smart contract.

    See: [withdrawCCDEvent](http://proposals.concordium.software/CIS/cis-5.html#withdrawccdevent)

    Attributes:
        tag (int): The event tag (247 for withdraw CCD events).
        ccd_amount (Optional[microCCD]): The amount of CCD being withdrawn.
        from_public_key_ed25519 (Optional[str]): The public key from which CCD is being withdrawn.
        to_address (Optional[str]): The address receiving the withdrawn CCD.
    """

    tag: int
    ccd_amount: Optional[microCCD] = None
    from_public_key_ed25519: Optional[str] = None
    to_address: Optional[str] = None


class withdrawCIS2TokensEvent(BaseModel):
    """A CIS-2 token withdraw event from a CIS-5 compliant smart contract.

    See: [withdrawCIS2TokensEvent](http://proposals.concordium.software/CIS/cis-5.html#withdrawcis2tokenstevent)

    Attributes:
        tag (int): The event tag (246 for withdraw CIS2 tokens events).
        token_amount (Optional[int]): The amount of CIS-2 tokens being withdrawn.
        token_id (Optional[str]): The ID of the token being withdrawn.
        cis2_token_contract_address (Optional[str]): The contract address of the CIS-2 token.
        from_public_key_ed25519 (Optional[str]): The public key from which tokens are being withdrawn.
        to_address (Optional[str]): The address receiving the withdrawn tokens.
    """

    tag: int
    token_amount: Optional[int] = None
    token_id: Optional[str] = None
    cis2_token_contract_address: Optional[str] = None
    from_public_key_ed25519: Optional[str] = None
    to_address: Optional[str] = None


class transferCCDEvent(BaseModel):
    """A CCD transfer event from a CIS-5 compliant smart contract.

    See: [transferCCDEvent](http://proposals.concordium.software/CIS/cis-5.html#transferccdevent)

    Attributes:
        tag (int): The event tag (245 for transfer CCD events).
        ccd_amount (Optional[microCCD]): The amount of CCD being transferred.
        from_public_key_ed25519 (Optional[str]): The public key from which CCD is being transferred.
        to_public_key_ed25519 (Optional[str]): The public key to which CCD is being transferred.
    """

    tag: int
    ccd_amount: Optional[microCCD] = None
    from_public_key_ed25519: Optional[str] = None
    to_public_key_ed25519: Optional[str] = None


class transferCIS2TokensEvent(BaseModel):
    """A CIS-2 token transfer event from a CIS-5 compliant smart contract.

    See: [transferCIS2TokensEvent](http://proposals.concordium.software/CIS/cis-5.html#transfercis2tokensevent)

    Attributes:
        tag (int): The event tag (244 for transfer CIS2 tokens events).
        token_amount (Optional[int]): The amount of CIS-2 tokens being transferred.
        token_id (Optional[str]): The ID of the token being transferred.
        cis2_token_contract_address (Optional[str]): The contract address of the CIS-2 token.
        from_public_key_ed25519 (Optional[str]): The public key from which tokens are being transferred.
        to_public_key_ed25519 (Optional[str]): The public key to which tokens are being transferred.
    """

    tag: int
    token_amount: Optional[int] = None
    token_id: Optional[str] = None
    cis2_token_contract_address: Optional[str] = None
    from_public_key_ed25519: Optional[str] = None
    to_public_key_ed25519: Optional[str] = None


class fiveStarsRegisterAccessEvent(BaseModel):
    """A custom event for registering access for 5tars.

    Attributes:
        tag (int): The event tag (0 for 5tars register access events).
        public_key (Optional[str]): The public key being registered.
        timestamp (Optional[int]): Unix timestamp of the registration.
    """

    tag: int
    public_key: Optional[str] = None
    timestamp: Optional[int] = None
