from . import kernel_pb2 as _kernel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class Cbor(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class TokenId(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class TokenModuleRef(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class TokenAmount(_message.Message):
    __slots__ = ("value", "decimals")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    DECIMALS_FIELD_NUMBER: _ClassVar[int]
    value: int
    decimals: int
    def __init__(self, value: _Optional[int] = ..., decimals: _Optional[int] = ...) -> None: ...

class TokenState(_message.Message):
    __slots__ = ("token_module_ref", "decimals", "total_supply", "module_state")
    TOKEN_MODULE_REF_FIELD_NUMBER: _ClassVar[int]
    DECIMALS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SUPPLY_FIELD_NUMBER: _ClassVar[int]
    MODULE_STATE_FIELD_NUMBER: _ClassVar[int]
    token_module_ref: TokenModuleRef
    decimals: int
    total_supply: TokenAmount
    module_state: Cbor
    def __init__(
        self,
        token_module_ref: _Optional[_Union[TokenModuleRef, _Mapping]] = ...,
        decimals: _Optional[int] = ...,
        total_supply: _Optional[_Union[TokenAmount, _Mapping]] = ...,
        module_state: _Optional[_Union[Cbor, _Mapping]] = ...,
    ) -> None: ...

class TokenAccountState(_message.Message):
    __slots__ = ("balance", "module_state")
    BALANCE_FIELD_NUMBER: _ClassVar[int]
    MODULE_STATE_FIELD_NUMBER: _ClassVar[int]
    balance: TokenAmount
    module_state: Cbor
    def __init__(
        self,
        balance: _Optional[_Union[TokenAmount, _Mapping]] = ...,
        module_state: _Optional[_Union[Cbor, _Mapping]] = ...,
    ) -> None: ...

class TokenModuleEvent(_message.Message):
    __slots__ = ("type", "details")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    type: str
    details: Cbor
    def __init__(
        self,
        type: _Optional[str] = ...,
        details: _Optional[_Union[Cbor, _Mapping]] = ...,
    ) -> None: ...

class TokenHolder(_message.Message):
    __slots__ = ("account",)
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    account: _kernel_pb2.AccountAddress
    def __init__(
        self, account: _Optional[_Union[_kernel_pb2.AccountAddress, _Mapping]] = ...
    ) -> None: ...

class TokenTransferEvent(_message.Message):
    __slots__ = ("to", "amount", "memo")
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    MEMO_FIELD_NUMBER: _ClassVar[int]
    to: TokenHolder
    amount: TokenAmount
    memo: _kernel_pb2.Memo
    def __init__(
        self,
        to: _Optional[_Union[TokenHolder, _Mapping]] = ...,
        amount: _Optional[_Union[TokenAmount, _Mapping]] = ...,
        memo: _Optional[_Union[_kernel_pb2.Memo, _Mapping]] = ...,
        **kwargs,
    ) -> None: ...

class TokenSupplyUpdateEvent(_message.Message):
    __slots__ = ("target", "amount")
    TARGET_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    target: TokenHolder
    amount: TokenAmount
    def __init__(
        self,
        target: _Optional[_Union[TokenHolder, _Mapping]] = ...,
        amount: _Optional[_Union[TokenAmount, _Mapping]] = ...,
    ) -> None: ...

class TokenEvent(_message.Message):
    __slots__ = (
        "token_id",
        "module_event",
        "transfer_event",
        "mint_event",
        "burn_event",
    )
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    MODULE_EVENT_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_EVENT_FIELD_NUMBER: _ClassVar[int]
    MINT_EVENT_FIELD_NUMBER: _ClassVar[int]
    BURN_EVENT_FIELD_NUMBER: _ClassVar[int]
    token_id: TokenId
    module_event: TokenModuleEvent
    transfer_event: TokenTransferEvent
    mint_event: TokenSupplyUpdateEvent
    burn_event: TokenSupplyUpdateEvent
    def __init__(
        self,
        token_id: _Optional[_Union[TokenId, _Mapping]] = ...,
        module_event: _Optional[_Union[TokenModuleEvent, _Mapping]] = ...,
        transfer_event: _Optional[_Union[TokenTransferEvent, _Mapping]] = ...,
        mint_event: _Optional[_Union[TokenSupplyUpdateEvent, _Mapping]] = ...,
        burn_event: _Optional[_Union[TokenSupplyUpdateEvent, _Mapping]] = ...,
    ) -> None: ...

class TokenEffect(_message.Message):
    __slots__ = ("events",)
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[TokenEvent]
    def __init__(
        self, events: _Optional[_Iterable[_Union[TokenEvent, _Mapping]]] = ...
    ) -> None: ...

class TokenModuleRejectReason(_message.Message):
    __slots__ = ("token_id", "type", "details")
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    token_id: TokenId
    type: str
    details: Cbor
    def __init__(
        self,
        token_id: _Optional[_Union[TokenId, _Mapping]] = ...,
        type: _Optional[str] = ...,
        details: _Optional[_Union[Cbor, _Mapping]] = ...,
    ) -> None: ...

class CreatePLT(_message.Message):
    __slots__ = ("token_id", "token_module", "decimals", "initialization_parameters")
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_MODULE_FIELD_NUMBER: _ClassVar[int]
    DECIMALS_FIELD_NUMBER: _ClassVar[int]
    INITIALIZATION_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    token_id: TokenId
    token_module: TokenModuleRef
    decimals: int
    initialization_parameters: Cbor
    def __init__(
        self,
        token_id: _Optional[_Union[TokenId, _Mapping]] = ...,
        token_module: _Optional[_Union[TokenModuleRef, _Mapping]] = ...,
        decimals: _Optional[int] = ...,
        initialization_parameters: _Optional[_Union[Cbor, _Mapping]] = ...,
    ) -> None: ...

class TokenCreationDetails(_message.Message):
    __slots__ = ("create_plt", "events")
    CREATE_PLT_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    create_plt: CreatePLT
    events: _containers.RepeatedCompositeFieldContainer[TokenEvent]
    def __init__(
        self,
        create_plt: _Optional[_Union[CreatePLT, _Mapping]] = ...,
        events: _Optional[_Iterable[_Union[TokenEvent, _Mapping]]] = ...,
    ) -> None: ...
