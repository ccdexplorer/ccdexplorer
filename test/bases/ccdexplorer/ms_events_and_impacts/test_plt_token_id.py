"""Where a PLT event's token id lives depends on the wrapper it arrived in.

protocol-level-tokens.proto, on `TokenTransferEvent.token_id` and
`TokenSupplyUpdateEvent.token_id`: "In the context of a `TokenEvent`, which
already specifies the token, this is absent. In the context of a `MetaEvent`,
it must be present."

So exactly one of the two places holds it, and reading only one of them loses
the id for every event of the other shape. These tests pin both shapes.
"""

import pytest
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_Cbor,
    CCD_MetaEvent,
    CCD_TokenAmount,
    CCD_TokenEvent,
    CCD_TokenHolder,
    CCD_TokenModuleEvent,
    CCD_TokenSupplyUpdateEvent,
    CCD_TokenTransferEvent,
)
from ccdexplorer.ms_events_and_impacts.subscriber.impacted_addresses_from_tx import (
    _token_id_for,
)

SENDER = "4R4SkdvpkepcFa6UtvyevTh1YCC2jJh3jLidQDVkC5rkC7tN8D"
RECEIVER = "4krZaD8KvXnrbPixexpbvvyQdvz9Qrbi1XLTCJLcN66Bm48QSs"


def _transfer(token_id=None):
    return CCD_TokenTransferEvent(
        **{
            "from": CCD_TokenHolder(account=SENDER),
            "to": CCD_TokenHolder(account=RECEIVER),
            "amount": CCD_TokenAmount(value="1000000", decimals=6),
            "token_id": token_id,
        }
    )


def _supply(token_id=None):
    return CCD_TokenSupplyUpdateEvent(
        target=CCD_TokenHolder(account=SENDER),
        amount=CCD_TokenAmount(value="1000000", decimals=6),
        token_id=token_id,
    )


def _module(token_id=None):
    return CCD_TokenModuleEvent(type="addAllowList", details=CCD_Cbor(), token_id=token_id)


# --- TokenEvent: the id is on the wrapper, absent from the sub-event ---------
# This is the shape of every ordinary PLT transaction, and the one that
# regressed: reading the sub-event alone yielded None and then failed
# PLTTransferType validation.


@pytest.mark.parametrize(
    "sub_event_kwargs",
    [
        {"transfer_event": _transfer()},
        {"mint_event": _supply()},
        {"burn_event": _supply()},
        {"module_event": _module()},
    ],
    ids=["transfer", "mint", "burn", "module"],
)
def test_token_event_takes_id_from_wrapper(sub_event_kwargs):
    event = CCD_TokenEvent(token_id="USDR", **sub_event_kwargs)
    assert _token_id_for(event) == "USDR"


# --- MetaEvent: no wrapper field at all, the id is on the sub-event ----------


@pytest.mark.parametrize(
    "sub_event_kwargs",
    [
        {"transfer_event": _transfer(token_id="USDR")},
        {"mint_event": _supply(token_id="USDR")},
        {"burn_event": _supply(token_id="USDR")},
        {"module_event": _module(token_id="USDR")},
    ],
    ids=["transfer", "mint", "burn", "module"],
)
def test_meta_event_takes_id_from_sub_event(sub_event_kwargs):
    event = CCD_MetaEvent(**sub_event_kwargs)
    # The attribute does not exist on this wrapper -- the resolver must not
    # assume it does.
    assert not hasattr(event, "token_id")
    assert _token_id_for(event) == "USDR"


def test_returns_none_when_neither_carries_it():
    """Undetermined is reported as such, not as an empty string.

    The caller stores this on `PLTTransferType.token_id`, which requires a
    string, so a wrong-but-truthy value would be written to the database
    instead of failing loudly.
    """
    assert _token_id_for(CCD_MetaEvent(transfer_event=_transfer())) is None


def test_wrapper_wins_over_sub_event():
    """If both are somehow set, the wrapper is the authority.

    `TokenEvent.token_id` is a required field in the proto; the sub-event's is
    optional and documented as absent in this context.
    """
    event = CCD_TokenEvent(token_id="USDR", transfer_event=_transfer(token_id="EUROe"))
    assert _token_id_for(event) == "USDR"
