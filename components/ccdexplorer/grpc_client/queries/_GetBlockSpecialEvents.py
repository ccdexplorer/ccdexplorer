from __future__ import annotations
from ccdexplorer.grpc_client.types_pb2 import BlockSpecialEvent
from ccdexplorer.domain.generic import NET
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
    assert_oneof_covered,
)
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockSpecialEvent_AccountAmounts_Entry,
    CCD_BlockSpecialEvent_AccountAmounts,
    CCD_BlockSpecialEvent_BakingRewards,
    CCD_BlockSpecialEvent_FinalizationRewards,
    CCD_BlockSpecialEvent_ValidatorPrimedForSuspension,
    CCD_BlockSpecialEvent_ValidatorSuspended,
    CCD_BlockSpecialEvent,
)
from enum import Enum


# Every arm of BlockSpecialEvent's `event` oneof.
_SPECIAL_EVENT_CONVERTERS = {
    "baking_rewards": "convertBakingRewards",
    "mint": "convertTypeWithSingleValues",
    "finalization_rewards": "convertFinalizationRewards",
    "block_reward": "convertTypeWithSingleValues",
    "payday_foundation_reward": "convertTypeWithSingleValues",
    "payday_account_reward": "convertTypeWithSingleValues",
    "block_accrue_reward": "convertTypeWithSingleValues",
    "payday_pool_reward": "convertTypeWithSingleValues",
    "validator_suspended": "convertValidatorSuspended",
    "validator_primed_for_suspension": "convertValidatorPrimedForSuspension",
}
assert_oneof_covered(BlockSpecialEvent.DESCRIPTOR, "event", _SPECIAL_EVENT_CONVERTERS)


class Mixin(_SharedConverters):
    def convertAccountAmountsEntries(
        self, message
    ) -> list[CCD_BlockSpecialEvent_AccountAmounts_Entry]:
        entries = []

        for list_entry in message:
            entries.append(
                CCD_BlockSpecialEvent_AccountAmounts_Entry(
                    **self.convertTypeWithSingleValues(list_entry)
                )
            )

        return entries

    def convertAccountAmountsBakingRewards(self, message) -> CCD_BlockSpecialEvent_AccountAmounts:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            result[key] = self.convertAccountAmountsEntries(value)

        return CCD_BlockSpecialEvent_AccountAmounts(**result)

    def convertAccountAmountsFinalizationRewards(
        self, message
    ) -> CCD_BlockSpecialEvent_AccountAmounts:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)
            result[key] = self.convertAccountAmountsEntries(value)

        return CCD_BlockSpecialEvent_AccountAmounts(**result)

    def convertBakingRewards(self, message) -> CCD_BlockSpecialEvent_BakingRewards:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is BlockSpecialEvent.AccountAmounts:
                result[key] = self.convertAccountAmountsBakingRewards(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockSpecialEvent_BakingRewards(**result)

    def convertFinalizationRewards(self, message) -> CCD_BlockSpecialEvent_FinalizationRewards:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) is BlockSpecialEvent.AccountAmounts:
                result[key] = self.convertAccountAmountsFinalizationRewards(value)

            elif type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockSpecialEvent_FinalizationRewards(**result)

    def convertValidatorPrimedForSuspension(
        self, message
    ) -> CCD_BlockSpecialEvent_ValidatorPrimedForSuspension:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockSpecialEvent_ValidatorPrimedForSuspension(**result)

    def convertValidatorSuspended(self, message) -> CCD_BlockSpecialEvent_ValidatorSuspended:
        result = {}
        for descriptor in message.DESCRIPTOR.fields:
            key, value = self.get_key_value_from_descriptor(descriptor, message)

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_BlockSpecialEvent_ValidatorSuspended(**result)

    def get_block_special_events(
        self,
        block_input: str | int,
        net: Enum = NET.MAINNET,
    ) -> list[CCD_BlockSpecialEvent]:
        blockHashInput = self.generate_block_hash_input_from(block_input)

        grpc_return_value = self.stub_on_net(  # type: ignore
            net, "GetBlockSpecialEvents", blockHashInput, streaming=True
        )

        events = []
        for special_event in list(grpc_return_value):
            key = special_event.WhichOneof("event")
            if key is None:
                continue
            converter = getattr(self, _SPECIAL_EVENT_CONVERTERS[key])
            events.append(CCD_BlockSpecialEvent(**{key: converter(getattr(special_event, key))}))

        return events
