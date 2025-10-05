# ruff: noqa: F403, F405, E402
from __future__ import annotations
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_RawQuorumCertificate,
    CCD_RoundStatus,
)
from ccdexplorer.grpc_client.types_pb2 import *
from ccdexplorer.domain.generic import NET
from enum import Enum
from ccdexplorer.grpc_client.queries._SharedConverters import (
    Mixin as _SharedConverters,
)
from ccdexplorer.grpc_client.CCD_Types import *

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ccdexplorer.grpc_client import GRPCClient


class Mixin(_SharedConverters):
    def convertRoundTimeout(self, message) -> CCD_RoundTimeout:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) is RawTimeoutCertificate:
                result[key] = self.convertRawTimeoutCertificate(value)
            elif type(value) is RawQuorumCertificate:
                result[key] = self.convertRawQuorumCertificate(value)

        return CCD_RoundTimeout(**result)

    def convertSignatories(self, message) -> list[CCD_FinalizerIndex]:
        signatories = []
        for finalizer_index in message:
            signatories.append(finalizer_index.value)

        return signatories

    def convertFinalizers(self, message) -> list[CCD_BakerId]:
        finalizers = []
        for baker_id in message:
            finalizers.append(baker_id.value)

        return finalizers

    def convertFullBakerInfo(self, message) -> list[CCD_FullBakerInfo]:
        bakers = []
        for baker in message:
            result = {}
            for field, value in baker.ListFields():
                key = field.name
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)

            bakers.append(CCD_FullBakerInfo(**result))
        return bakers

    def convertRawFinalizerRounds(self, message) -> list[CCD_RawFinalizerRound]:
        rounds = []
        for round in message:
            result = {}
            for field, value in round.ListFields():
                key = field.name
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                elif key == "finalizers":
                    result[key] = self.convertSignatories(value)
            rounds.append(CCD_RawFinalizerRound(**result))
        return rounds

    def convertRawTimeoutCertificate(self, message) -> CCD_RawTimeoutCertificate:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif key in ["qc_rounds_first_epoch", "qc_rounds_second_epoch"]:
                result[key] = self.convertRawFinalizerRounds(value)

        return CCD_RawTimeoutCertificate(**result)

    def convertRawQuorumCertificate(self, message) -> CCD_RawQuorumCertificate:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif key == "signatories":
                result[key] = self.convertSignatories(value)

        return CCD_RawQuorumCertificate(**result)

    def convertRoundStatus(self, message) -> CCD_RoundStatus:
        result = {}
        for field, value in message.ListFields():
            key = field.name
            if self.valueIsEmpty(value):
                pass
            else:
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                elif type(value) is RawQuorumCertificate:
                    result[key] = self.convertRawQuorumCertificate(value)
                elif type(value) is RoundTimeout:
                    result[key] = self.convertRoundTimeout(value)

        return CCD_RoundStatus(**result)

    def convertQuorumMessage(self, message) -> CCD_QuorumMessage:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_QuorumMessage(**result)

    def convertTimeoutMessage(self, message) -> CCD_TimeoutMessage:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif type(value) is RawQuorumCertificate:
                result[key] = self.convertRawQuorumCertificate(value)

        return CCD_TimeoutMessage(**result)

    def convertPersistentRoundStatus(self, message) -> CCD_PersistentRoundStatus:
        result = {}
        for field, value in message.ListFields():
            key = field.name
            if self.valueIsEmpty(value):
                pass
            else:
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                elif type(value) is QuorumMessage:
                    result[key] = self.convertQuorumMessage(value)
                elif type(value) is TimeoutMessage:
                    result[key] = self.convertTimeoutMessage(value)
                elif type(value) is RawQuorumCertificate:
                    result[key] = self.convertRawQuorumCertificate(value)

        return CCD_PersistentRoundStatus(**result)

    def convertLiveBlocks(self, message) -> list[CCD_BlockHash]:
        live_blocks = []
        for hash in message:
            live_blocks.append(self.convertType(hash))

        return live_blocks

    def convertBlocksAtBranchHeight(self, message) -> list[CCD_BlockHash]:
        blocks_at_branch_height = []
        for hash in message:
            blocks_at_branch_height.append(self.convertType(hash))

        return blocks_at_branch_height

    def convertBranches(self, message) -> list[CCD_BranchBlocks]:
        branches = []
        for hash in message:
            for field, value in hash.ListFields():
                key = field.name
                if key == "blocks_at_branch_height":
                    blocks_at_branch_height = self.convertBlocksAtBranchHeight(value)
                branches.append(
                    CCD_BranchBlocks(**{"blocks_at_branch_height": blocks_at_branch_height})
                )

        return branches

    def convertRoundExistingBlock(self, message) -> CCD_RoundExistingBlock:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_RoundExistingBlock(**result)

    def convertRoundExistingBlocks(self, message) -> list[CCD_RoundExistingBlock]:
        round_existing_blocks = []
        for value in message:
            round_existing_blocks.append(self.convertRoundExistingBlock(value))

        return round_existing_blocks

    def convertRoundExistingQC(self, message) -> CCD_RoundExistingQC:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)

        return CCD_RoundExistingQC(**result)

    def convertRoundExistingQCs(self, message) -> list[CCD_RoundExistingQC]:
        round_existing_qcs = []
        for value in message:
            round_existing_qcs.append(self.convertRoundExistingQC(value))

        return round_existing_qcs

    def convertBlockTableSummary(self, message) -> CCD_BlockTableSummary:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            if key == "live_blocks":
                result[key] = self.convertLiveBlocks(value)

        return CCD_BlockTableSummary(**result)

    def convertRawFinalizationEntry(self, message) -> CCD_RawFinalizationEntry:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif type(value) is RawQuorumCertificate:
                result[key] = self.convertRawQuorumCertificate(value)

        return CCD_RawFinalizationEntry(**result)

    def convertBakersAndFinalizers(self, message) -> CCD_BakersAndFinalizers:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif key == "finalizers":
                result[key] = self.convertFinalizers(value)
            elif key == "bakers":
                result[key] = self.convertFullBakerInfo(value)

        return CCD_BakersAndFinalizers(**result)

    def convertEpochBakers(self, message) -> CCD_EpochBakers:
        result = {}
        for field, value in message.ListFields():
            key = field.name
            if self.valueIsEmpty(value):
                pass
            else:
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                elif type(value) is BakersAndFinalizers:
                    result[key] = self.convertBakersAndFinalizers(value)

        return CCD_EpochBakers(**result)

    def convertTimeoutMessages(self, message) -> CCD_TimeoutMessages:
        result = {}
        for field, value in message.ListFields():
            key = field.name

            if type(value) in self.simple_types:
                result[key] = self.convertType(value)
            elif key in ["first_epoch_timeouts", "second_epoch_timeouts"]:
                result[key] = self.convertTimeoutMessage(value)

        return CCD_TimeoutMessages(**result)

    def get_consensus_detailed_status(
        self: GRPCClient,
        gen_index: Optional[int] = None,
        net: Enum = NET.MAINNET,
    ) -> CCD_ConsensusDetailedStatus:
        result = {}
        consensus_detailed_status_query = self.generate_consensus_detailed_status_query()

        grpc_return_value: ConsensusDetailedStatus = self.stub_on_net(
            net, "GetConsensusDetailedStatus", consensus_detailed_status_query
        )

        for field, value in grpc_return_value.ListFields():
            key = field.name
            if self.valueIsEmpty(value):
                pass
            else:
                if type(value) in self.simple_types:
                    result[key] = self.convertType(value)
                elif type(value) is PersistentRoundStatus:
                    result[key] = self.convertPersistentRoundStatus(value)
                elif type(value) is RoundStatus:
                    result[key] = self.convertRoundStatus(value)
                elif type(value) is BlockTableSummary:
                    result[key] = self.convertBlockTableSummary(value)
                elif key == "branches":
                    result[key] = self.convertBranches(value)
                elif key == "round_existing_blocks":
                    result[key] = self.convertRoundExistingBlocks(value)
                elif key == "round_existing_qcs":
                    result[key] = self.convertRoundExistingQCs(value)
                elif type(value) is RawFinalizationEntry:
                    result[key] = self.convertRawFinalizationEntry(value)
                elif type(value) is EpochBakers:
                    result[key] = self.convertEpochBakers(value)
                elif type(value) is TimeoutMessages:
                    result[key] = self.convertTimeoutMessages(value)

        return CCD_ConsensusDetailedStatus(**result)
