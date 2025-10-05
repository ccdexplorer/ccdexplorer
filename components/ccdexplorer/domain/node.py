from pydantic import BaseModel
from typing import Optional


# do not try to add a link back to the baker/account here
# tried this and it fails (recursion, circular imports, etc).
class ConcordiumNodeFromDashboard(BaseModel):
    nodeName: str
    nodeId: str
    peerType: str
    uptime: int
    client: str
    averagePing: Optional[float] = None
    peersCount: int
    peersList: list
    bestBlock: str
    bestBlockHeight: int
    bestBlockBakerId: Optional[int] = None
    bestArrivedTime: Optional[str] = None
    blockArrivePeriodEMA: Optional[float] = None
    blockArrivePeriodEMSD: Optional[float] = None
    blockArriveLatencyEMA: float
    blockArriveLatencyEMSD: float
    blockReceivePeriodEMA: Optional[float] = None
    blockReceivePeriodEMSD: Optional[float] = None
    blockReceiveLatencyEMA: float
    blockReceiveLatencyEMSD: float
    finalizedBlock: str
    finalizedBlockHeight: int
    finalizedTime: Optional[str] = None
    finalizationPeriodEMA: Optional[float] = None
    finalizationPeriodEMSD: Optional[float] = None
    packetsSent: int
    packetsReceived: int
    consensusRunning: bool
    bakingCommitteeMember: str
    consensusBakerId: Optional[int] = None
    finalizationCommitteeMember: bool
    transactionsPerBlockEMA: float
    transactionsPerBlockEMSD: float
    bestBlockTransactionsSize: int
    bestBlockTotalEncryptedAmount: Optional[int] = None
    bestBlockTotalAmount: Optional[int] = None
    bestBlockTransactionCount: Optional[int] = None
    bestBlockTransactionEnergyCost: Optional[int] = None
    bestBlockExecutionCost: Optional[float] = None
    bestBlockCentralBankAmount: Optional[int] = None
    blocksReceivedCount: int
    blocksVerifiedCount: int
    genesisBlock: str
    finalizationCount: int
    finalizedBlockParent: str
    averageBytesPerSecondIn: int
    averageBytesPerSecondOut: int
