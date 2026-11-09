"""The watchtower's four checks.

Ported from sderuiter/concordium-watchtower (last commit 07f5462) on
2026-10-27. That repo pinned `ccdexplorer_fundamentals`, which cannot read the
current protocol and fails, and it was the library's last consumer -- nothing
in this monorepo references it any more.

The checks take their collaborators as arguments rather than reaching for
module-level clients, so each one can be exercised against fakes. The wiring
lives in __main__.py.
"""

import datetime as dt
from typing import Iterable

from pydantic import BaseModel

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor, net_db
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType

# Mainnet and testnet only. `NET` gained DEVNET since this code was written,
# and the original picked its database with
# `self.mainnet if net == NET.MAINNET else self.testnet` -- which would have
# read devnet heights out of the testnet database and alerted on the
# difference.
WATCHED_NETS: tuple[NET, ...] = (NET.MAINNET, NET.TESTNET)

# A heartbeat or bot this far behind the chain is worth waking someone for.
LAG_BLOCKS = 10
SECONDARY_LAG_SECONDS = 600
PING_MS = 250


class ReplicaSetMember(BaseModel):
    name: str
    state: int
    stateStr: str
    self: bool | None = None
    pingMs: int | None = None
    optimeDate: dt.datetime | None = None


class ReplicaSetOutput(BaseModel):
    members: list[ReplicaSetMember]


class WatchTower:
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        admin_chat_id: str | int | None = None,
        watched_nets: Iterable[NET] = WATCHED_NETS,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.admin_chat_id = admin_chat_id
        self.watched_nets = tuple(watched_nets)

    def inform(self, message: str) -> None:
        """One alert, to be read when convenient. Telegram only.

        Everything used to come through here, which is why TooterType could
        not be used as a severity: "ReplicaSet has no PRIMARY" and
        "Primary: ccd-100" went out identically, both as REQUESTS_ERROR. The
        ones that mean something is down now go to `alert` instead.
        """
        self.tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=self.admin_chat_id,
            body=f"Watchtower: {message}",
            notifier_type=TooterType.REQUESTS_ERROR,
        )

    def alert(self, message: str, key: str) -> None:
        """One alert that means something is down. Telegram, and a page.

        Reserved for unavailability, not degradation. Heartbeat lag and
        replication lag used to come through here; both are the estate
        working and not keeping up, and a page that fires for those is a
        page that gets muted. They inform instead.

        Same Telegram message as `inform`, plus an iOS critical alert that
        ignores Do Not Disturb. `key` names the condition rather than the
        wording, because this runs every CHECK_INTERVAL and most of these
        messages carry a number that moves -- see Tooter.critical.
        """
        self.tooter.critical(
            f"Watchtower: {message}",
            key=f"watchtower:{key}",
            chat_id=self.admin_chat_id,
        )

    def _helpers(self, net: NET) -> dict:
        db = net_db(self.mongodb, net)
        return {x["_id"]: x for x in db[Collections.helpers].find({})}

    async def check_heartbeat_running(self) -> None:
        for net in self.watched_nets:
            helpers = self._helpers(net)
            recorded = helpers.get("heartbeat_last_processed_block", {}).get("height")
            if recorded is None:
                self.alert(
                    f"{net.value}: no heartbeat_last_processed_block recorded.",
                    key=f"heartbeat-missing:{net.value}",
                )
                continue

            tip = self.grpcclient.get_block_info("last_final", net).height
            behind = tip - recorded
            if behind >= LAG_BLOCKS:
                # Behind, not stopped: the indexer is running and catching
                # up, so this is a thing to read rather than a reason to
                # override Do Not Disturb. A marker missing entirely is a
                # different matter and still pages, above.
                self.inform(f"{net.value}: Heartbeat lagging {behind} blocks.")

    async def check_bot(self) -> None:
        helpers = self._helpers(NET.MAINNET)
        heartbeat = helpers.get("heartbeat_last_processed_block", {}).get("height")
        bot = helpers.get("bot_last_processed_block", {}).get("height")
        if heartbeat is None or bot is None:
            self.inform("Cannot compare bot to heartbeat: a marker is missing.")
            return

        behind = heartbeat - bot
        if behind >= LAG_BLOCKS:
            self.inform(f"Bot lagging {behind} blocks.")

    async def check_mongo(self) -> None:
        try:
            status = self.mongodb.connection.admin.command("replSetGetStatus")
            hello = self.mongodb.connection.admin.command("hello")
            output = ReplicaSetOutput(members=[ReplicaSetMember(**x) for x in status["members"]])

            # No primary is the case this check exists for, and the original
            # read `next(m for m ... == "PRIMARY")` outside its try, so an
            # election crashed it instead of reporting.
            primary = next((m for m in output.members if m.stateStr == "PRIMARY"), None)
            if primary is None:
                self.alert("ReplicaSet has no PRIMARY.", key="no-primary")

            if len(output.members) < 3:
                online = " ".join(m.name for m in output.members)
                self.alert(
                    f"There are only {len(output.members)} replicaset members online! Online: {online}",
                    key="members-missing",
                )
                self.inform(f"Primary: {hello.get('primary')}")
                return

            for member in output.members:
                if member.pingMs and member.pingMs > PING_MS:
                    self.inform(f"Member {member.name} has ping of {member.pingMs} ms to primary.")
                if member.state not in (1, 2):
                    self.alert(
                        f"Member {member.name} has state {member.stateStr}.",
                        key=f"member-state:{member.name}",
                    )
                if (
                    member.stateStr == "SECONDARY"
                    and primary is not None
                    and member.optimeDate is not None
                    and primary.optimeDate is not None
                ):
                    lag = (primary.optimeDate - member.optimeDate).total_seconds()
                    if lag > SECONDARY_LAG_SECONDS:
                        # A healthy SECONDARY that is behind. The set has a
                        # primary and is serving reads and writes, so nothing
                        # is down -- a member in a bad *state* is caught
                        # above and does page.
                        self.inform(f"Member {member.name} is {lag / 60:.0f} mins behind primary.")
        except Exception as e:
            self.alert(f"check_mongo: {e}", key="check-mongo-failed")
