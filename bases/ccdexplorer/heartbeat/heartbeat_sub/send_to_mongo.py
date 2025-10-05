from .utils import Utils, Queue
from ccdexplorer.mongodb import Collections, MongoDB
from ccdexplorer.tooter import TooterChannel, TooterType, Tooter
from pymongo.collection import Collection
from pymongo.results import ClientBulkWriteResult
from pymongo.asynchronous.collection import AsyncCollection


from ccdexplorer.env import ADMIN_CHAT_ID

from rich.console import Console

console = Console()


class SendToMongo(Utils):
    async def send_to_mongo(self):
        """
        This method takes all queues with mongoDB messages and sends them to the
        respective collections.
        """
        self.queues: dict[Queue, list]
        self.db: dict[Collections, Collection]
        self.motordb: dict[Collections, AsyncCollection]
        self.mongodb: MongoDB
        self.tooter: Tooter
        self.net: str

        try:
            ops: list = []

            for q in (Queue.special_events, Queue.transactions):
                if self.queues.get(q):
                    ops.extend(self.queues[q])
                    self.queues[q] = []

            # blocks LAST
            if self.queues.get(Queue.blocks):
                ops.extend(self.queues[Queue.blocks])
                self.queues[Queue.blocks] = []

            if not ops:
                return

            result: ClientBulkWriteResult = self.mongodb.connection.bulk_write(ops)

            if len(self.queues[Queue.block_heights]) == 1:
                console.log(f"Sent to Mongo  : {self.queues[Queue.block_heights][0]:,.0f}")
            else:
                console.log(
                    f"Sent to Mongo   : {self.queues[Queue.block_heights][0]:,.0f} - {self.queues[Queue.block_heights][-1]:,.0f}"
                )
            if result.upserted_count > 0:
                console.log(f"Upserted: {result.upserted_count:,.0f}")
            if result.matched_count > 0:
                console.log(f"Matched:  {result.matched_count:,.0f}")
            if result.modified_count > 0:
                console.log(f"Modified: {result.modified_count:,.0f}")

            self.queues[Queue.block_heights] = []

            if len(self.queues[Queue.block_per_day]) > 0:
                result2 = self.db[Collections.blocks_per_day].bulk_write(
                    self.queues[Queue.block_per_day]
                )
                console.log(f"End of day: U {result2.upserted_count:5,.0f}")
                self.queues[Queue.block_per_day] = []

        except Exception as e:
            # pass
            console.log(e)
            self.tooter.relay(
                channel=TooterChannel.NOTIFIER,
                title="",
                chat_id=int(ADMIN_CHAT_ID),  # type: ignore
                body=f"Heartbeat on {self.net} send_to_mongo: {e}",
                notifier_type=TooterType.MONGODB_ERROR,
            )

            # await asyncio.sleep(1)
