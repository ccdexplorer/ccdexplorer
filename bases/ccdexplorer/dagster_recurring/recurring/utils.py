from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from pymongo.collection import Collection


def get_helper(_id: str, net: str, mongodb: MongoDB) -> int:
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    doc: dict | None = db[Collections.helpers].find_one({"_id": _id})
    return doc.get("height", -1) if doc else -1


def update_helper(_id: str, height: int, net: str, mongodb: MongoDB) -> None:
    db: dict[Collections, Collection] = mongodb.mainnet if net == "mainnet" else mongodb.testnet
    db[Collections.helpers].bulk_write(
        [
            ReplaceOne(
                {"_id": _id},
                replacement={
                    "_id": _id,
                    "height": height,
                },
                upsert=True,
            )
        ]
    )
