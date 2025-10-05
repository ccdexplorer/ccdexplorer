from __future__ import annotations

import subprocess
import time
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
    MongoMotor,
)
from ccdexplorer.tooter import Tooter
from .core import Daily
from git import Repo

from ccdexplorer.env import ON_SERVER

grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


if __name__ == "__main__":
    print(f"{ON_SERVER=}.")
    new_dir = "/Users/sander/Developer/open_source/ccdexplorer-accounts"
    if ON_SERVER:
        new_dir = "/home/git_dir"
    repo_new = Repo(new_dir)

    if ON_SERVER:
        new_dir = "/home/git_dir"
        result = subprocess.run(
            [
                "git",
                "-C",
                "/home/git_dir",
                "remote",
                "-v",
            ],
            stdout=subprocess.PIPE,
        )
        print(
            "xxxx-xx-xx",
            f"Result of adding repo (statistics) through subprocess: {repo_new.remote(name='origin').exists()=}",
        )

    origin = repo_new.remote(name="origin")
    origin.pull()

    while True:
        last_date_known: str | None = None
        last_date_processed: str | None = None
        last_hash_for_day: str | None = None
        last_height_for_day: int | None = None

        pipeline = [{"$sort": {"height_for_last_block": -1}}, {"$limit": 1}]
        result = list(mongodb.mainnet[Collections.blocks_per_day].aggregate(pipeline))
        if len(result) == 1:
            last_date_known = result[0]["date"]
            last_hash_for_day = result[0]["hash_for_last_block"]
            last_height_for_day = result[0]["height_for_last_block"]

        result = mongodb.mainnet[Collections.helpers].find_one(
            {"_id": "last_known_nightly_accounts"}
        )
        if result:
            last_date_processed = result["date"]

        if last_date_known != last_date_processed:
            assert last_date_known is not None
            assert last_hash_for_day is not None
            assert last_height_for_day is not None
            Daily(
                last_date_known,
                last_hash_for_day,
                last_height_for_day,
                grpcclient,
                repo_new,
                new_dir,
                mongodb,
                tooter,
            )
        else:
            print("Nothing to do...")
        time.sleep(5 * 60)
