import functools
import os

import dagster as dg
from ccdexplorer.mongodb import MongoDB
from ccdexplorer.tooter import Tooter
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.ccdscan import CCDScan

from dateutil import parser
from git import Commit, Repo

from ccdexplorer.env import REPO_DIR


class GRPCResource(dg.ConfigurableResource):
    """Resource to access the GRPCClient"""

    def get_client(self) -> GRPCClient:
        grpc = GRPCClient()
        return grpc


@functools.cache
def shared_mongodb() -> MongoDB:
    """One MongoDB client per process, built on first use.

    Each MongoDB() opens a fresh MongoClient: three server connections on the
    replica set plus a server_info() round trip before any query runs. Built
    per get_client() call, as this used to be, that was a new client for every
    asset execution, never closed. Caching per process keeps one for the life
    of a run worker, and one for the long-lived code server where schedules
    evaluate. Lazy rather than at import, so importing the definitions opens
    nothing.
    """
    return MongoDB(Tooter(), nearest=True, caller_name="dagster_nightrunner")


class MongoDBResource(dg.ConfigurableResource):
    """Resource to access the shared MongoDB database"""

    def get_client(self) -> MongoDB:
        return shared_mongodb()


class CCDScanResource(dg.ConfigurableResource):
    """Resource to access CCDScan through its GraphQL API"""

    def get_client(self) -> CCDScan:
        tooter: Tooter = Tooter()
        ccdscan: CCDScan = CCDScan(tooter)
        return ccdscan


class RepoResource(dg.ConfigurableResource):
    """Resource to access the repository at https://github.com/ccdexplorer/ccdexplorer-accounts"""

    def get_commits_by_day(self) -> dict[str, Commit]:
        ON_SERVER = os.environ.get("ON_SERVER", False)

        print(f"{ON_SERVER=}.")
        repo_dir = REPO_DIR
        if ON_SERVER:
            repo_dir = "/home/git_dir"

        repo = Repo(repo_dir)
        origin = repo.remote(name="origin")
        _ = origin.pull()
        commits = list(repo.iter_commits("main"))
        commits_by_day = {f"{parser.parse(x.message):%Y-%m-%d}": x for x in commits}
        return commits_by_day


# Create single instances
mongodb_resource_instance = MongoDBResource()
repo_resource_instance = RepoResource()
grpc_resource_instance = GRPCResource()
ccdscan_resource_instance = CCDScanResource()
