import datetime as dt

import dagster as dg
import requests
from ccdexplorer.mongodb import Collections, MongoDB

from ..nightrunner.update_tx_by_project import try_to_fill_unknown_txs
from ._resources import MongoDBResource, mongodb_resource_instance

projects_json = dg.AssetSpec(
    key="projects_json",
    description="Projects.json from repo ccdexplorer/.github",
    group_name="source_github",
)


@dg.sensor(
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def projects_json_sensor(
    context: dg.SensorEvaluationContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dg.SensorResult:
    mongodb: MongoDB = mongo_resource.get_client()
    repo_owner = "ccdexplorer"
    repo_name = ".github"
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits"
    response = requests.get(url)

    if response.status_code != 200:
        return dg.SensorResult(skip_reason=f"GitHub API error: {response.status_code}")

    latest_commit = response.json()[0]
    latest_hash = latest_commit["sha"]
    latest_time = latest_commit["commit"]["committer"]["date"]

    # Check MongoDB for the trigger condition
    last_hash_seen = mongodb.mainnet[Collections.helpers].find_one(
        {"_id": "latest_projects_json_commit"}
    )
    last_hash_seen = last_hash_seen.get("value") if last_hash_seen else None

    # Compare with stored cursor
    if last_hash_seen != latest_hash:
        # Update the MongoDB with the latest commit hash
        mongodb.mainnet[Collections.helpers].update_one(
            {"_id": "latest_projects_json_commit"},
            {"$set": {"value": latest_hash}},
            upsert=True,
        )
        return dg.SensorResult(
            asset_events=[dg.AssetMaterialization(asset_key="projects_json")],
            cursor=latest_hash,
            tags={"commit_time": latest_time, "commit_sha": latest_hash},
        )

    else:
        return dg.SensorResult(skip_reason="Condition not met: update project.json")


@dg.asset(
    deps=[projects_json],
    automation_condition=dg.AutomationCondition.eager() & ~dg.AutomationCondition.in_progress(),
    name="updated_sender_ids",
    group_name="source_mongo",
)
def updated_sender_ids(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
):
    """Asset is materialized when all txs have been classified with a
    recognized_sender_id from project.json where available."""
    mongodb: MongoDB = mongo_resource.get_client()

    # first we need to go through all txs that currently have no
    # recognized_sender_id. If we find any, we will set that tx date
    # for statistic `statistics_transaction_types_by_project_dates` as `complete=False`
    # This will trigger the backfill for asset `tx_by_project`
    try_to_fill_unknown_txs(context, mongodb, "2025-09-01")
    return dg.SensorResult(
        asset_events=[dg.AssetMaterialization(asset_key="updated_sender_ids")],
        cursor=dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S"),
    )


@dg.asset_sensor(
    asset_key=updated_sender_ids.key,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def backfill_tx_by_project_sensor(
    context: dg.SensorEvaluationContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
):
    """Sensor to backfill `tx_by_project` asset when `updated_sender_ids` is updated."""
    mongodb: MongoDB = mongo_resource.get_client()

    # find dates that have `complete=False`
    pipeline = [
        {"$match": {"type": "statistics_transaction_types_by_project_dates"}},
        {"$match": {"complete": False}},
        {"$sort": {"date": -1}},
        {"$project": {"date": 1, "_id": 0}},
    ]

    dates_for_backfill = [
        doc["date"] for doc in mongodb.mainnet[Collections.statistics].aggregate(pipeline)
    ]
    context.log.info(f"Dates for backfill: {dates_for_backfill}")
    asset_events = [
        dg.AssetMaterialization(
            asset_key=dg.AssetKey("tx_by_project"),
            partition=partition_key,
        )
        for partition_key in dates_for_backfill
    ]

    return dg.SensorResult(asset_events=asset_events)


# Define the Definitions object
defs = dg.Definitions(
    assets=[projects_json, updated_sender_ids],
    sensors=[projects_json_sensor, backfill_tx_by_project_sensor],
    resources={"mongo_resource": mongodb_resource_instance},
)
