import dagster as dg
from ccdexplorer.mongodb import Collections

from ._resources import shared_mongodb

# The token partitions below still need a query at import, but it now reuses
# the process's shared client instead of building a second one. It also stops
# introducing itself to mongod as "dagster_paydays": this is the nightrunner
# code location, and the wrong appName sent its connections to the wrong
# service in every attribution.
mongodb = shared_mongodb()

token_list = [
    x["_id"].replace("w", "")
    for x in mongodb.mainnet[Collections.tokens_tags].find({"token_type": "fungible"})
    if x.get("get_price_from")
]
plt_list = [
    x["_id"] for x in mongodb.mainnet[Collections.plts_tags].find({}) if x.get("get_price_from")
]

token_list = plt_list + token_list
partitions_def_tokens = dg.StaticPartitionsDefinition(token_list)

partitions_def_from_genesis = dg.DailyPartitionsDefinition(start_date="2021-06-09")
partitions_def_from_trading = dg.DailyPartitionsDefinition(start_date="2022-02-10")
partitions_def_from_staking = dg.DailyPartitionsDefinition(start_date="2022-06-23")
partitions_def_from_plts = dg.DailyPartitionsDefinition(start_date="2025-09-22")
partitions_def_from_agent_registry = dg.DailyPartitionsDefinition(start_date="2026-05-27")
partitions_def_grouping = dg.StaticPartitionsDefinition(["daily", "weekly", "monthly"])


partitions_def_grouping_from_genesis = dg.MultiPartitionsDefinition(
    {"date": partitions_def_from_genesis, "grouping": partitions_def_grouping}
)
