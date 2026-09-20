import dagster as dg
from ccdexplorer.mongodb import Collections

from ._resources import shared_mongodb

# The token partitions below still need a query at import, but it reuses the
# process's shared client instead of building a second one. It also stops
# introducing itself to mongod as "dagster_paydays": this is the nightrunner
# code location, and the wrong appName sent its connections to the wrong
# service in every attribution.


def current_token_keys() -> list[str]:
    """The symbols a historical rate has to be fetched for.

    `get_price_from` names the symbol that prices a token; it is not a flag.
    tETH carries "ETH" because tETH is worth whatever ETH is worth, and every
    consumer of exchange_rates_historical reads it by symbol --
    update_account_graph does `exchange_rates_historical.get("CCD", ...)`,
    never by a token's own id.

    Reading the field as a flag and keying on `_id` put ten partitions here
    that nothing can do anything with: tETH, tUSDC, tWBTC and the rest have no
    CoinGecko id, so coingecko_historical finds nothing to request. They do not
    fail, because perform_historical_rates_update returns True for anything
    outside its allowlist, but accounts_repo still asks for a run per partition
    every night, and each of those pays a process start and a definitions
    import to do nothing.

    Sorted, so the partition set only changes when the configured symbols do.
    """
    mongodb = shared_mongodb()
    symbols = {
        x["get_price_from"]
        for x in mongodb.mainnet[Collections.tokens_tags].find({"token_type": "fungible"})
        if x.get("get_price_from")
    }
    symbols |= {
        x["get_price_from"]
        for x in mongodb.mainnet[Collections.plts_tags].find({})
        if x.get("get_price_from")
    }
    return sorted(symbols)


partitions_def_tokens = dg.StaticPartitionsDefinition(current_token_keys())

partitions_def_from_genesis = dg.DailyPartitionsDefinition(start_date="2021-06-09")
partitions_def_from_trading = dg.DailyPartitionsDefinition(start_date="2022-02-10")
partitions_def_from_staking = dg.DailyPartitionsDefinition(start_date="2022-06-23")
partitions_def_from_plts = dg.DailyPartitionsDefinition(start_date="2025-09-22")
partitions_def_from_agent_registry = dg.DailyPartitionsDefinition(start_date="2026-05-27")
partitions_def_grouping = dg.StaticPartitionsDefinition(["daily", "weekly", "monthly"])


partitions_def_grouping_from_genesis = dg.MultiPartitionsDefinition(
    {"date": partitions_def_from_genesis, "grouping": partitions_def_grouping}
)
