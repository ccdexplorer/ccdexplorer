import dagster as dg
from ccdexplorer.mongodb import Collections

from ._resources import shared_mongodb

net_partition = dg.StaticPartitionsDefinition(["mainnet", "testnet", "devnet"])
hourly_partition = dg.HourlyPartitionsDefinition(
    start_date="2021-06-09-09", timezone="UTC", fmt="%Y-%m-%d-%H"
)


partitions_def_hourly_net = dg.MultiPartitionsDefinition(
    {"datetime": hourly_partition, "net": net_partition}
)


# Tokens to fetch spot prices for, as dynamic partitions.
#
# This used to be a static list built at import from two Mongo queries, so every
# process importing these definitions -- each run worker, for every job in this
# code location, several a minute -- opened a fresh client just to list tokens,
# whether or not the run had anything to do with prices. Dynamic partitions need
# nothing at import: the spot_retrieval schedule adds the current keys on each
# tick, from the long-lived code server.
partitions_def_tokens = dg.DynamicPartitionsDefinition(name="spot_retrieval_tokens")


def current_token_keys() -> list[str]:
    """The symbols a USD rate has to be fetched for.

    `get_price_from` names the symbol that prices a token; it is not a flag.
    wETH and tETH both carry "ETH", because both are worth whatever ETH is
    worth. Consumers look a rate up by that symbol and never by the token's own
    id -- account_v2 does `exchange_rates[token_tag["get_price_from"]]` -- so
    the symbols are what this needs to fetch.

    Reading the field as a flag and keying on `_id` instead had it fetching
    rates nobody reads (tETH, tUSDC, tWBTC and seven more, none of which any
    price source lists, so each one failed the run every ten minutes) while
    never fetching three that consumers do read: BNB, MATIC and UMB. Holders of
    tBNB, tPOL and tUMB had no USD value at all.

    Dropping the `_id`-based keys also retires `.replace("w", "")`, which
    removed every "w" in an id rather than a wrapped-token prefix.

    Sorted so the partition range the schedule builds from the stored order
    stays stable as symbols are added.
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
