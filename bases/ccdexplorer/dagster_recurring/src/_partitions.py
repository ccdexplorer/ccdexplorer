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
    """Tokens and PLTs that currently have a price source configured.

    The key format is unchanged from the static list this replaces, so existing
    materialisations stay attached to the same partitions.
    """
    mongodb = shared_mongodb()
    tokens = [
        x["_id"].replace("w", "")
        for x in mongodb.mainnet[Collections.tokens_tags].find({"token_type": "fungible"})
        if x.get("get_price_from")
    ]
    plts = [
        x["_id"] for x in mongodb.mainnet[Collections.plts_tags].find({}) if x.get("get_price_from")
    ]
    return tokens + plts
