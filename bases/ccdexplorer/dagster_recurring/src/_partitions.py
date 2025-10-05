import dagster as dg
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
)
from ccdexplorer.tooter import Tooter

tooter: Tooter = Tooter()
mongodb: MongoDB = MongoDB(tooter, nearest=True)

net_partition = dg.StaticPartitionsDefinition(["mainnet", "testnet"])

token_list = [
    x["_id"].replace("w", "")
    for x in mongodb.mainnet[Collections.tokens_tags].find({"token_type": "fungible"})
    if x.get("get_price_from")
]
plt_list = [
    x["_id"] for x in mongodb.mainnet[Collections.plts_tags].find({}) if x.get("get_price_from")
]

token_list = token_list + plt_list
partitions_def_tokens = dg.StaticPartitionsDefinition(token_list)
