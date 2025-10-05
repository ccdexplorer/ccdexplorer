import dagster as dg
from ccdexplorer.mongodb import MongoDB, Collections
from ccdexplorer.tooter import Tooter

tooter: Tooter = Tooter()
mongodb: MongoDB = MongoDB(tooter, nearest=True)

instance = dg.DagsterInstance.get()
for x in mongodb.mainnet[Collections.paydays].find({}).sort("date", 1):
    print(x["date"])
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            dg.AssetKey("paydays_day_info"),
            metadata={"date": x["date"], "hash": x["_id"]},
        )
    )
