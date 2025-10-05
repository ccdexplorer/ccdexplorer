from ccdexplorer.mongodb import MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import AnalysisType, write_queue_to_collection, get_df_from_git


def perform_data_for_daily_limits(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB
) -> dict:
    analysis = AnalysisType.statistics_daily_limits

    queue = []
    df = get_df_from_git(commits_by_day[d_date])

    if df is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)

    # the actual analysis
    df.sort_values(by="total_balance", inplace=True, ascending=False)
    df.reset_index(inplace=True)

    dct = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "amount_to_make_top_100": (df.iloc[100]["total_balance"] if len(df) > 100 else 0.0),
        "amount_to_make_top_250": (df.iloc[250]["total_balance"] if len(df) > 250 else 0.0),
    }

    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )

    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(dct)
    return {"message": dct}
