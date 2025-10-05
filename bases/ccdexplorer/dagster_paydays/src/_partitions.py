import dagster as dg

partitions_def_from_staking = dg.DailyPartitionsDefinition(
    start_date="2022-06-24", end_offset=1
)
partitions_def_apy_averages = dg.StaticPartitionsDefinition(["30", "90", "180"])
partitions_def_daily_apy_averages = dg.MultiPartitionsDefinition(
    {"date": partitions_def_from_staking, "grouping": partitions_def_apy_averages}
)
