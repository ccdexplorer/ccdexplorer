from ccdexplorer.mongodb import Collections, MongoDB
from ccdexplorer.grpc_client import GRPCClient
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    write_queue_to_collection,
)

# tooter: Tooter = Tooter()
# mongodb: MongoDB = MongoDB(tooter, nearest=True)


def get_hash_from_date(date: str, mongodb: MongoDB) -> str:
    result = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": date})
    if result:
        return result["hash_for_last_block"]
    else:
        return ""


def perform_data_for_microccd(
    context, d_date: str, mongodb: MongoDB, grpcclient: GRPCClient
) -> dict:
    """ """

    queue = []
    analysis = AnalysisType.statistics_microccd

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)
    block_hash = get_hash_from_date(d_date, mongodb)
    cp = grpcclient.get_block_chain_parameters(block_hash)
    version = None
    if cp.v0:
        version = cp.v0
    if cp.v1:
        version = cp.v1
    if cp.v2:
        version = cp.v2
    if cp.v3:
        version = cp.v3
    dct = {}
    if version:
        # these are retrieved as string and stored as string in MongoDB,
        # as they are larger than 8-bit.
        GTU_denominator = version.micro_ccd_per_euro.denominator
        GTU_numerator = version.micro_ccd_per_euro.numerator

        NRG_denominator = version.euro_per_energy.denominator
        NRG_numerator = version.euro_per_energy.numerator

        dct = {
            "_id": _id,
            "type": analysis.value,
            "date": d_date,
            "GTU_denominator": GTU_denominator,
            "GTU_numerator": GTU_numerator,
            "NRG_denominator": NRG_denominator,
            "NRG_numerator": NRG_numerator,
        }

        queue.append(
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        )
        if len(queue) > 0:
            _ = mongodb.mainnet[Collections.statistics].bulk_write(queue)
            queue = []

        context.log.info(f"info: {dct}")
        write_queue_to_collection(mongodb, queue, analysis)
    return {"dct": dct}
