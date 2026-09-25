# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
import datetime as dt
import json
import time
from collections import Counter
from datetime import timedelta
from enum import Enum
from typing import Any

from ccdexplorer.ccdexplorer_api.app.utils import await_await, apply_docstring_router_wrappers
import dateutil
import grpc
import httpx2 as httpx
import pandas as pd
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_ArInfo,
    CCD_BlockInfo,
    CCD_BlockItemSummary,
    CCD_IpInfo,
    CCD_WinningBaker,
)
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
    net_db,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from grpc._channel import _MultiThreadedRendezvous

from ccdexplorer.env import API_KEY_HEADER as API_KEY_HEADER_NAME
from fastapi.security.api_key import APIKeyHeader
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_exchange_rates,
    get_exchange_rates_historical,
    get_grpcclient,
    get_mongo_db,
    get_mongo_motor,
)

router = APIRouter(tags=["Misc"], prefix="/v2")
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME)
apply_docstring_router_wrappers(router)


class ExchangePeriod(Enum):
    h1 = 1
    d1 = 24
    d7 = 24 * 7
    d30 = 24 * 30
    d90 = 24 * 90
    all = 24 * 365 * 1000  # 1000 years


@router.get(
    "/{net}/misc/tx-data/{project_id}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_tx_data_for_project(
    request: Request,
    net: str,
    project_id: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get transactions counts for projects (and the chain).
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    dates_to_include = generate_dates_from_start_until_end(start_date, end_date)
    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": "statistics_transaction_types"}},
        {"$match": {"project": project_id}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    return JSONResponse([x for x in result])


@router.get(
    "/{net}/misc/today-in/{date}",
    response_class=JSONResponse,
)
async def get_today_in_data(
    request: Request,
    net: str,
    date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get all interesting facts for this day.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = net_db(mongomotor, net)

    return_result = {"date": date}

    # day data
    result = await db_to_use[Collections.blocks_per_day].find_one({"date": date})
    if result:
        return_result["day_data"] = result

        pipeline = [
            {"$match": {"account_transaction": {"$exists": True}}},
            {
                "$match": {
                    "block_info.height": {
                        "$gte": result["height_for_first_block"],
                        "$lte": result["height_for_last_block"],
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "tx_count": {"$count": {}},
                    "fee_for_day": {"$sum": "$account_transaction.cost"},
                }
            },
        ]
        result = await await_await(db_to_use, Collections.transactions, pipeline)
        return_result["tx_count"] = result[0]["tx_count"]
        return_result["fee_for_day"] = result[0]["fee_for_day"]

    # logged events by contract
    pipeline = [
        {"$match": {"tx_info.date": date}},
        {"$group": {"_id": "$event_info.contract", "count": {"$count": {}}}},
        {"$sort": {"count": -1}},
    ]
    result = await await_await(db_to_use, Collections.tokens_logged_events_v2, pipeline)
    return_result["logged_events_by_contract"] = result

    # tx types
    pipeline = [
        {"$match": {"date": date}},
        {"$match": {"type": "statistics_transaction_types"}},
        {"$match": {"project": "all"}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    if len(result) > 0:
        if "tx_type_counts" in result[0]:
            result = result[0]["tx_type_counts"]
        else:
            result = {}
    else:
        result = {}
    return_result["tx_types"] = result

    # impacted addresses
    pipeline = [
        {"$match": {"date": date}},
        {  # this filters out account rewards, as they are special events
            "$match": {"tx_hash": {"$exists": True}},
        },
        {"$group": {"_id": "$impacted_address_canonical", "count": {"$count": {}}}},
        {"$sort": {"count": -1}},
    ]
    impacted_addresses_result = await await_await(
        db_to_use, Collections.impacted_addresses, pipeline
    )

    # filter out public keys that do not "exist"
    public_keys_in_results = [x["_id"] for x in impacted_addresses_result if len(x["_id"]) == 64]
    pipeline = [
        {"$match": {"public_key": {"$in": public_keys_in_results}}},
        {"$project": {"_id": 0, "public_key": 1}},
        {"$group": {"_id": "$public_key"}},  # Group by public_key to remove duplicates
    ]
    result = await await_await(db_to_use, Collections.cis5_public_keys_info, pipeline)
    recognized_public_keys = [x["_id"] for x in result]
    final_result = [
        x
        for x in impacted_addresses_result
        if ((len(x["_id"]) < 64) or (x["_id"] in recognized_public_keys))
    ]
    return_result["impacted_addresses"] = final_result

    # validator suspended and primed for...
    pipeline = [{"$match": {"date": date}}]
    primed_suspended = await await_await(db_to_use, Collections.validator_logs, pipeline)
    return_result["primed_suspended"] = primed_suspended

    return return_result


@router.get(
    "/{net}/misc/cns-domain/{tokenID}",
    response_class=JSONResponse,
)
async def get_bictory_cns_domain(
    request: Request,
    net: str,
    tokenID: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get possible Bictory CNS Domain name from tokenId.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = net_db(mongomotor, net)
    result = await db_to_use[Collections.cns_domains].find_one({"_id": tokenID})
    if result:
        return JSONResponse({"domain_name": result["domain_name"]})
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Domain name for tokenID {tokenID} is not found on {net}.",
        )


@router.get(
    "/{net}/misc/credential-issuers",
    response_class=JSONResponse,
)
async def get_credential_issuers(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get credential issuers for the requested net.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = net_db(mongomotor, net)
    result = await db_to_use[Collections.credentials_issuers].find({}).to_list(length=None)
    if result:
        credential_issuers = [x["_id"] for x in result]
        return JSONResponse(credential_issuers)
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Error getting credential issuers on {net}.",
        )


@router.get(
    "/{net}/misc/exchange-rates",
    response_class=JSONResponse,
)
async def get_spot_exchange_rates(
    request: Request,
    net: str,
    exchange_rates: dict = Depends(get_exchange_rates),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get spot exchange rates.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    return exchange_rates


#: The chain sets its CCD/EUR rate by update transaction every thirty minutes,
#: and has done since 2021. That is the only complete intraday price history
#: there is -- the daily forex job writes one point a day, and the spot job
#: only started keeping intraday recently and prunes after two weeks.
CCD_PRICE_UPDATE_TYPE = "micro_ccd_per_euro_update"

#: Beyond about a week, thirty-minute resolution is thousands of points nobody
#: can see on a chart. Past this the daily on-chain values are used instead --
#: the same rate, already aggregated once a day by the nightrunner.
CCD_PRICE_INTRADAY_MAX_HOURS = 168

#: A chart is about a thousand pixels wide. More points than this is bytes
#: nobody can perceive, so the series is thinned to fit before it is sent.
CCD_PRICE_MAX_POINTS = 500

#: A year. Further back exists on chain, but nothing asks for it yet.
CCD_PRICE_MAX_HOURS = 8760


def _eur_per_ccd(payload: dict) -> float | None:
    """microCCD per euro, as the chain stores it, inverted into EUR per CCD.

    Stored as strings because the numerator outgrows 64 bits.
    """
    try:
        numerator = int(payload["numerator"])
        denominator = int(payload["denominator"])
    except (KeyError, TypeError, ValueError):
        return None
    if numerator <= 0:
        return None
    return 1_000_000 * denominator / numerator


def _append_spot(points: list, spot_usd, spot_at, eur_usd) -> bool:
    """Close the series on the current market price. Returns whether it did.

    Every price chart ends on the spot, in the line as well as the headline.
    The chain rate is up to thirty minutes old and is a fee rate besides, so a
    chart drawn only from it stops short of the price a reader would look up --
    and the headline would then disagree with the end of its own line.

    Appended rather than substituted: the chain points before it are what they
    were. And only when it is genuinely newer, because a spot reading that lags
    the chain would bend the last segment backwards in time.
    """
    if not spot_usd or not spot_at:
        return False
    if spot_at.tzinfo is None:
        spot_at = spot_at.replace(tzinfo=dt.timezone.utc)
    if points and spot_at <= points[-1]["at"]:
        return False
    points.append(
        {
            "at": spot_at,
            "eur": (spot_usd / eur_usd) if eur_usd else None,
            "usd": spot_usd,
        }
    )
    return True


def _thin(points: list, limit: int) -> list:
    """Keep at most ``limit`` points, evenly spaced, always including the last.

    The last one matters more than the spacing: it is what the headline change
    is measured against, and dropping it would make the chart disagree with
    the number printed above it.
    """
    if len(points) <= limit:
        return points
    step = len(points) / limit
    thinned = [points[int(index * step)] for index in range(limit)]
    if thinned[-1] is not points[-1]:
        thinned[-1] = points[-1]
    return thinned


@router.get("/{net}/misc/ccd-price/last/{hours}", response_class=JSONResponse)
async def get_ccd_price_series(
    request: Request,
    net: str,
    hours: int,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    exchange_rates: dict = Depends(get_exchange_rates),
    exchange_rates_historical: dict = Depends(get_exchange_rates_historical),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return the CCD price over the last ``hours``, from the chain's own rate.

    The chain carries a CCD/EUR rate, set by update transaction every thirty
    minutes since 2021, which it uses to price transaction fees. It is not a
    market feed -- it is governance-set -- but it tracks one closely, and it is
    the only complete intraday history available: the daily forex job writes a
    single point per day, and that point is a snapshot taken shortly after
    midnight, so by mid-morning it is hours stale.

    Below a week the individual updates are returned. Above it, the daily
    on-chain values the nightrunner already aggregates, because thirty-minute
    resolution over a year is seventeen thousand points nobody can see.

    Prices are converted to USD here rather than by the caller, so a chart
    stays a renderer. ``spot_usd`` is the market price from the spot job and
    will differ from the last point by a few tenths of a percent -- one is what
    the chain charges fees at, the other is what an exchange quotes.
    """
    if net != "mainnet":
        raise HTTPException(
            status_code=422,
            detail="CCD only has a price on mainnet.",
        )

    hours = max(1, min(int(hours), CCD_PRICE_MAX_HOURS))
    now = dt.datetime.now(dt.timezone.utc)
    cutoff = now - timedelta(hours=hours)
    db_to_use = net_db(mongomotor, net)

    points: list[dict] = []
    if hours <= CCD_PRICE_INTRADAY_MAX_HOURS:
        source = "chain-30min"
        # Sorted by height using the (type.contents, block_info.height) index,
        # then trimmed by time, rather than scanning a slot_time range that
        # would walk every transaction in the window -- around a hundred
        # thousand a day -- to find the forty-eight that are rate updates.
        limit = hours * 2 + 10
        cursor = (
            db_to_use[Collections.transactions]
            .find(
                {"type.contents": CCD_PRICE_UPDATE_TYPE},
                {"block_info.slot_time": 1, f"update.payload.{CCD_PRICE_UPDATE_TYPE}": 1},
            )
            .sort("block_info.height", -1)
            .limit(limit)
        )
        for doc in await cursor.to_list(length=limit):
            at = (doc.get("block_info") or {}).get("slot_time")
            eur = _eur_per_ccd(
                (doc.get("update") or {}).get("payload", {}).get(CCD_PRICE_UPDATE_TYPE, {})
            )
            if at is None or eur is None:
                continue
            if at.tzinfo is None:
                at = at.replace(tzinfo=dt.timezone.utc)
            if at >= cutoff:
                points.append({"at": at, "eur": eur})
        points.sort(key=lambda point: point["at"])
    else:
        source = "chain-daily"
        start = cutoff.strftime("%Y-%m-%d")
        cursor = db_to_use[Collections.statistics].find(
            {"type": "statistics_microccd", "date": {"$gte": start}},
            {"date": 1, "GTU_numerator": 1, "GTU_denominator": 1},
        )
        for doc in await cursor.to_list(length=None):
            eur = _eur_per_ccd(
                {"numerator": doc.get("GTU_numerator"), "denominator": doc.get("GTU_denominator")}
            )
            if eur is None:
                continue
            points.append(
                {
                    "at": dt.datetime.strptime(doc["date"], "%Y-%m-%d").replace(
                        tzinfo=dt.timezone.utc
                    ),
                    "eur": eur,
                }
            )
        points.sort(key=lambda point: point["at"])

        # The nightrunner only writes a day once it is over, so the daily
        # series stops at yesterday. Left alone, a 90-day chart would end a
        # day short while the 24-hour one is current, and the headline change
        # would be measured to yesterday. The newest rate update closes it.
        newest = await (
            db_to_use[Collections.transactions]
            .find(
                {"type.contents": CCD_PRICE_UPDATE_TYPE},
                {"block_info.slot_time": 1, f"update.payload.{CCD_PRICE_UPDATE_TYPE}": 1},
            )
            .sort("block_info.height", -1)
            .limit(1)
            .to_list(length=1)
        )
        for doc in newest:
            at = (doc.get("block_info") or {}).get("slot_time")
            eur = _eur_per_ccd(
                (doc.get("update") or {}).get("payload", {}).get(CCD_PRICE_UPDATE_TYPE, {})
            )
            if at is None or eur is None:
                continue
            if at.tzinfo is None:
                at = at.replace(tzinfo=dt.timezone.utc)
            if not points or at > points[-1]["at"]:
                points.append({"at": at, "eur": eur})

    if not points:
        raise HTTPException(
            status_code=404,
            detail=f"No CCD price points found for the last {hours} hours on {net}.",
        )

    # USD per EUR, from the most recent daily forex row we hold.
    eur_rates = (exchange_rates_historical or {}).get("EUR") or {}
    eur_usd = eur_rates.get(max(eur_rates)) if eur_rates else None

    # Every price chart ends on the current spot, in the line as well as the
    # headline. The chain rate is up to thirty minutes old and is a fee rate
    # besides, so a chart drawn only from it stops short of the price anyone
    # reading it would look up -- and the headline would then disagree with
    # the end of its own line. Appended rather than substituted: the chain
    # points before it are what they were.
    spot = (exchange_rates or {}).get("CCD") or {}
    spot_usd = spot.get("rate")
    spot_at = spot.get("timestamp")
    ends_with_spot = _append_spot(points, spot_usd, spot_at, eur_usd)

    points = _thin(points, CCD_PRICE_MAX_POINTS)
    series = [
        {
            "at": point["at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "eur": point["eur"],
            # The spot point carries its own USD price; the chain points are
            # converted from EUR.
            "usd": point.get("usd")
            if point.get("usd") is not None
            else ((point["eur"] * eur_usd) if eur_usd and point["eur"] else None),
        }
        for point in points
    ]
    usd_values = [row["usd"] for row in series if row["usd"] is not None]
    first, last = series[0], series[-1]

    return {
        "net": net,
        "hours": hours,
        "source": source,
        "points": len(series),
        "ends_with_spot": ends_with_spot,
        "series": series,
        "first_usd": first["usd"],
        "last_usd": last["usd"],
        "change_pct": (
            ((last["usd"] / first["usd"]) - 1) * 100 if first["usd"] and last["usd"] else None
        ),
        "high_usd": max(usd_values) if usd_values else None,
        "low_usd": min(usd_values) if usd_values else None,
        "eur_usd": eur_usd,
        # The market price, for the headline. It disagrees with the last point
        # by a few tenths of a percent, and that is the honest difference
        # between a fee rate and an exchange quote.
        "spot_usd": spot_usd,
        "spot_at": spot_at.strftime("%Y-%m-%dT%H:%M:%SZ") if spot_at else None,
    }


#: Kraken's public OHLC feed, which needs no key. CCD/USD directly, so no
#: stablecoin leg, and its intervals happen to be exactly the ones a candle
#: chart wants. This is the only third party anything in the API calls: the
#: chain rate above owes nothing to anyone, and this is what somebody actually
#: paid, with volume behind it.
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_CCD_PAIR = "CCDUSD"

#: minutes, as Kraken names them.
KRAKEN_INTERVALS = {"15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}

#: Kraken returns 720 candles whatever you ask for; this is how many reach the
#: chart. Enough to read, few enough that each candle has width.
KRAKEN_MAX_BARS = 120

#: Served from the last good response for this long if Kraken is unreachable.
#: A chart that is an hour stale beats no chart, and this is the only part of
#: the site that depends on somebody else's uptime.
KRAKEN_STALE_SECONDS = 3600

_kraken_cache: dict[str, tuple[float, dict]] = {}


async def _kraken_ohlc(interval: str) -> dict | None:
    """Candles from Kraken, cached, falling back to the last good response."""
    minutes = KRAKEN_INTERVALS[interval]
    cached = _kraken_cache.get(interval)
    now = time.time()
    if cached and now - cached[0] < 60:
        return cached[1]

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                KRAKEN_OHLC_URL, params={"pair": KRAKEN_CCD_PAIR, "interval": minutes}
            )
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        rows = next(value for key, value in payload["result"].items() if key != "last")
    except Exception as error:  # Kraken is not ours; degrade rather than fail
        print(f"kraken ohlc {interval}: {error}")
        if cached and now - cached[0] < KRAKEN_STALE_SECONDS:
            return cached[1]
        return None

    candles = [
        {
            "at": dt.datetime.fromtimestamp(int(row[0]), dt.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[6]),
            "trades": int(row[7]),
        }
        for row in rows
    ][-KRAKEN_MAX_BARS:]
    result = {"candles": candles}
    _kraken_cache[interval] = (now, result)
    return result


@router.get("/{net}/misc/ccd-ohlc/{interval}", response_class=JSONResponse)
async def get_ccd_ohlc(
    request: Request,
    net: str,
    interval: str,
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """CCD/USD candles from Kraken, with volume.

    Distinct from ccd-price above, which draws the chain's own fee rate. This
    is one exchange's order book: what people paid, with volume, but only back
    as far as Kraken keeps and only while Kraken is up.
    """
    if net != "mainnet":
        raise HTTPException(status_code=422, detail="CCD only trades on mainnet.")
    if interval not in KRAKEN_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"interval must be one of {', '.join(KRAKEN_INTERVALS)}.",
        )

    result = await _kraken_ohlc(interval)
    if not result or not result["candles"]:
        raise HTTPException(status_code=503, detail="No candles available from Kraken.")

    candles = result["candles"]
    first, last = candles[0], candles[-1]
    change = ((last["close"] / first["open"]) - 1) * 100 if first["open"] else None
    empty = sum(1 for c in candles if c["volume"] == 0)
    return {
        "net": net,
        "venue": "Kraken",
        "pair": "CCD/USD",
        "interval": interval,
        "candles": candles,
        "bars": len(candles),
        # A candle with no trades is a flat dash, not a gap. Reported so a
        # caller can say how much of the window was actually traded.
        "bars_without_trades": empty,
        "open": first["open"],
        "high": max(c["high"] for c in candles),
        "low": min(c["low"] for c in candles),
        "close": last["close"],
        "change_pct": change,
        "volume": sum(c["volume"] for c in candles),
        "trades": sum(c["trades"] for c in candles),
    }


@router.get("/{net}/misc/protocol-updates")
async def get_protocol_updates(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_BlockItemSummary]:
    """
    Endpoint to get protocol update transactions for the requested net.
    """
    db_to_use = net_db(mongomotor, net)
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    pipeline = [
        {"$match": {"update.payload.protocol_update": {"$exists": True}}},
        {"$sort": {"block_info.height": -1}},
    ]

    result = await await_await(db_to_use, Collections.transactions, pipeline)
    return result


@router.get("/{net}/misc/current-node")
async def get_current_node(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get the identity (host:port) of the gRPC node currently
    selected for the requested net. Best-effort: GRPCClient can rotate hosts
    on failure, so this reflects whichever host most recently served (or was
    last attempted for) a call on this net -- intended for diagnostics, not
    as a guarantee of which node served any specific prior request.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )
    return {"node": grpcclient.current_node(NET(net))}


@router.get("/{net}/misc/identity-providers")
async def get_identity_providers(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_IpInfo]:
    """
    Endpoint to get identity providers for the requested net.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    identity_providers = grpcclient.get_identity_providers("last_final", NET(net))
    return identity_providers


@router.get("/{net}/misc/anonymity-revokers")
async def get_anonymity_revokers(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[CCD_ArInfo]:
    """
    Endpoint to get anonymity revokers for the requested net.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    anonymity_revokers = grpcclient.get_anonymity_revokers("last_final", NET(net))
    return anonymity_revokers


@router.get(
    "/{net}/misc/labeled-accounts",
    response_class=JSONResponse,
)
async def get_labeled_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get community labeled accounts.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    # labeled accounts only exist for mainnet
    # db_to_use = mongomotor.mainnet
    db_utilities = mongomotor.utilities

    result = await db_utilities[CollectionsUtilities.labeled_accounts].find({}).to_list(length=None)
    labeled_accounts = {}
    for r in result:
        current_group = labeled_accounts.get(r["label_group"], {})
        current_group[r["_id"]] = r["label"]
        labeled_accounts[r["label_group"]] = current_group

    result = (
        await db_utilities[CollectionsUtilities.labeled_accounts_metadata]
        .find({})
        .to_list(length=None)
    )

    colors = {}
    descriptions = {}
    for r in result:
        colors[r["_id"]] = r.get("color")
        descriptions[r["_id"]] = r.get("description")

    tags = {
        "labels": labeled_accounts,
        "colors": colors,
        "descriptions": descriptions,
    }

    return JSONResponse(tags)


@router.get(
    "/{net}/misc/community-labeled-accounts",
    response_class=JSONResponse,
)
async def get_community_labeled_accounts(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get community labeled accounts (indexes).
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    # labeled accounts only exist for mainnet
    db_to_use = mongomotor.mainnet
    db_utilities = mongomotor.utilities

    result = await db_utilities[CollectionsUtilities.labeled_accounts].find({}).to_list(length=None)
    labeled_accounts = {}
    for r in result:
        current_group = labeled_accounts.get(r["label_group"], {})
        if "account_index" in r:
            current_group[r["account_index"]] = r["label"]
        else:
            current_group[r["_id"]] = r["label"]
        labeled_accounts[r["label_group"]] = current_group

    result = (
        await db_utilities[CollectionsUtilities.labeled_accounts_metadata]
        .find({})
        .to_list(length=None)
    )

    colors = {}
    descriptions = {}
    for r in result:
        colors[r["_id"]] = r.get("color")
        descriptions[r["_id"]] = r.get("description")

    ### insert projects into tags
    # display_names
    projects_display_names = {
        x["_id"]: x["display_name"]
        for x in await db_utilities[CollectionsUtilities.projects].find({}).to_list(length=None)
    }
    # account addresses
    project_account_addresses = (
        await db_to_use[Collections.projects].find({"type": "account_address"}).to_list(length=None)
    )

    dd = {}
    for paa in project_account_addresses:
        if "display_name" in paa:
            dd[paa["account_index"]] = paa["display_name"]
        else:
            dd[paa["account_index"]] = projects_display_names[paa["project_id"]]
    labeled_accounts["projects"] = dd

    # contract addresses
    project_contract_addresses = (
        await db_to_use[Collections.projects]
        .find({"type": "contract_address"})
        .to_list(length=None)
    )

    dd = {}
    for paa in project_contract_addresses:
        if "display_name" in paa:
            dd[paa["contract_address"]] = paa["display_name"]
        else:
            dd[paa["contract_address"]] = projects_display_names[paa["project_id"]]
    labeled_accounts["contracts"].update(dd)

    labels_melt = {}
    for label_group in labeled_accounts.keys():
        label_group_color = colors[label_group]
        for address, tag in labeled_accounts[label_group].items():
            labels_melt[address] = {
                "label": tag,
                "group": label_group,
                "color": label_group_color,
            }

    colors = {}
    descriptions = {}
    for r in result:
        colors[r["_id"]] = r.get("color")
        descriptions[r["_id"]] = r.get("description")

    del labeled_accounts["projects"]
    tags = {
        "labels_melt": labels_melt,
        "labeled_accounts": labeled_accounts,
        "colors": colors,
        "descriptions": descriptions,
    }

    return JSONResponse(tags)


def generate_dates_from_start_until_end(start: str, end: str):
    start_date = dateutil.parser.parse(start)
    end_date = dateutil.parser.parse(end)
    date_range = []

    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return date_range


@router.get(
    "/{net}/misc/statistics-chain/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_data_for_chain_analysis(
    request: Request,
    net: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get data for analysis.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    dates_to_include = generate_dates_from_start_until_end(start_date, end_date)
    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": "statistics_transaction_types"}},
        {"$match": {"project": "all"}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0, "project": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    return JSONResponse([x for x in result])


@router.get(
    "/{net}/misc/statistics/{analysis}/{start_date}/{end_date}",
    response_class=JSONResponse,
)
async def get_data_for_analysis(
    request: Request,
    net: str,
    analysis: str,
    start_date: str,
    end_date: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get data for analysis.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    try:
        dates_to_include = generate_dates_from_start_until_end(start_date, end_date)
    except:  # noqa: E722
        raise HTTPException(
            status_code=422,
            detail="No valid date(s) given.",
        )

    pipeline = [
        {"$match": {"date": {"$in": dates_to_include}}},
        {"$match": {"type": analysis}},
        {"$project": {"_id": 0, "type": 0, "usecase": 0}},
        {"$sort": {"date": 1}},
    ]
    result = await await_await(mongomotor.mainnet, Collections.statistics, pipeline)
    return JSONResponse([x for x in result])


@router.get(
    "/{net}/misc/validator-nodes/count",
    response_class=JSONResponse,
)
async def get_nodes_count(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> int:
    """
    Endpoint to get count of all validator nodes.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = mongomotor.mainnet
    result = await db_to_use[Collections.paydays_v2_current_payday].count_documents({})
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail="Error requesting nodes for {net}.",
        )


@router.get(
    "/{net}/misc/node/{node_id}",
    response_class=JSONResponse,
)
async def get_node_info(
    request: Request,
    net: str,
    node_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> JSONResponse:
    """
    Endpoint to get node information for a given node id.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = mongomotor.mainnet
    result = await db_to_use[Collections.dashboard_nodes].find_one({"_id": node_id})
    if result:
        return result
    else:
        raise HTTPException(
            status_code=404,
            detail="Error requesting nodes for {net}.",
        )


@router.get(
    "/{net}/misc/projects/all-ids",
    response_class=JSONResponse,
)
async def get_all_project_ids(
    request: Request,
    net: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    project_ids = {}
    result = await mongomotor.utilities[CollectionsUtilities.projects].find({}).to_list(length=None)
    for project in result:
        project_ids[project["_id"]] = project

    return project_ids


@router.get(
    "/{net}/misc/projects/{project_id}",
    response_class=JSONResponse,
)
async def get_project_id(
    request: Request,
    net: str,
    project_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> Any | None:
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    result = await mongomotor.utilities[CollectionsUtilities.projects].find_one({"_id": project_id})

    return result


@router.get(
    "/{net}/misc/projects/{project_id}/addresses",
    response_class=JSONResponse,
)
async def get_project_addresses(
    request: Request,
    net: str,
    project_id: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    db_to_use = net_db(mongomotor, net)
    project_addresses = (
        await db_to_use[Collections.projects].find({"project_id": project_id}).to_list(length=None)
    )

    return project_addresses


@router.get(
    "/misc/release-notes",
    response_class=JSONResponse,
)
async def get_release_notes(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    api_key: str = Security(API_KEY_HEADER),
) -> list:
    db_to_use = mongomotor.utilities
    release_notes = list(
        reversed(await db_to_use[CollectionsUtilities.release_notes].find({}).to_list(length=None))
    )

    return release_notes


@router.get(
    "/{net}/misc/consensus-detailed-status",
    response_class=JSONResponse,
)
async def get_consensus_detailed_status(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get consensus detailed status for the requested net.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    try:
        cds = grpcclient.get_consensus_detailed_status(net=NET(net))
    except Exception as e:  # noqa: E722
        raise HTTPException(
            status_code=404,
            detail=f"Error getting consensus detailed status on {net}. {e}",
        )

    return cds.model_dump(exclude_none=True)


async def get_exchange_txs_as_receiver(
    exchanges_canonical, start_block, end_block, mongomotor: MongoMotor
):
    pipeline = [
        {"$match": {"receiver_canonical": {"$in": exchanges_canonical}}},
        {"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}},
    ]
    txs_as_receiver = await await_await(
        mongomotor.mainnet, Collections.involved_accounts_transfer, pipeline
    )
    return txs_as_receiver


async def get_exchange_txs_as_sender(
    exchanges_canonical, start_block, end_block, mongomotor: MongoMotor
):
    pipeline = [
        {"$match": {"sender_canonical": {"$in": exchanges_canonical}}},
        {"$match": {"block_height": {"$gt": start_block, "$lte": end_block}}},
    ]
    txs_as_sender = await await_await(
        mongomotor.mainnet, Collections.involved_accounts_transfer, pipeline
    )
    return txs_as_sender


@router.get("/{net}/sellers-and-buyers/{period}")
async def get_sellers_and_buyers_for_period(
    request: Request,
    net: str,
    period: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
    tags: dict = Depends(get_labeled_accounts),
):
    """
    This endpoint retrieves sellers and buyers to known exchange accounts.

    Currently not in use.

    Possible values for period (in hours):
    h1 = 1
    d1 = 24
    d7 = 24 * 7
    d30 = 24 * 30
    d90 = 24 * 90
    all = 24 * 365 * 1000

    """
    tags_json = json.loads(tags.body)
    exchanges_canonical = [x[:29] for x in tags_json["labels"]["exchanges"].keys()]

    now = dt.datetime.now().now().astimezone(dt.UTC)

    try:
        period = ExchangePeriod[period]
    except KeyError:
        raise HTTPException(
            status_code=422,
            detail="Invalid period given.",
        )

    all_txs = []
    start_time = now - dt.timedelta(hours=period.value)
    start_time_block = (
        await mongomotor.mainnet[Collections.blocks]
        .find({"slot_time": {"$lt": start_time}})
        .sort("slot_time", -1)
        .to_list(length=1)
    )
    if len(start_time_block) > 0:
        start_block = start_time_block[0]["height"]
    else:
        start_block = 0

    end_block = 1_000_000_000

    txs_as_sender = await get_exchange_txs_as_sender(
        exchanges_canonical, start_block, end_block, mongomotor
    )
    txs_as_receiver = await get_exchange_txs_as_receiver(
        exchanges_canonical, start_block, end_block, mongomotor
    )

    all_txs.extend(txs_as_sender)
    all_txs.extend(txs_as_receiver)

    no_inter_exch_txs = [
        x
        for x in all_txs
        if not (
            (x["sender_canonical"] in exchanges_canonical)
            and (x["receiver_canonical"] in exchanges_canonical)
        )
    ]

    buyers = []
    sellers = []

    df = pd.DataFrame(no_inter_exch_txs)
    if len(df) > 0:
        f_sellers = df["receiver_canonical"].isin(exchanges_canonical)
        f_buyers = df["sender_canonical"].isin(exchanges_canonical)

        df_buyers_gb = df[f_buyers].groupby(["receiver_canonical"])
        df_sellers_gb = df[f_sellers].groupby(["sender_canonical"])

        buyers_address = set(df_buyers_gb.groups.keys())
        sellers_address = set(df_sellers_gb.groups.keys())

        all_addresses = list(buyers_address | sellers_address)

        for address in all_addresses:
            if address in buyers_address:
                group = df_buyers_gb.get_group((address,))
                buy_sum = group.amount.sum() / 1_000_000
                bought_txs_count = len(group)
            else:
                buy_sum = 0
                bought_txs_count = 0

            if address in sellers_address:
                group = df_sellers_gb.get_group((address,))
                sell_sum = group.amount.sum() / 1_000_000
                sold_txs_count = len(group)
            else:
                sell_sum = 0
                sold_txs_count = 0

            total = buy_sum - sell_sum
            if total < 0:
                seller = {
                    "address_canonical": address,
                    "total": total,
                    "bought": buy_sum,
                    "sold": sell_sum,
                    "sold_txs_count": sold_txs_count,
                    "bought_txs_count": bought_txs_count,
                }
                sellers.append(seller)
            else:
                buyer = {
                    "address_canonical": address,
                    "total": total,
                    "bought": buy_sum,
                    "sold": sell_sum,
                    "sold_txs_count": sold_txs_count,
                    "bought_txs_count": bought_txs_count,
                }
                buyers.append(buyer)

        sellers = sorted(sellers, key=lambda d: d["total"])
        buyers = sorted(buyers, key=lambda d: d["total"], reverse=True)
    return {
        "sellers": sellers,
        "buyers": buyers,
        "period_in_hours": period,
    }


@router.get("/{net}/misc/winning-bakers-epoch/genesis-index/{genesis_index}/epoch/{epoch}")
async def get_winning_bakers_epoch(
    request: Request,
    genesis_index: int,
    epoch: int,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """
    Endpoint to get a summary of winning bakers for a given epoch and genesis index.

    Currently not in use.
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    try:
        winning_bakers: list[CCD_WinningBaker] = grpcclient.get_winning_bakers_epoch(
            genesis_index, epoch, NET(net)
        )  # type: ignore
    except _MultiThreadedRendezvous as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE and "Future epoch" in e.details():  # type: ignore
            raise HTTPException(
                status_code=404,
                detail="Future epoch.",
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Error getting winning bakers for genesis index {genesis_index} and epoch {epoch} on {net}.",
            )
    filtered = [x for x in winning_bakers if not x.present]

    winner_counts = Counter(x.winner for x in filtered)

    sorted_counts = dict(sorted(winner_counts.items(), key=lambda item: item[1], reverse=True))
    return sorted_counts


@router.get(
    "/{net}/misc/validators-failed-rounds",
    response_class=JSONResponse,
)
async def get_validators_failed_rounds(
    request: Request,
    net: str,
    mongodb: MongoDB = Depends(get_mongo_db),
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> list[dict]:
    if net not in ["mainnet"]:
        raise HTTPException(
            status_code=404,
            detail="Mainnet only.",
        )

    doc = (mongodb.mainnet[Collections.helpers].find_one({"_id": "last_known_payday"})) or {}
    latest_payday_block: CCD_BlockInfo = grpcclient.get_block_info(block_input=doc["hash"])

    # A document is labelled with the epoch it holds the rounds of, so epochs
    # from the payday block's epoch onwards are exactly this payday's rounds.
    # (Before the labels were corrected this window was one epoch behind: it
    # picked up the tail of the previous payday and missed this one's newest.)
    pipeline = [
        {"$match": {"genesis_index": latest_payday_block.genesis_index}},
        {"$match": {"epoch": {"$gte": latest_payday_block.epoch}}},
        {"$sort": {"epoch": 1}},  # ensure lowest epoch gets hour_index = 1
    ]

    result = mongodb.mainnet_db["paydays_v2_validators_missed"].aggregate(pipeline)

    cumulative = {}
    entries = []

    for i, entry in enumerate(result):
        entry["hour_index"] = i + 1

        missed = {}
        current_missed = entry.get("missed_rounds_count", {})

        # Add all known validators so far to `missed`, even if missed_epoch is 0
        validator_ids = set(cumulative.keys()) | set(current_missed.keys())

        for validator_id in validator_ids:
            node_info = (
                mongodb.mainnet[Collections.dashboard_nodes].find_one(
                    {"consensusBakerId": str(validator_id)}
                )
                or {}
            )

            missed_epoch = current_missed.get(validator_id, 0)
            cumulative[validator_id] = cumulative.get(validator_id, 0) + missed_epoch

            missed[validator_id] = {
                "missed_epoch": missed_epoch,
                "missed_payday": cumulative[validator_id],
            }
            if node_info:
                missed[validator_id]["node_name"] = node_info.get("nodeName", None)

        entry["missed"] = missed
        entries.append(entry)

    return entries


@router.get(
    "/{net}/misc/validator-score-parameters",
    response_class=JSONResponse,
)
async def get_validator_score_parameters(
    request: Request,
    net: str,
    grpcclient: GRPCClient = Depends(get_grpcclient),
    api_key: str = Security(API_KEY_HEADER),
) -> dict:
    """Return the validator score parameters in effect at the last final block.

    Currently that is only `maximum_missed_rounds`: the number of consecutive
    missed rounds at which a validator is automatically suspended. It is a
    governance-updatable chain parameter, so callers must read it rather than
    assume a value, but it changes rarely enough to be worth caching.

    Args:
        request: FastAPI request (unused, required by the router wrappers).
        net: Network identifier, must be `mainnet`, `testnet` or `devnet`.
        grpcclient: Shared gRPC client dependency.
        api_key: API key extracted from the request headers.

    Returns:
        `{"maximum_missed_rounds": int}`.

    Raises:
        HTTPException: If the network is unsupported, or the protocol version
            in effect has no validator score parameters (pre-suspension).
    """
    if net not in ["mainnet", "testnet", "devnet"]:
        raise HTTPException(
            status_code=422,
            detail="Don't be silly. We only support mainnet, testnet, and devnet.",
        )

    chain_parameters = grpcclient.get_block_chain_parameters("last_final", net=NET(net))
    score_parameters = getattr(chain_parameters.v3, "validator_score_parameters", None)

    if not score_parameters:
        raise HTTPException(
            status_code=404,
            detail=f"No validator score parameters in effect on {net}.",
        )

    return {"maximum_missed_rounds": score_parameters.maximum_missed_rounds}
