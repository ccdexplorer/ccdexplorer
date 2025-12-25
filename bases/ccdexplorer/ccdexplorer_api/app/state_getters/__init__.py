# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import Request
from ccdexplorer.env import API_URL, ADMIN_CHAT_ID
import datetime as dt
from ccdexplorer.ccdexplorer_api.app.models import User
from pymongo.asynchronous.database import AsyncDatabase
from ccdexplorer.domain.mongo import MongoTypeBlockPerDay
from ccdexplorer.tooter import TooterChannel, TooterType
from ccdexplorer.mongodb import CollectionsUtilities, Collections


def get_dict_diff(old_dict: dict, new_dict: dict) -> str:
    added = set(new_dict.keys()) - set(old_dict.keys())
    removed = set(old_dict.keys()) - set(new_dict.keys())
    changed = {
        k: (old_dict[k], new_dict[k])
        for k in set(old_dict.keys()) & set(new_dict.keys())
        if old_dict[k] != new_dict[k]
    }

    diff_parts = []
    if added:
        diff_parts.append(f"Added keys: {', '.join(added)}")
    if removed:
        diff_parts.append(f"Removed keys: {', '.join(removed)}")
    if changed:
        changes = [f"{k}: {old} → {new}" for k, (old, new) in changed.items()]
        diff_parts.append(f"Changed values: {', '.join(changes)}")

    return "\n".join(diff_parts) if diff_parts else "No differences"


async def get_httpx_client(req: Request):
    return req.app.httpx_client


def get_and_save_user_from_collection(req: Request):
    result = req.app.mongodb.utilities_db["api_users"].find({})
    req.app.users_from_collection = {x["token"]: User(**x) for x in list(result)}
    req.app.users_last_requested = dt.datetime.now().astimezone(dt.timezone.utc)


def get_user_details(req: Request, token: str | None = None) -> User | None:
    get_and_save_user_from_collection(req=req)
    if not token:
        token = req.cookies.get("api.ccdexplorer.io")

    try:
        users_from_collection = req.app.users_from_collection
        user = users_from_collection.get(token)
        if user:
            if type(user) is not User:
                user = User(**user)
    except AttributeError:
        user = None

    return user


async def get_mongo_db(req: Request):
    return req.app.mongodb


async def get_mongo_motor(req: Request):
    return req.app.motormongo


async def get_grpcclient(req: Request):
    return req.app.grpcclient


async def get_tooter(req: Request):
    return req.app.tooter


async def get_api_keys(
    req: Request | None = None, motormongo=None, app=None, for_: str | None = None
):
    now = dt.datetime.now().astimezone(dt.timezone.utc)

    if ((now - app.api_keys_last_requested).total_seconds() < 5) and (app.api_keys):
        # print(f"keys for {for_} from app_cache")
        return app.api_keys

    # print(f"keys for {for_} from collection")
    now = dt.datetime.now().astimezone(dt.UTC)
    pipeline = [
        {"$match": {"scope": API_URL}},
        {"$match": {"api_key_end_date": {"$gte": now}}},
    ]

    db: AsyncDatabase = motormongo.utilities_db
    coll = db["api_api_keys"]

    keys = {}
    cursor = await coll.aggregate(pipeline)
    async for d in cursor:
        keys[d["_id"]] = d
    try:
        if app.api_keys != keys:
            app.tooter.relay(
                channel=TooterChannel.NOTIFIER,
                title="",
                chat_id=ADMIN_CHAT_ID,
                body=get_dict_diff(app.api_keys, keys),
                notifier_type=TooterType.INFO,
            )
    except:  # noqa: E722
        pass
    # print(keys)
    app.api_keys = keys
    app.api_keys_last_requested = now

    return keys


def save_api_keys_for_topic(req: Request = None, mongodb=None, app=None, for_: str | None = None):
    # print(f"keys for {for_} from collection")
    now = dt.datetime.now().astimezone(dt.UTC)
    pipeline = [
        {"$match": {"scope": API_URL}},
        {"$match": {"api_key_end_date": {"$gte": now}}},
    ]

    db = mongodb.utilities_db

    keys = {x["_id"]: x for x in db["api_api_keys"].aggregate(pipeline)}

    app.api_keys = keys
    app.api_keys_last_requested = now


def get_exchange_rates(
    req: Request = None,
    motormongo=None,
    app=None,
):
    collection = req.app.mongodb.utilities if req else app.mongodb.utilities
    exchange_rates = req.app.exchange_rates if req else app.exchange_rates
    exchange_rates_last_requested = (
        req.app.exchange_rates_last_requested if req else app.exchange_rates_last_requested
    )
    if (
        (
            dt.datetime.now().astimezone(dt.timezone.utc) - exchange_rates_last_requested
        ).total_seconds()
        < 10
    ) and (exchange_rates):
        pass

    else:
        coll = collection[CollectionsUtilities.exchange_rates]

        exchange_rates = {x["token"]: x for x in coll.find({})}

        exchange_rates_last_requested = dt.datetime.now().astimezone(dt.timezone.utc)
        if req:
            req.app.exchange_rates = exchange_rates
            req.app.exchange_rates_last_requested = exchange_rates_last_requested
        else:
            app.exchange_rates = exchange_rates
            app.exchange_rates_last_requested = exchange_rates_last_requested
    return exchange_rates


def get_exchange_rates_historical(
    req: Request = None,
    motormongo=None,
    app=None,
):
    collection = req.app.mongodb.utilities if req else app.mongodb.utilities
    exchange_rates_historical = (
        req.app.exchange_rates_historical if req else app.exchange_rates_historical
    )
    exchange_rates_historical_last_requested = (
        req.app.exchange_rates_historical_last_requested
        if req
        else app.exchange_rates_historical_last_requested
    )
    if (
        (
            dt.datetime.now().astimezone(dt.timezone.utc) - exchange_rates_historical_last_requested
        ).total_seconds()
        < 5 * 60
    ) and (exchange_rates_historical):
        pass

    else:
        coll = collection[CollectionsUtilities.exchange_rates_historical]
        exchange_rates_historical = {}
        for x in coll.find({}):
            if x["token"] not in exchange_rates_historical:
                exchange_rates_historical[x["token"]] = {}
            token_dict = exchange_rates_historical[x["token"]]
            token_dict[x["date"]] = x["rate"]
            exchange_rates_historical[x["token"]] = token_dict
        # exchange_rates_historical = {x["token"]: x for x in coll.find({})}

        exchange_rates_historical_last_requested = dt.datetime.now().astimezone(dt.timezone.utc)
        if req:
            req.app.exchange_rates_historical = exchange_rates_historical
            req.app.exchange_rates_historical_last_requested = (
                exchange_rates_historical_last_requested
            )
        else:
            app.exchange_rates_historical = exchange_rates_historical
            app.exchange_rates_historical_last_requested = exchange_rates_historical_last_requested
    return exchange_rates_historical


def get_blocks_per_day(
    req: Request,
):
    if (
        (
            dt.datetime.now().astimezone(dt.timezone.utc) - req.app.blocks_per_day_last_requested
        ).total_seconds()
        < 60
    ) and (req.app.blocks_per_day):
        pass

    else:
        if "net" in req.path_params:
            db_to_use = (
                req.app.mongodb.testnet
                if req.path_params["net"] == "testnet"
                else req.app.mongodb.mainnet
            )
        else:
            db_to_use = req.app.mongodb.mainnet

        result = {
            x["_id"]: MongoTypeBlockPerDay(**x)
            for x in db_to_use[Collections.blocks_per_day].find({})
        }
        req.app.blocks_per_day = result
        req.app.blocks_per_day_last_requested = dt.datetime.now().astimezone(dt.timezone.utc)

    return req.app.blocks_per_day


def get_memos(
    req: Request,
):
    if (
        (
            dt.datetime.now().astimezone(dt.timezone.utc) - req.app.memos_last_requested
        ).total_seconds()
        < 60
    ) and (req.app.memos):
        set_memos: dict[str, list] = req.app.memos
        # print("memo_transfers from cache.")
    else:
        if "net" in req.path_params:
            db_to_use = (
                req.app.mongodb.testnet
                if req.path_params["net"] == "testnet"
                else req.app.mongodb.mainnet
            )
        set_memos = {
            x["_id"]: x["tx_hashes"] for x in db_to_use[Collections.memos_to_hashes].find({})
        }
        req.app.memos_last_requested = dt.datetime.now().astimezone(dt.timezone.utc)
        req.app.memos = set_memos
    return set_memos
