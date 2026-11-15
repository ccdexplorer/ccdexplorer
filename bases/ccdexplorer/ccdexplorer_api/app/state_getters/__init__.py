# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportAssignmentType=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportArgumentType=false
from fastapi import Request
from ccdexplorer.env import API_URL, ADMIN_CHAT_ID, environment
import datetime as dt
from ccdexplorer.ccdexplorer_api.app.models import User
from pymongo.asynchronous.database import AsyncDatabase
from ccdexplorer.domain.mongo import MongoTypeBlockPerDay
from ccdexplorer.tooter import TooterChannel, TooterType
from ccdexplorer.mongodb import CollectionsUtilities, Collections, net_db


def site_users_collection_name() -> CollectionsUtilities:
    """Which SiteUser collection ``site_auth.py``/``site_user_v2.py`` read and
    write: ``users_v2_dev`` when ``ENVIRONMENT=dev``, ``users_v2_prod``
    otherwise.

    Mirrors the bot's own dev/prod split for this same collection
    (``ccdexplorer_bot/bot/__init__.py::users_collection``), just switched by
    an explicit env var instead of "is pytest running" -- this runs under a
    real uvicorn process (e.g. Playwright's e2e webServer, which sets
    ENVIRONMENT=dev so its test account never touches real user data), not
    pytest.
    """
    return (
        CollectionsUtilities.users_v2_dev
        if environment.get("ENVIRONMENT") == "dev"
        else CollectionsUtilities.users_v2_prod
    )


def mask_key(key: object) -> str:
    """An API key as a reader can recognise it but not use it."""
    text = str(key)
    return f"{text[:8]}…" if len(text) > 8 else "…"


def get_dict_diff(old_dict: dict, new_dict: dict) -> str:
    """What changed between two key sets, safe to send to a chat.

    The keys of these dicts are API keys, and the notification goes to
    Telegram, where it stays. So they appear as a recognisable prefix, and a
    changed entry says which fields changed rather than printing the
    documents -- which carry the key again as their _id.
    """
    added = set(new_dict.keys()) - set(old_dict.keys())
    removed = set(old_dict.keys()) - set(new_dict.keys())
    changed = {
        k: sorted(
            f
            for f in set(old_dict[k]) | set(new_dict[k])
            if old_dict[k].get(f) != new_dict[k].get(f)
        )
        for k in set(old_dict.keys()) & set(new_dict.keys())
        if old_dict[k] != new_dict[k]
    }

    diff_parts = []
    if added:
        diff_parts.append(f"Added keys: {', '.join(sorted(mask_key(k) for k in added))}")
    if removed:
        diff_parts.append(f"Removed keys: {', '.join(sorted(mask_key(k) for k in removed))}")
    if changed:
        changes = [f"{mask_key(k)}: {', '.join(fields)}" for k, fields in changed.items()]
        diff_parts.append(f"Changed: {'; '.join(changes)}")

    return "\n".join(diff_parts) if diff_parts else "No differences"


async def get_httpx_client(req: Request):
    return req.app.httpx_client


def get_user_details(req: Request, token: str | None = None) -> User | None:
    """The api account behind the login cookie (or an explicit token).

    One document is read, by token. It used to load every api user, on every
    page view, and pick one out of the resulting dict.
    """
    if not token:
        token = req.cookies.get("api.ccdexplorer.io")
    if not token:
        return None

    try:
        doc = req.app.mongodb.utilities_db["api_users"].find_one({"token": token})
    except AttributeError:
        return None
    return User(**doc) if doc else None


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


#: How long a per-net lookup is reused. Short, because both are read on pages
#: where a stale answer would show.
PER_NET_CACHE_SECONDS = 60


def _net_of(req: Request) -> str:
    """The network a request is about; routes without one are mainnet's."""
    return req.path_params.get("net", "mainnet")


def _cached_per_net(req: Request, name: str, load):
    """One cached value per network.

    These were one value for the whole app, so a testnet request within a
    minute of a mainnet one was answered with mainnet's data.
    """
    net = _net_of(req)
    cache = req.app.__dict__.setdefault("_per_net_cache", {})
    stamp, value = cache.get((name, net), (None, None))
    now = dt.datetime.now().astimezone(dt.timezone.utc)
    if stamp is not None and value and (now - stamp).total_seconds() < PER_NET_CACHE_SECONDS:
        return value
    value = load(net_db(req.app.mongodb, net))
    cache[(name, net)] = (now, value)
    return value


def get_blocks_per_day(
    req: Request,
):
    return _cached_per_net(
        req,
        "blocks_per_day",
        lambda db: {
            x["_id"]: MongoTypeBlockPerDay(**x) for x in db[Collections.blocks_per_day].find({})
        },
    )


def get_memos(
    req: Request,
):
    return _cached_per_net(
        req,
        "memos",
        lambda db: {x["_id"]: x["tx_hashes"] for x in db[Collections.memos_to_hashes].find({})},
    )
