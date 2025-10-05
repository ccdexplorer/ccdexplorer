from uuid import uuid4
import math
from ccdexplorer.ccdexplorer_api.app.utils import await_await, get_plts_that_track_eur
import httpx
from ccdexplorer.mongodb import (
    MongoMotor,
    Collections,
    CollectionsUtilities,
)

import datetime as dt
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from pymongo import ReplaceOne, DeleteOne
import dateutil
from ccdexplorer.env import environment, API_NET, API_URL

from ccdexplorer.ccdexplorer_api.app.models import APIKey, User, APIPayment, APIPlans, plans
from ccdexplorer.ccdexplorer_api.app.state_getters import (
    get_mongo_motor,
    get_user_details,
    get_api_keys,
)

router = APIRouter(include_in_schema=False)


async def get_user_api_keys(user, mongomotor):
    """This function gets all API keys for the logged in user
    that belong to the scope where are in (localhost, dev, prod)."""
    pipeline = [
        {"$match": {"scope": API_URL}},
        {"$match": {"api_account_id": user.api_account_id}},
    ]
    result = await await_await(mongomotor.utilities_db, CollectionsUtilities.api_api_keys, pipeline)
    user_api_keys = [APIKey(**x) for x in result]
    return user_api_keys


async def get_payment_tx_and_update_payments(request: Request, user: User, db_to_use):
    eur_plts = await get_plts_that_track_eur(db_to_use)
    # note that we get ALL payments to the api here (because only _canonical is indexed)
    pipeline = [{"$match": {"impacted_address_canonical": user.alias_account_id[:29]}}]
    txs = await await_await(db_to_use, Collections.impacted_addresses, pipeline)

    ############################################

    user.payments = {}
    if user.plan:
        for tx in txs:
            if tx["impacted_address"] != user.alias_account_id:
                continue
            token_id = tx["balance_movement"]["plt_transfer_in"][0]["event"]["token_id"]
            if token_id not in eur_plts:
                continue

            amount = int(
                tx["balance_movement"]["plt_transfer_in"][0]["event"]["amount"]["value"]
            ) * (
                math.pow(
                    10, -tx["balance_movement"]["plt_transfer_in"][0]["event"]["amount"]["decimals"]
                )
            )
            if amount > 0:
                days = amount / plans[APIPlans[user.plan]].eur_rate
            else:
                days = 0
            user.payments[tx["tx_hash"]] = APIPayment(
                tx_hash=tx["tx_hash"],
                tx_date=tx["date"],  # type: ignore
                amount=amount,
                token_id=token_id,  # type: ignore
                paid_days_for_plan=days,
            )

        _ = await request.app.motormongo.utilities[CollectionsUtilities.api_users].bulk_write(
            [
                ReplaceOne(
                    {"_id": user.api_account_id},
                    user.model_dump(exclude_none=True),
                    upsert=True,
                )
            ]
        )
        await set_end_date_for_plan(request, user)


@router.get("/account")
async def account_home(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
):
    user: User = get_user_details(request)  # type: ignore
    if not user:
        response = RedirectResponse(url="/auth/login", status_code=303)
        return response

    request.app.api_keys = await get_api_keys(
        motormongo=mongomotor, app=request.app, for_="account_home"
    )
    # find transactions through logged events
    db_to_use = (
        request.app.motormongo.testnet if API_NET == "testnet" else request.app.motormongo.mainnet
    )

    _ = await get_payment_tx_and_update_payments(request, user, db_to_use)
    # read user again back from updated db
    user: User | None = get_user_details(request)

    user_api_keys = await get_user_api_keys(user, request.app.motormongo)
    if len(user_api_keys) == 0:
        sample_key = "528e6511-d55a-49d3-a4f1-fcce5eef03cc"
    else:
        sample_key = user_api_keys[0].id

    # reload from collection
    user: User | None = get_user_details(request)
    if not user:
        return None

    user.active = user.plan_end_date.astimezone(dt.UTC) > dt.datetime.now().astimezone(dt.UTC)
    if user.plan:
        plan_daily_limit = plans[APIPlans[user.plan]].day_limit
        plan_daily_fee = plans[APIPlans[user.plan]].eur_rate
    else:
        plan_daily_limit = None
        plan_daily_fee = None

    if user.plan == "free":
        plan_min_limit = plans[APIPlans[user.plan]].min_limit

    # redis key info

    # for sliding_window use these
    # day_calls_made = await request.app.redis.zcard(f"v2:*:{user.api_account_id}:day")
    # day_calls_remaining = plans[APIPlans[user.plan]].day_limit - day_calls_made

    # for normal redis use these
    day_calls_remaining = await request.app.redis.get(f"v2:*:{user.api_account_id}:day")
    if day_calls_remaining:
        day_calls_remaining = day_calls_remaining.decode()
    else:
        day_calls_remaining = plan_daily_limit

    ttl = await request.app.redis.ttl(f"v2:*:{user.api_account_id}:day")
    ttl_date = dt.datetime.now().astimezone(dt.UTC) + dt.timedelta(seconds=ttl)
    ttl_humanize = dt.timedelta(seconds=ttl)

    if user.plan == "free":
        ttl_min = await request.app.redis.ttl(f"v2:*:{user.api_account_id}:minute")
        ttl_date_min = dt.datetime.now().astimezone(dt.UTC) + dt.timedelta(seconds=ttl_min)
        ttl_humanize_min = dt.timedelta(seconds=ttl_min)

        min_calls_remaining = await request.app.redis.get(f"v2:*:{user.api_account_id}:minute")
        if min_calls_remaining:
            min_calls_remaining = min_calls_remaining.decode()
        else:
            min_calls_remaining = plan_min_limit  # type: ignore
    else:
        min_calls_remaining = None
        ttl_min = None
        ttl_date_min = None
        ttl_humanize_min = None
        plan_min_limit = None

    context = {
        "request": request,
        "env": environment,
        "user": user,
        "user_api_keys": user_api_keys,
        "sample_key": sample_key,
        # "total_paid_amount": total_paid_amount,
        "day_calls_remaining": day_calls_remaining,
        "min_calls_remaining": min_calls_remaining,
        "ttl_date": ttl_date,
        "ttl": ttl,
        "ttl_min": ttl_min,
        "ttl_date_min": ttl_date_min,
        "ttl_humanize_min": ttl_humanize_min,
        "ttl_humanize": ttl_humanize,
        "plan_daily_limit": plan_daily_limit,
        "plan_min_limit": plan_min_limit,  # type: ignore
        "plan_daily_fee": plan_daily_fee,
        "net": API_NET,
    }
    return request.app.state.templates.TemplateResponse("account/home.html", context)


@router.get("/account/keys")
async def account_keys(request: Request):
    user: User | None = get_user_details(request)
    if not user:
        response = RedirectResponse(url="/auth/login", status_code=303)
        return response

    result = (
        await request.app.motormongo.utilities_db["api_api_keys"]
        .find({"api_account_id": user.api_account_id})
        .to_list(length=None)
    )

    user_api_keys = []
    if len(result) > 0:
        user_api_keys = [{"key": x["_id"], "tier": x["api_group"]} for x in result]
    context = {
        "request": request,
        "env": environment,
        "user": user,
        "user_api_keys": user_api_keys,
    }
    return request.app.state.templates.TemplateResponse("account/keys.html", context)


@router.post(
    "/account/refresh",
)
async def account_home_refresh(
    request: Request,
):
    response = RedirectResponse(url="/account", status_code=200)
    response.headers["HX-Refresh"] = "true"
    return response


@router.post("/account/new-key")
async def account_new_key(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
):
    user: User | None = get_user_details(request)
    if not user:
        response = RedirectResponse(url="/auth/login", status_code=303)
        return response
    # api keys for the free plan are always active, set to 365 days (but of course rate limited)
    if user.plan == "free":
        api_key_end_date = dt.datetime.now().astimezone(dt.UTC) + dt.timedelta(days=365)
    # api keys for paid plans are initially
    else:
        api_key_end_date = user.plan_end_date

    key_id = str(uuid4())
    key_to_add = APIKey(
        api_account_id=user.api_account_id,
        scope=API_URL,  # type: ignore
        api_group=user.plan,  # type: ignore
        # id=key_id,
        api_key_end_date=api_key_end_date,
    )
    key_to_add = key_to_add.model_dump()
    if "id" in key_to_add:
        del key_to_add["id"]
    request.app.motormongo.utilities_db["api_api_keys"].bulk_write(
        [ReplaceOne({"_id": key_id}, key_to_add, upsert=True)]
    )
    request.app.api_keys = await get_api_keys(
        motormongo=mongomotor, app=request.app, for_="account_new_key"
    )
    response = RedirectResponse(url="/account", status_code=200)
    response.headers["HX-Redirect"] = "/account"
    return response


@router.delete("/account/key/{key}")
async def account_delete_key(
    request: Request,
    key: str,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
):
    request.app.motormongo.utilities_db["api_api_keys"].bulk_write(
        [
            DeleteOne(
                {"_id": key},
            )
        ]
    )
    request.app.api_keys = await get_api_keys(
        motormongo=mongomotor, app=request.app, for_="account_delete_key"
    )
    response = RedirectResponse(url="/account", status_code=200)
    response.headers["HX-Redirect"] = "/account"
    return response


async def set_end_date_for_plan(request: Request, user: User):
    # 1 get tx hashes from payments
    # 2 get
    if len(user.payments) == 0:  # type: ignore
        user.active = False
        if user.plan == "free":
            end_date = dt.datetime.now().astimezone(dt.UTC) + dt.timedelta(days=365)
        else:
            end_date = dt.datetime.now().astimezone(dt.UTC) - dt.timedelta(days=1)
    else:
        sorted_txs = sorted(user.payments.items(), key=lambda x: x[1].tx_date)  # type: ignore
        # set initial value
        start_date = dateutil.parser.parse(sorted_txs[0][1].tx_date)
        end_date = start_date

        for _, tx in sorted_txs:
            try:
                response = await request.app.httpx_client.get(
                    f"{request.app.api_url}/v2/{API_NET}/transaction/{tx.tx_hash}"
                )
                response.raise_for_status()
                tx_classified = CCD_BlockItemSummary(**response.json())
            except httpx.HTTPError as e:
                print(e)
                return None

            start_date = tx_classified.block_info.slot_time  # type: ignore
            if start_date < end_date:
                # calculate the overlap days
                days_overlap = (end_date - start_date).total_seconds() / (60 * 60 * 24)
                end_date = start_date + dt.timedelta(days=tx.paid_days_for_plan + int(days_overlap))
            else:
                # either there is just 1 payment,
                # or the user has a period between the previous payment ending
                # and the next payment
                end_date = start_date + dt.timedelta(days=tx.paid_days_for_plan)

    # user.plan_end_date = dt.datetime.combine(end_date, dt.time.max).astimezone(dt.UTC)
    user.plan_end_date = end_date
    # write back to user
    _ = await request.app.mongomotor.utilities[CollectionsUtilities.api_users].bulk_write(
        [
            ReplaceOne(
                {"_id": user.api_account_id},
                user.model_dump(exclude_none=True),
                upsert=True,
            )
        ]
    )

    # await get_user_api_keys(user, mongomotor)


async def set_end_date_for_api_keys(user: User, mongomotor: MongoMotor):
    if len(user.payments) == 0:  # type: ignore
        user.active = False

    await get_user_api_keys(user, mongomotor)
