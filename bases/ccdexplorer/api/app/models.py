from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Optional
from ratelimit import Rule
from pydantic import BaseModel, Field


class APIPlansDetail(BaseModel):
    plan_name: str
    eur_rate: float
    day_limit: Optional[int] = 0
    min_limit: Optional[int] = 0
    sec_limit: Optional[int] = 0


class APIPlans(Enum):
    free = "Free"
    standard = "Standard"
    pro = "Pro"
    unrestricted = "Unrestricted"


plans: dict[APIPlans, APIPlansDetail] = {}
plans.update(
    {
        APIPlans.free: APIPlansDetail(
            plan_name=APIPlans.free.value,
            eur_rate=0.0000001,
            day_limit=100,
            min_limit=2,
        )
    }
)
plans.update(
    {
        APIPlans.standard: APIPlansDetail(
            plan_name=APIPlans.standard.value,
            eur_rate=1,
            day_limit=10_000,
            sec_limit=5,
        )
    }
)
plans.update(
    {
        APIPlans.pro: APIPlansDetail(
            plan_name=APIPlans.pro.value, eur_rate=3, day_limit=100_000, sec_limit=5
        )
    }
)
plans.update(
    {
        APIPlans.unrestricted: APIPlansDetail(
            plan_name=APIPlans.unrestricted.value, eur_rate=0.0000001, sec_limit=5
        )
    }
)

rate_limit_rules = []
plans_for_display = {}
for plan, detail in plans.items():
    day = detail.day_limit if detail.day_limit else None
    min = detail.min_limit if detail.min_limit else None
    sec = detail.sec_limit if detail.sec_limit else None
    group = plan.value.lower()
    zone = "v2"
    rate_limit_rules.append(Rule(day=day, minute=min, second=sec, group=group, zone=zone))

    # for display
    if group == "free":
        plans_for_display[group] = {
            "price": "EUR 0",
            "server_limit": f"{min} API calls/minute limit",
            "day_limit": f"Up to {day:,.0f} API calls/day",
            "note": "*Link back to the api is required",
        }
    if group in ["standard", "pro"]:
        plans_for_display[group] = {
            "price": f"EUR {detail.eur_rate:,.0f} / day",
            "server_limit": f"{sec} API calls/second limit",
            "day_limit": f"Up to {day:,.0f} API calls/day",
        }


rate_limit_rules.append(Rule(group="ccdexplorer.io"))


class APIPayment(BaseModel):
    tx_hash: str
    tx_date: str
    amount: float
    token_id: str | None = None
    paid_days_for_plan: float  # this is the result of the amount and the plan type
    # if users want to change paid plans, this lets us retroactively change the
    # paid days. For example, suppose a user starts with the Standard plan and pays 100 EUROe
    # for 100 paid days. If they decide to change to the Pro plan after 10 days,
    # we can manually override the number of days for the first 10 days to be 10
    # The automatic calculation will take the new plan into account, which would
    # make the 100 EUROe count as only 33.3 days, while in effect the fair amount would
    # be 10 days + 90/3 = 40 days in total. So the end date needs to be adjusted based on
    # if the manual override is present.
    manual_override: Optional[int] = None


class User(BaseModel):
    scope: str  # localhost/dev/prod url
    token: str  # uuid
    alias_id: int  # this generates the account to send tokens to
    alias_account_id: str  # this is the alias account
    api_account_id: str  # uuid
    email: str
    password: str  # hashed
    reset_password_token: Optional[str] = None  # gets added if user clicks reset password
    plan: Optional[str] = None
    payments: Optional[dict[str, APIPayment]] = None
    # this field is generated from the payments array
    active: Optional[bool] = None
    plan_end_date: dt.datetime


class APIKey(BaseModel):
    id: str | None = Field(default=None, alias="_id")
    scope: str  # localhost/dev/prod url
    api_account_id: str  # uuid
    api_group: str  # this group/tier determines the limits
    api_key_end_date: dt.datetime
