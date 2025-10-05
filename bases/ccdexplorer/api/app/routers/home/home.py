from ccdexplorer.env.settings import API_NET
from ccdexplorer.mongodb import (
    MongoMotor,
)
from fastapi import APIRouter, Depends, Request
from ccdexplorer.env import environment
from ccdexplorer.api.app.models import User, plans_for_display

from ccdexplorer.api.app.utils import get_plts_that_track_eur
from ccdexplorer.api.app.state_getters import get_mongo_motor, get_user_details


router = APIRouter(include_in_schema=False)


@router.get("/")
async def home_route(
    request: Request,
    mongomotor: MongoMotor = Depends(get_mongo_motor),
):
    user: User | None = get_user_details(request)
    db_to_use = (
        request.app.motormongo.testnet if API_NET == "testnet" else request.app.motormongo.mainnet
    )
    eur_plts = await get_plts_that_track_eur(db_to_use)
    faqs = [x for x in await mongomotor.utilities_db["api_faq"].find({}).to_list(length=None)]
    context = {
        "request": request,
        "env": environment,
        "user": user,
        "faqs": faqs,
        "plans_for_display": plans_for_display,
        "eur_plts": eur_plts,
    }
    return request.app.state.templates.TemplateResponse("plans/home.html", context)
