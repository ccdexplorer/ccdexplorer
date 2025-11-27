import asyncio
import datetime as dt
import inspect
from collections import defaultdict
from enum import Enum
from typing import Any, Optional
from pymongo.asynchronous.collection import AsyncCollection
from ccdexplorer.mongodb.core import Collections, CollectionsUtilities
import httpx
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_RejectReason,
    CCD_UpdatePayload,
)
from pydantic import BaseModel


def _make_docstring_wrapper(registrar):
    """Return a decorator factory that injects summary/description from docstrings."""

    def wrapper(*args, **kwargs):
        supplied_summary = kwargs.pop("summary", None)
        supplied_description = kwargs.pop("description", None)

        def decorator(func):
            doc = inspect.getdoc(func) or ""
            first_paragraph = doc.strip().split("\n\n", 1)[0] if doc else ""
            first_line = first_paragraph.splitlines()[0] if first_paragraph else ""
            summary = supplied_summary or first_line or None
            description = supplied_description or first_paragraph or first_line or None
            return registrar(
                *args,
                summary=summary,
                description=description,
                **kwargs,
            )(func)

        return decorator

    return wrapper


def apply_docstring_router_wrappers(router):
    """Ensure router methods derive summary/description values from docstrings."""

    def safe_getattr(name):
        return getattr(router, name, None)

    for method_name in ("get", "post", "put", "delete", "patch"):
        registrar = safe_getattr(method_name)
        if registrar is not None:
            setattr(router, method_name, _make_docstring_wrapper(registrar))


class PLTToken(BaseModel):
    token_id: str
    balance: str
    decimals: Optional[int] = None
    token_module: Optional[str] = None
    exchange_rate: Optional[float] = None
    token_symbol: Optional[str] = None
    token_value: Optional[float] = None
    token_value_USD: Optional[float] = None
    tag_information: Optional[dict] = None
    token_metadata: Optional[dict] = None


class TokenHolding(BaseModel):
    token_address: str
    contract: str
    token_id: str
    token_amount: str
    decimals: Optional[int] = None
    token_symbol: Optional[str] = None
    token_value: Optional[float] = None
    token_value_USD: Optional[float] = None
    verified_information: Optional[dict] = None
    address_information: Optional[dict] = None
    account_address_for_token: Optional[str] = None


class APIResponseResult(BaseModel):
    return_value: Optional[Any] = None
    status_code: int
    ok: bool
    message: Optional[str] = None
    duration_in_sec: float

    def get_value_if_ok_or_none(self):
        return


async def get_plts_that_track_eur(
    db_to_use: dict[Collections | CollectionsUtilities, AsyncCollection],
) -> dict:
    pipeline = [{"$match": {"stablecoin_tracks": "EUR"}}]
    return {x["_id"]: x for x in await await_await(db_to_use, Collections.plts_tags, pipeline)}


async def await_await(
    db: dict[Collections | CollectionsUtilities, AsyncCollection],
    collection: Collections | CollectionsUtilities,
    pipeline: list[dict],
    length: int | None = None,
    **kwargs,
) -> list:
    if isinstance(collection, CollectionsUtilities):
        collection = CollectionsUtilities(collection.value)
    else:
        collection = Collections(collection.value)
    return await (await db[collection].aggregate(pipeline, **kwargs)).to_list(length=length)


async def get_url_from_api(url: str, httpx_client: httpx.AsyncClient):
    api_response = APIResponseResult(status_code=-1, duration_in_sec=-1, ok=False)
    response = None
    now = dt.datetime.now().astimezone(dt.UTC)
    try:
        response = await httpx_client.get(url)
        try:
            api_response.return_value = response.json()
        except:  # noqa: E722
            # if the response happens to be empty, json decoder gives an error.
            api_response.return_value = None
        api_response.status_code = response.status_code
        api_response.ok = True if response.status_code == 200 else False
    except httpx.HTTPError:
        api_response.return_value = None
        if response:
            api_response.status_code = response.status_code
            api_response.return_value = response.json()
    except asyncio.CancelledError:
        api_response.return_value = None
        if response:
            api_response.status_code = response.status_code
            api_response.return_value = response.json()

    end = dt.datetime.now().astimezone(dt.UTC)

    api_response.duration_in_sec = (end - now).total_seconds()
    if not api_response:
        api_response = APIResponseResult(status_code=-1, duration_in_sec=-1, ok=False)
    # print(
    #     f"GET: {api_response.duration_in_sec:2,.4f}s | {api_response.status_code} | {url}"
    # )
    return api_response


class TypeContentsCategories(Enum):
    transfer = "Transfers"
    smart_contract = "Smart Cts"
    data_registered = "Data"
    staking = "Staking"
    identity = "Identity"
    chain = "Chain"
    rejected = "Rejected"
    plt = "PLT"


class TypeContentsCategoryColors(Enum):
    transfer = ("#33C364",)
    smart_contract = ("#E87E90", "#7939BA", "#B37CDF")
    data_registered = ("#48A2AE",)
    staking = ("#8BE7AA",)
    identity = ("#F6DB9A",)
    chain = ("#FFFDE4",)
    rejected = ("#DC5050",)
    plt = ("#FAD050",)


class TypeContents(BaseModel):
    display_str: str
    category: TypeContentsCategories
    color: str


tx_type_translation: dict[str, TypeContents] = {}
# smart contracts
tx_type_translation["module_deployed"] = TypeContents(
    display_str="new module",
    category=TypeContentsCategories.smart_contract,
    color=TypeContentsCategoryColors.smart_contract.value[0],
)
tx_type_translation["contract_initialized"] = TypeContents(
    display_str="new contract",
    category=TypeContentsCategories.smart_contract,
    color=TypeContentsCategoryColors.smart_contract.value[1],
)
tx_type_translation["contract_update_issued"] = TypeContents(
    display_str="contract updated",
    category=TypeContentsCategories.smart_contract,
    color=TypeContentsCategoryColors.smart_contract.value[2],
)

# account transfer
tx_type_translation["account_transfer"] = TypeContents(
    display_str="transfer",
    category=TypeContentsCategories.transfer,
    color=TypeContentsCategoryColors.transfer.value[0],
)
tx_type_translation["transferred_with_schedule"] = TypeContents(
    display_str="scheduled transfer",
    category=TypeContentsCategories.transfer,
    color=TypeContentsCategoryColors.transfer.value[0],
)
tx_type_translation["transferred_to_encrypted"] = TypeContents(
    display_str="transfer (encrypted)",
    category=TypeContentsCategories.transfer,
    color=TypeContentsCategoryColors.transfer.value[0],
)
tx_type_translation["encrypted_amount_transferred"] = TypeContents(
    display_str="transfer (encrypted)",
    category=TypeContentsCategories.transfer,
    color=TypeContentsCategoryColors.transfer.value[0],
)
tx_type_translation["transferred_to_public"] = TypeContents(
    display_str="transfer (encrypted)",
    category=TypeContentsCategories.transfer,
    color=TypeContentsCategoryColors.transfer.value[0],
)

# staking
tx_type_translation["baker_added"] = TypeContents(
    display_str="validator added",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["baker_removed"] = TypeContents(
    display_str="validator removed",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["baker_stake_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["baker_restake_earnings_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["baker_restake_earnings_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["baker_keys_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["baker_configured"] = TypeContents(
    display_str="validator configured",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_added"] = TypeContents(
    display_str="validator added",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_removed"] = TypeContents(
    display_str="validator removed",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_stake_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_restake_earnings_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_restake_earnings_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_keys_updated"] = TypeContents(
    display_str="validator updated",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["validator_configured"] = TypeContents(
    display_str="validator configured",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)

tx_type_translation["token_creation"] = TypeContents(
    display_str="token creation",
    category=TypeContentsCategories.plt,
    color=TypeContentsCategoryColors.plt.value[0],
)

tx_type_translation["token_update_effect"] = TypeContents(
    display_str="token update",
    category=TypeContentsCategories.plt,
    color=TypeContentsCategoryColors.plt.value[0],
)

tx_type_translation["delegation_configured"] = TypeContents(
    display_str="delegation configured",
    category=TypeContentsCategories.staking,
    color=TypeContentsCategoryColors.staking.value[0],
)
# credentials
tx_type_translation["credential_keys_updated"] = TypeContents(
    display_str="credentials updated",
    category=TypeContentsCategories.identity,
    color=TypeContentsCategoryColors.identity.value[0],
)

tx_type_translation["credentials_updated"] = TypeContents(
    display_str="credentials updated",
    category=TypeContentsCategories.identity,
    color=TypeContentsCategoryColors.identity.value[0],
)

tx_type_translation["credentials_updated"] = TypeContents(
    display_str="credentials updated",
    category=TypeContentsCategories.identity,
    color=TypeContentsCategoryColors.identity.value[0],
)
# data registered
tx_type_translation["data_registered"] = TypeContents(
    display_str="data registered",
    category=TypeContentsCategories.data_registered,
    color=TypeContentsCategoryColors.data_registered.value[0],
)

# rejected
for reason in CCD_RejectReason.model_fields:
    tx_type_translation[reason] = TypeContents(
        display_str=reason.replace("_", " "),
        category=TypeContentsCategories.rejected,
        color=TypeContentsCategoryColors.rejected.value[0],
    )

payload_translation = {}
payload_translation["protocol_update"] = "protocol"
payload_translation["election_difficulty_update"] = "election difficulty"
payload_translation["euro_per_energy_update"] = "EUR per NRG"
payload_translation["micro_ccd_per_euro_update"] = "CCD per EUR"
payload_translation["foundation_account_update"] = "foundation account"
payload_translation["mint_distribution_update"] = "mint distribution"
payload_translation["transaction_fee_distribution_update"] = "tx fee distribution"
payload_translation["baker_stake_threshold_update"] = "validator stake threshold"
payload_translation["root_update"] = "root"
payload_translation["level_1_update"] = "level 1"
payload_translation["add_anonymity_revoker_update"] = "add anonymity revoker"
payload_translation["add_identity_provider_update"] = "add identity provider"
payload_translation["cooldown_parameters_cpv_1_update"] = "cooldown parameters"
payload_translation["pool_parameters_cpv_1_update"] = "pool parameters"
payload_translation["time_parameters_cpv_1_update"] = "time parameters"
payload_translation["mint_distribution_cpv_1_update"] = "mint distribution"
payload_translation["validator_score_parameters_update"] = "validator score parameters"
payload_translation["mint_distribution_cpv_1_update"] = "mint distribution"
payload_translation["finalization_committee_parameters_update"] = (
    "finalization committee parameters"
)
payload_translation["create_plt_update"] = "create PLT"


# update
for payload in CCD_UpdatePayload.model_fields:
    tx_type_translation[payload] = TypeContents(
        display_str=payload_translation[payload],
        category=TypeContentsCategories.chain,
        color=TypeContentsCategoryColors.chain.value[0],
    )

# identity
tx_type_translation["normal"] = TypeContents(
    display_str="account creation",
    category=TypeContentsCategories.identity,
    color=TypeContentsCategoryColors.identity.value[0],
)

tx_type_translation["initial"] = TypeContents(
    display_str="account creation",
    category=TypeContentsCategories.identity,
    color=TypeContentsCategoryColors.identity.value[0],
)


category_to_types: dict[str, list[str]] = defaultdict(list)

for tx_type, info in tx_type_translation.items():
    # use the enum member’s name as the key:
    cat_key = info.category.name
    category_to_types[cat_key].append(tx_type)

category_to_types = dict(category_to_types)
