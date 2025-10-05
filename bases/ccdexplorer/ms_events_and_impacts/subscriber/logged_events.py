# pyright: reportArgumentType=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportOperatorIssue=false
# pyright: reportOptionalMemberAccess=false
# pyright: reportOptionalSubscript=false
# pyright: reportOptionalCall=false
# pyright: reportPossiblyUnboundVariable=false
# pyright: reportIndexIssue=false
# pyright: reportGeneralTypeIssues=false
# pyright: reportMissingTypeStubs=false
# pyright: reportAssignmentType=false
import asyncio
import datetime as dt
from enum import Enum
from typing import Any, Optional
from ccdexplorer.domain.generic import StandardIdentifiers
from ccdexplorer.domain.mongo import (
    MongoTypeLoggedEventV2,
    MongoTokensImpactedAddress,
    MongoTypeInstance,
)
from ccdexplorer.domain.cis import (
    CISProcessEventRequest,
    depositCCDEvent,
    itemCreatedEvent,
    itemStatusChangedEvent,
    mintEvent,
    burnEvent,
    transferCCDEvent,
    transferCIS2TokensEvent,
    transferEvent,
    tokenMetadataEvent,
    updateOperatorEvent,
    withdrawCCDEvent,
    depositCIS2TokensEvent,
    withdrawCIS2TokensEvent,
)
from ccdexplorer.celery_app import app as celery_app
from ccdexplorer.cis import (
    CIS,
)
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    CCD_BlockItemSummary,
    CCD_ContractAddress,
    CCD_ShortBlockInfo,
)
from ccdexplorer.grpc_client.types_pb2 import VersionedModuleSource
from ccdexplorer.mongodb import (
    Collections,
)
from ccdexplorer.tooter import Tooter
from ccdexplorer.schema_parser import Schema
from pydantic import BaseModel
from pymongo import DeleteOne, ReplaceOne
from pymongo.collection import Collection
from rich.console import Console

from ccdexplorer.env import RUN_ON_NET

console = Console()


async def publish_to_celery(processor: str, payload: dict[str, Any]) -> None:
    """
    Publish one task per processor to queue: {RUN_ON_NET}:queue:{processor}.
    Uses send_task so producer has zero dependency on worker code.
    """
    task_name = "process_block"

    # exact queue naming required:
    qname = f"{RUN_ON_NET}:queue:{processor}"

    # Send the task. Use .to_thread so we don't block the event loop on the small I/O.
    # Args are your payload; also include 'processor' explicitly so workers can route internally if desired.
    await asyncio.to_thread(
        celery_app.send_task,
        task_name,
        args=[],  # prefer kwargs for clarity
        kwargs={"processor": processor, "payload": payload},
        queue=qname,
    )


class AddressTypes(Enum):
    # Value is whether a canonical version should be stored
    account_or_contract = True
    public_key = False


class LoggedEventFromTX(BaseModel):
    for_ia: MongoTypeLoggedEventV2
    for_collection: dict


class LoggedEventsFromTX(BaseModel):
    for_ia: list[MongoTypeLoggedEventV2] = []
    for_collection: list[dict] = []


class CIS5Impacted(BaseModel):
    wallet_contract_address: str
    cis2_token_contract_address: Optional[str] = None
    token_id_or_ccd: str
    address_or_public_key: str
    address_canonical_or_public_key: str


class CIS5PublicKeyInfo(BaseModel):
    public_key: str
    wallet_contract_address: str
    deployment_tx: str
    deployment_block_height: int
    date: str


class LoggedEvent:
    def get_current_known_ci5_public_keys(self, db_to_use: dict[Collections, Collection]):
        if self.cis5_keys_cache is not None:
            if self.cis5_keys_update_last_requested + dt.timedelta(
                seconds=60
            ) > dt.datetime.now().astimezone(dt.UTC):
                return self.cis5_keys_cache

        result = db_to_use[Collections.cis5_public_keys_info].find({})
        cis5_keys = {x["_id"]: x for x in result}
        self.cis5_keys_cache = cis5_keys
        self.cis5_keys_update_last_requested = dt.datetime.now().astimezone(dt.UTC)
        return cis5_keys

    def get_impacted_addresses_from_logged_events(
        self, logged_events: list[MongoTypeLoggedEventV2]
    ) -> list[MongoTokensImpactedAddress]:
        """
        Note that this may lead to impacted address ids that are not unique (enough),
        however the goal of the impacted addresses collection is to flag transactions in
        which accounts are affected, ie when inspecting an account, finding all transactions
        whwre this account balance may have been impacted (CCD and other tokens).
        Hence, duplicated ids will lead to the same document being written more than once.
        Not ideal, but not an issue given the purpose of the collection.
        Note there are no address fields in any of the CIS-6 logged events and no address fields
        we ca track for CIS-4 logged events.
        """
        all_ias: list[MongoTokensImpactedAddress] = []
        for le in logged_events:
            impacted_addresses: list[dict] = []
            if not le.event_info.standard:
                if le.event_info.event_type == "five_stars_register_access_event":
                    impacted_address = le.recognized_event.public_key
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )
                else:
                    continue

            if not le.recognized_event:
                continue

            if le.event_info.standard == StandardIdentifiers.CIS_2.value:
                if "mint_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.to_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif "burn_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif "transfer_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.to_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )
                    impacted_address = le.recognized_event.from_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif "operator_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.owner
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )
                    impacted_address = le.recognized_event.operator
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

            elif le.event_info.standard == StandardIdentifiers.CIS_3.value:
                if "nonce_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.sponsoree
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

            elif le.event_info.standard == StandardIdentifiers.CIS_4.value:
                pass

            elif le.event_info.standard == StandardIdentifiers.CIS_5.value:
                if "nonce_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.sponsoree
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )

                elif "deposit_ccd_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )

                elif "deposit_cis2_tokens_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )

                elif "withdraw_ccd_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )
                    impacted_address = le.recognized_event.to_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif "withdraw_cis2_tokens_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )
                    impacted_address = le.recognized_event.to_address
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.account_or_contract,
                        }
                    )

                elif "transfer_ccd_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )

                elif "transfer_cis2_tokens_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    impacted_addresses.append(
                        {
                            "address": impacted_address,
                            "address_type": AddressTypes.public_key,
                        }
                    )

            elif le.event_info.standard == StandardIdentifiers.CIS_6.value:
                pass

            # finally store impacted addresses

            for ia in impacted_addresses:
                address: str = ia["address"]
                if address is not None:
                    address_type: AddressTypes = ia["address_type"]
                    _id = f"{le.tx_info.tx_hash}-{address}-{le.event_info.event_type}"
                    canonical = address[:29] if address_type.value else address

                    impacted_address_as_class = MongoTokensImpactedAddress(
                        **{
                            "_id": _id,
                            "tx_hash": le.tx_info.tx_hash,
                            "impacted_address": address,
                            "impacted_address_canonical": canonical,
                            "event_type": le.event_info.event_type,
                            "token_address": le.event_info.token_address,
                            "contract": le.event_info.contract,
                            "block_height": le.tx_info.block_height,
                            "date": f"{le.tx_info.date}",
                        }
                    )
                    all_ias.append(impacted_address_as_class)

        return all_ias

    def get_cis5_keys_and_contracts_from_logged_events(
        self, logged_events: list[MongoTypeLoggedEventV2]
    ) -> list[dict]:
        """ """
        all_cis5_impacts: list[dict] = []
        db_to_use = self.testnet if self.net.value == "testnet" else self.mainnet
        for le in logged_events:
            cis5_impacted: list[CIS5Impacted] = []
            if not le.event_info.standard:
                continue

            if not le.recognized_event:
                continue

            if le.event_info.standard == StandardIdentifiers.CIS_5.value:
                wallet_contract_address = le.event_info.contract
                assert wallet_contract_address is not None
                if "nonce_event" in le.event_info.event_type:
                    impacted_address = le.recognized_event.sponsoree
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )

                if "cis2" in le.event_info.event_type:
                    cis2_token_contract_address = le.recognized_event.cis2_token_contract_address
                    assert cis2_token_contract_address is not None

                if "deposit_ccd_event" in le.event_info.event_type:
                    assert isinstance(le.recognized_event, depositCCDEvent)
                    impacted_address = le.recognized_event.from_address
                    assert impacted_address is not None
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address[:29],
                        )
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    assert impacted_address is not None
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )

                elif "deposit_cis2_tokens_event" in le.event_info.event_type:
                    assert isinstance(le.recognized_event, depositCIS2TokensEvent)
                    impacted_address = le.recognized_event.from_address
                    assert impacted_address is not None
                    assert le.recognized_event.token_id is not None
                    assert isinstance(cis2_token_contract_address, str | None)  # type: ignore
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            cis2_token_contract_address=cis2_token_contract_address,
                            token_id_or_ccd=le.recognized_event.token_id,
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address[:29],
                        )
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    assert impacted_address is not None
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            cis2_token_contract_address=cis2_token_contract_address,
                            token_id_or_ccd=le.recognized_event.token_id,
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )

                elif "withdraw_ccd_event" in le.event_info.event_type:
                    assert isinstance(le.recognized_event, withdrawCCDEvent)
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )
                    impacted_address = le.recognized_event.to_address
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address[:29],
                        )
                    )

                elif "withdraw_cis2_tokens_event" in le.event_info.event_type:
                    assert isinstance(le.recognized_event, withdrawCIS2TokensEvent)
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            cis2_token_contract_address=cis2_token_contract_address,
                            token_id_or_ccd=le.recognized_event.token_id,
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )
                    impacted_address = le.recognized_event.to_address
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            cis2_token_contract_address=cis2_token_contract_address,
                            token_id_or_ccd=le.recognized_event.token_id,
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address[:29],
                        )
                    )

                elif "transfer_ccd_event" in le.event_info.event_type:
                    assert isinstance(le.recognized_event, transferCCDEvent)
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            token_id_or_ccd="ccd",
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )

                elif "transfer_cis2_tokens_event" in le.event_info.event_type:
                    assert isinstance(le.recognized_event, transferCIS2TokensEvent)
                    impacted_address = le.recognized_event.from_public_key_ed25519
                    cis5_impacted.append(
                        CIS5Impacted(
                            wallet_contract_address=wallet_contract_address,
                            cis2_token_contract_address=cis2_token_contract_address,
                            token_id_or_ccd=le.recognized_event.token_id,
                            address_or_public_key=impacted_address,
                            address_canonical_or_public_key=impacted_address,
                        )
                    )
                    impacted_address = le.recognized_event.to_public_key_ed25519
                    # Special case for 5TARS. Their burn is implemented as a transfer to "0".
                    if (
                        impacted_address
                        != "0000000000000000000000000000000000000000000000000000000000000000"
                    ):
                        cis5_impacted.append(
                            CIS5Impacted(
                                wallet_contract_address=wallet_contract_address,
                                cis2_token_contract_address=cis2_token_contract_address,
                                token_id_or_ccd=le.recognized_event.token_id,
                                address_or_public_key=impacted_address,
                                address_canonical_or_public_key=impacted_address,
                            )
                        )

            # finally store cis5 impacts

            for cis5_ia in cis5_impacted:
                wallet_contract_address = CCD_ContractAddress.from_str(
                    cis5_ia.wallet_contract_address
                )

                # need to implement BalanceOF here.
                # If token amount is 0, then remove
                # Only add/update if token amount > 0
                cis2_token_address_or_ccd = (
                    f"{cis5_ia.cis2_token_contract_address}-{cis5_ia.token_id_or_ccd}"
                    if cis5_ia.cis2_token_contract_address
                    else cis5_ia.token_id_or_ccd
                )
                _id = f"{cis5_ia.wallet_contract_address}-{cis2_token_address_or_ccd}-{cis5_ia.address_or_public_key}"

                # find token amount

                if cis5_ia.cis2_token_contract_address:
                    result = db_to_use[Collections.instances].find_one(
                        {"_id": wallet_contract_address.to_str()}
                    )
                    instance = MongoTypeInstance(**result)  # type: ignore

                    if instance and instance.v1:
                        entrypoint = instance.v1.name[5:] + ".cis2BalanceOf"
                    else:
                        return []
                    cis2_token_contract_address = CCD_ContractAddress.from_str(
                        cis5_ia.cis2_token_contract_address
                    )

                    if len(cis5_ia.address_or_public_key) == 64:
                        # public key
                        ci = CIS(
                            self.grpc_client,
                            wallet_contract_address.index,
                            wallet_contract_address.subindex,
                            entrypoint,
                            NET(self.net),
                        )
                        rr, ii = ci.CIS2balanceOf(
                            "last_final",
                            cis2_token_contract_address,
                            cis5_ia.token_id_or_ccd,
                            [cis5_ia.address_or_public_key],
                        )
                    else:
                        # account
                        result = db_to_use[Collections.instances].find_one(
                            {"_id": cis2_token_contract_address.to_str()}
                        )
                        instance = MongoTypeInstance(**result)  # type: ignore

                        if instance and instance.v1:
                            entrypoint = instance.v1.name[5:] + ".balanceOf"
                        else:
                            return []
                        ci = CIS(
                            self.grpc_client,
                            cis2_token_contract_address.index,
                            cis2_token_contract_address.subindex,
                            entrypoint,
                            NET(self.net),
                        )
                        rr, ii = ci.balanceOf(
                            "last_final",
                            cis5_ia.token_id_or_ccd,
                            [cis5_ia.address_or_public_key],
                        )

                    if ii.failure.used_energy > 0:
                        print(ii.failure)
                        # this indicates that we had a lookup failure
                        token_amount = -1
                    else:
                        token_amount = rr[0]

                impact = cis5_ia.model_dump(exclude_none=True)
                impact.update({"_id": _id})
                # depending on token amount, issue a DeleteOne or ReplaceOne
                impact.update({"action_type": "upsert"})
                if cis5_ia.cis2_token_contract_address:
                    if token_amount == 0:
                        impact.update({"action_type": "delete"})

                all_cis5_impacts.append(impact)

        return all_cis5_impacts

    def get_new_cis5_keys_from_logged_events(
        self,
        logged_events: list[MongoTypeLoggedEventV2],
        current_known_cis5_public_keys: dict,
    ) -> dict:
        """
        To account for the non-CIS event from 5-stars, we need to check for
        the existence of a public key, however, the variable new_cis5_keys
        is a bit of a misnomer now, as we also use it to track public keys
        from contracts (such as 5-stars) that do not have a CIS-5 event.
        """
        new_cis5_keys = {}
        for le in logged_events:
            if (not le.event_info.standard) and (
                le.event_info.event_type != "five_stars_register_access_event"
            ):
                # so if we do have a five_stars_register_access_event, we need to parse.
                continue

            if not le.recognized_event:
                continue
            to_ = None
            from_ = None
            sponsoree_ = None
            if le.event_info.standard == StandardIdentifiers.CIS_5.value:
                wallet_contract_address = le.event_info.contract
                if ("deposit" in le.event_info.event_type) or (  # type: ignore
                    "transfer" in le.event_info.event_type
                ):  # type: ignore
                    to_ = le.recognized_event.to_public_key_ed25519  # type: ignore

                if ("withdraw" in le.event_info.event_type) or (  # type: ignore
                    "transfer" in le.event_info.event_type
                ):
                    from_ = le.recognized_event.from_public_key_ed25519  # type: ignore

                if "nonce" in le.event_info.event_type:  # type: ignore
                    sponsoree_ = le.recognized_event.sponsoree  # type: ignore

            elif le.event_info.event_type == "five_stars_register_access_event":
                wallet_contract_address = le.event_info.contract
                # le.recognized_event: fiveStarsRegisterAccessEvent  # type: ignore
                to_ = le.recognized_event.public_key  # type: ignore

            if to_:
                to_key = f"{wallet_contract_address}-{to_}"  # type: ignore
                if (
                    to_key not in current_known_cis5_public_keys.keys()
                    and to_key not in new_cis5_keys.keys()
                ):
                    new_cis5_keys[to_key] = {
                        "public_key": to_,
                        "wallet_contract_address": wallet_contract_address,  # type: ignore
                        "deployment_tx": le.tx_info.tx_hash,
                        "deployment_block_height": le.tx_info.block_height,
                        "date": f"{le.tx_info.date}",
                    }

            if from_:
                from_key = f"{wallet_contract_address}-{from_}"  # type: ignore
                if (
                    from_key not in current_known_cis5_public_keys.keys()
                    and from_key not in new_cis5_keys.keys()
                ):
                    new_cis5_keys[from_key] = {
                        "public_key": from_,
                        "wallet_contract_address": wallet_contract_address,  # type: ignore
                        "deployment_tx": le.tx_info.tx_hash,
                        "deployment_block_height": le.tx_info.block_height,
                        "date": f"{le.tx_info.date}",
                    }

            if sponsoree_:
                sponsoree_key = f"{wallet_contract_address}-{sponsoree_}"  # type: ignore
                if (
                    sponsoree_key not in current_known_cis5_public_keys.keys()
                    and sponsoree_key not in new_cis5_keys.keys()
                ):
                    new_cis5_keys[sponsoree_key] = {
                        "public_key": sponsoree_key,
                        "wallet_contract_address": wallet_contract_address,  # type: ignore
                        "deployment_tx": le.tx_info.tx_hash,
                        "deployment_block_height": le.tx_info.block_height,
                        "date": f"{le.tx_info.date}",
                    }

        current_known_cis5_public_keys.update(new_cis5_keys)
        return new_cis5_keys

    def get_schema_from_source(self, module_ref: str, net: str):
        self.source_module_ref_to_schema_cache: dict
        self.grpc_client: GRPCClient
        if not self.source_module_ref_to_schema_cache.get(module_ref):
            ms: VersionedModuleSource = self.grpc_client.get_module_source_original_classes(
                module_ref, "last_final", net=NET(net)
            )
            schema = Schema(ms.v1.value, 1) if ms.v1 else Schema(ms.v0.value, 0)
            self.source_module_ref_to_schema_cache[module_ref] = schema
        else:
            schema = self.source_module_ref_to_schema_cache[module_ref]
        return schema

    def init_cis(self, contract_index, contract_subindex, entrypoint) -> CIS:
        cis = CIS(
            self.grpc_client,
            contract_index,
            contract_subindex,
            entrypoint,
            NET(self.net),
        )

        return cis

    def generate_event_id(self, req: CISProcessEventRequest) -> str:
        part_1 = f"{req.tx.block_info.height}-{req.tx.hash}-"
        part_2 = (
            f"contract_initialized-{req.event_index}"
            if req.tx.account_transaction.effects.contract_initialized
            else f"contract_updated-{req.effect_index}-{req.effect_type}-{req.event_index}"
        )
        return part_1 + part_2

    def formulate_logged_event(self, req: CISProcessEventRequest) -> LoggedEventFromTX:
        _id = self.generate_event_id(req)

        add_token_address_to_event_info = False
        if req.standard == StandardIdentifiers.CIS_2:
            if req.tag in [255, 254, 253, 252, 251, 250]:
                add_token_address_to_event_info = True
                req.recognized_event: (
                    mintEvent,
                    burnEvent,
                    transferEvent,
                    tokenMetadataEvent,
                    updateOperatorEvent,
                )  # type: ignore
                if req.tag == 252:
                    token_address = f"{req.instance_address}-operator"
                elif req.tag == 250:
                    token_address = f"{req.instance_address}-nonce"
                else:
                    # only try to get token_id if it is actually a recognized event
                    # this filters out wrongly formatted hexes we can't parse.
                    if req.recognized_event:
                        token_address = f"{req.instance_address}-{req.recognized_event.token_id}"
                    else:
                        token_address = ""

        if req.standard == StandardIdentifiers.CIS_6:
            # We need to parse the schema to get the string versions of the status.
            (
                source_module_ref,
                source_module_name,
            ) = self.get_source_module_refs_from_instance(req.instance_address)
            schema = self.get_schema_from_source(source_module_ref, self.net)
            try:
                event_json = schema.event_to_json(
                    source_module_name,
                    bytes.fromhex(req.event),
                )
                if req.tag == 236:
                    req.recognized_event: itemStatusChangedEvent  # type: ignore
                    req.recognized_event.new_status = list(
                        event_json["ItemStatusChanged"][0]["new_status"].keys()
                    )[0]
                elif req.tag == 237:
                    req.recognized_event: itemCreatedEvent  # type: ignore
                    req.recognized_event.initial_status = list(
                        event_json["ItemCreated"][0]["initial_status"].keys()
                    )[0]
            except Exception as _:
                pass

        d_event_info = {
            "contract": req.instance_address,
            "logged_event": req.event,
            "effect_index": req.effect_index,
            "event_index": req.event_index,
        }
        if req.standard:
            d_event_info.update({"standard": req.standard.value})

        if req.event_name:
            d_event_info.update({"event_type": req.event_name})

        if add_token_address_to_event_info:
            d_event_info.update({"token_address": token_address})

        d_tx_info = {
            "date": f"{req.tx.block_info.slot_time:%Y-%m-%d}",
            "tx_hash": req.tx.hash,
            "tx_index": req.tx.index,
            "block_height": req.tx.block_info.height,
        }
        d = {
            "_id": _id,
            "event_info": d_event_info,
            "tx_info": d_tx_info,
        }

        if req.recognized_event:
            recognized_event_dict = req.recognized_event.model_dump()
            if "token_amount" in recognized_event_dict:
                recognized_event_dict["token_amount"] = str(recognized_event_dict["token_amount"])
            if "ccd_amount" in recognized_event_dict:
                recognized_event_dict["ccd_amount"] = str(recognized_event_dict["ccd_amount"])

            d.update({"recognized_event": recognized_event_dict})

            if "to_address" in recognized_event_dict:
                if recognized_event_dict["to_address"] is not None:
                    d.update({"to_address_canonical": recognized_event_dict["to_address"][:29]})
            if "from_address" in recognized_event_dict:
                if recognized_event_dict["from_address"] is not None:
                    d.update({"from_address_canonical": recognized_event_dict["from_address"][:29]})

            # do not cut off stuff after 29 characters. There is no canonical version
            # of a public key.
            if "to_public_key_ed25519" in recognized_event_dict:
                d.update({"to_address_canonical": recognized_event_dict["to_public_key_ed25519"]})
            if "from_public_key_ed25519" in recognized_event_dict:
                d.update(
                    {"from_address_canonical": recognized_event_dict["from_public_key_ed25519"]}
                )
        return LoggedEventFromTX(for_collection=d, for_ia=MongoTypeLoggedEventV2(**d))

    def create_upsert_from(self, the_list: list[dict], use_action_indicator: bool = False):
        if len(the_list) > 0:
            if isinstance(the_list[0], dict):
                if not use_action_indicator:
                    return [ReplaceOne({"_id": x["_id"]}, x, True) for x in the_list]
                else:
                    # this is used to make sure CIS-5 tokens where the account
                    # token amount is 0, are deleted from the collection.
                    return_list = []
                    for item in the_list:
                        if item["action_type"] == "delete":
                            return_list.append(DeleteOne({"_id": item["_id"]}))
                        else:
                            return_list.append(ReplaceOne({"_id": item["_id"]}, item, True))
                    return return_list
            else:
                the_list_amended = []
                for item in the_list:
                    item: BaseModel
                    repl_dict = item.model_dump(exclude_none=True)
                    if "id" in repl_dict:
                        repl_dict["_id"] = repl_dict["id"]
                        del repl_dict["id"]
                    the_list_amended.append(repl_dict)
                return [ReplaceOne({"_id": x["_id"]}, x, True) for x in the_list_amended]
        else:
            return []

    def create_upsert_from_dict(self, the_dict: dict):
        return [ReplaceOne({"_id": k}, v, True) for k, v in the_dict.items()]

    async def process_new_logged_events_from_block(
        self,
        net: NET,
        block_height: int,
        block_hash: str,
        store_progress: bool = True,
        cleanup: bool = False,
    ):
        """
        We work on a block, as that's the unit of work for Heartbeat, and the reference point for everyting
        on chain.
        We retrieve transaction summaries from the db. Next, we loop through all txs
        and extract all (logged) events from each tx. Note that we store all events, even events we
        cannot process.
        From the events from all txs in a block we then extract impacted addresses. Note this includes
        regular addresses, contract addresses but also the smart contract addresses (CIS-5).
        Finally, we update the block_log with relevant information.
        """
        self.block_height = block_height
        self.block_hash = block_hash
        db_to_use = self.testnet if net.value == "testnet" else self.mainnet
        # block_transactions = list(
        #     db_to_use[Collections.transactions].find(
        #         {"block_info.height": block_height}
        #     )
        # )
        block_info: CCD_BlockInfo = self.grpc_client.get_block_info(block_height, net)
        short_block_info: CCD_ShortBlockInfo = CCD_ShortBlockInfo(**block_info.model_dump())
        ccd_block = self.grpc_client.get_block_transaction_events(block_height, net)
        block_transactions = ccd_block.transaction_summaries

        logged_events_from_tx: LoggedEventsFromTX = LoggedEventsFromTX()
        impacted_addresses_from_events_to_store_in_collection: list[MongoTokensImpactedAddress] = []
        cis5_keys_and_contracts: list[MongoTokensImpactedAddress] = []
        impacted_addresses_from_balance_movements: list = []
        new_cis5_public_keys = {}
        if len(block_transactions) > 0:
            current_known_cis5_public_keys = self.get_current_known_ci5_public_keys(db_to_use)

        for tx_index, tx in enumerate(block_transactions):
            tx.block_info = short_block_info
            impacted_addresses_from_balance_movements.extend(
                self.extract_impacted_addresses_from_tx(tx)
            )

            logged_events_from_this_tx = self.process_new_logged_events_from_tx(net, tx)
            # extend the final lists of logged events for the block
            logged_events_from_tx.for_collection.extend(logged_events_from_this_tx.for_collection)
            logged_events_from_tx.for_ia.extend(logged_events_from_this_tx.for_ia)

            if len(logged_events_from_tx.for_ia) > 0:
                impacted_addresses_from_events_to_store_in_collection.extend(
                    self.get_impacted_addresses_from_logged_events(logged_events_from_tx.for_ia)
                )

                cis5_keys_and_contracts.extend(
                    self.get_cis5_keys_and_contracts_from_logged_events(
                        logged_events_from_tx.for_ia  # type: ignore
                    )
                )

                new_cis5_public_keys.update(
                    self.get_new_cis5_keys_from_logged_events(
                        logged_events_from_tx.for_ia,
                        current_known_cis5_public_keys,  # type: ignore
                    )
                )

        logged_events_for_collection = self.create_upsert_from(logged_events_from_tx.for_collection)
        impacted_addresses_for_collection = self.create_upsert_from(
            impacted_addresses_from_events_to_store_in_collection  # type: ignore
        )
        cis5_keys_and_contracts_for_collection = self.create_upsert_from(
            cis5_keys_and_contracts,
            use_action_indicator=True,  # type: ignore
        )
        new_cis5_public_keys_for_collection = self.create_upsert_from_dict(new_cis5_public_keys)

        impacted_addresses_from_balance_movements_for_collection = self.create_upsert_from(
            impacted_addresses_from_balance_movements
        )
        # now combine impacted addresses from balance movements and from logged events
        impacted_addresses_for_collection.extend(
            impacted_addresses_from_balance_movements_for_collection
        )

        if len(logged_events_for_collection) > 0:
            _ = db_to_use[Collections.tokens_logged_events_v2].bulk_write(
                logged_events_for_collection
            )
            await self.send_logged_events_to_redis(logged_events_from_tx)

        if len(impacted_addresses_for_collection) > 0:
            _ = db_to_use[Collections.impacted_addresses].bulk_write(
                impacted_addresses_for_collection
            )

        if len(cis5_keys_and_contracts_for_collection) > 0:
            _ = db_to_use[Collections.cis5_public_keys_contracts].bulk_write(
                cis5_keys_and_contracts_for_collection
            )

        if len(new_cis5_public_keys_for_collection) > 0:
            _ = db_to_use[Collections.cis5_public_keys_info].bulk_write(
                new_cis5_public_keys_for_collection
            )

        blocks_log: dict | None = db_to_use[Collections.blocks_log].find_one({"_id": block_height})
        for item in [
            "transaction_hashes",
            "tokens_logged_events",
            "impacted_addresses",
            "special_events",
            "tokens_logged_events_v2",
            "logged_events",
            "impacted_addresses_from_events",
            "impacted_addresses_from_balance_movements",
        ]:
            if blocks_log:
                if item in blocks_log:
                    del blocks_log[item]

        if not blocks_log:
            blocks_log = {
                "_id": block_height,
                "hash": block_hash,
                "process_date": dt.datetime.now().astimezone(dt.timezone.utc),
                "version": 2,
            }

        if len(logged_events_from_tx.for_collection) > 0:
            blocks_log.update(
                {"logged_events": [x["_id"] for x in logged_events_from_tx.for_collection]}
            )
        if len(impacted_addresses_from_events_to_store_in_collection) > 0:
            blocks_log.update(
                {
                    "impacted_addresses_from_events": [
                        x.id for x in impacted_addresses_from_events_to_store_in_collection
                    ]
                }
            )
        if len(impacted_addresses_from_balance_movements) > 0:
            blocks_log.update(
                {
                    "impacted_addresses_from_balance_movements": [
                        x.id for x in impacted_addresses_from_balance_movements
                    ],
                    "version": 2,
                }
            )
        if len(impacted_addresses_from_balance_movements) > 0:
            document_to_save = ReplaceOne(
                {"_id": block_height},
                blocks_log,
                upsert=True,
            )

            if len(impacted_addresses_from_events_to_store_in_collection) > 100_000:
                _ = db_to_use[Collections.blocks_log].bulk_write(
                    [
                        ReplaceOne(
                            {"_id": block_height},
                            {
                                "_id": block_height,
                                "error": f"Document too large. {len(impacted_addresses_from_events_to_store_in_collection)=:,.0f}",
                            },
                            upsert=True,
                        )
                    ]
                )
            else:
                _ = db_to_use[Collections.blocks_log].bulk_write([document_to_save])

        else:
            _ = db_to_use[Collections.blocks_log].bulk_write([DeleteOne({"_id": block_height})])
        print(
            f"{net.value}: {block_height:,.0f} - {len(block_transactions)} tx(s). IA (tx / events): {len(impacted_addresses_from_balance_movements)} / {len(impacted_addresses_from_events_to_store_in_collection)}"
        )

        # only in regular processing, not in cleanup
        if store_progress:
            query = {"_id": "event_creation_last_processed_block"}
            db_to_use[Collections.helpers].replace_one(
                query,
                {
                    "_id": "event_creation_last_processed_block",
                    "height": block_height,
                },
                upsert=True,
            )

    def process_new_logged_events_from_tx(
        self, net: NET, tx: CCD_BlockItemSummary
    ) -> LoggedEventsFromTX:
        """
        This method tries to parse any events that may exist in this transaction.
        Only logged events that are specified in the CIS-Standards can be parsed,
        as only for these events the schema is known.
        For a few events it is needed to also know the CIS-Standard that the contract
        adheres to, as there are overlapping `tags`.
        We determine the CIS standard by calling `supports`.
        """
        self.motor_mainnet: dict[Collections, Collection]
        self.motor_testnet: dict[Collections, Collection]
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter
        self.net = net

        db_to_use = self.testnet if net.value == "testnet" else self.mainnet
        logged_events_from_tx = LoggedEventsFromTX()
        if not tx.account_transaction:
            return logged_events_from_tx

        if tx.account_transaction.effects.contract_initialized:
            contract_index = tx.account_transaction.effects.contract_initialized.address.index
            contract_subindex = tx.account_transaction.effects.contract_initialized.address.subindex
            instance_address = CCD_ContractAddress.from_index(
                contract_index, contract_subindex
            ).to_str()

            entrypoint = (
                f"{tx.account_transaction.effects.contract_initialized.init_name[5:]}.supports"
            )
            contract_name = entrypoint.split(".")[0]
            cis = self.init_cis(contract_index, contract_subindex, entrypoint)
            supports_cis_standards = self.find_cis_standards_support(cis, instance_address)

            for event_index, event in enumerate(
                tx.account_transaction.effects.contract_initialized.events
            ):
                tag, recognized_event, event_name, cis_standard = cis.recognize_event(
                    event, supports_cis_standards, contract_name
                )

                # log ALL events
                cis_process_event_request = CISProcessEventRequest(
                    tx=tx,
                    event_index=event_index,
                    standard=cis_standard,
                    instance_address=instance_address,
                    event=event,
                    event_name=event_name,
                    tag=tag,
                    recognized_event=recognized_event,
                )

                logged_event_from_tx = self.formulate_logged_event(cis_process_event_request)

                logged_events_from_tx.for_collection.append(logged_event_from_tx.for_collection)
                logged_events_from_tx.for_ia.append(logged_event_from_tx.for_ia)

        elif tx.account_transaction.effects.contract_update_issued:
            for effect_index, effect in enumerate(
                tx.account_transaction.effects.contract_update_issued.effects
            ):
                if effect.interrupted:
                    effect_type = "interrupted"
                    contract_index = effect.interrupted.address.index
                    contract_subindex = effect.interrupted.address.subindex
                    instance_address = CCD_ContractAddress.from_index(
                        contract_index, contract_subindex
                    ).to_str()
                    # we need to get the instance name from the collection,
                    # as receive_name is not a property of the interrupted event.
                    entrypoint = self.get_entrypoint_for_instance_address(
                        db_to_use, instance_address
                    )

                if effect.updated:
                    effect_type = "updated"
                    contract_index = effect.updated.address.index
                    contract_subindex = effect.updated.address.subindex
                    instance_address = CCD_ContractAddress.from_index(
                        contract_index, contract_subindex
                    ).to_str()
                    entrypoint = f"{effect.updated.receive_name.split('.')[0]}.supports"

                if effect.interrupted or effect.updated:
                    contract_name = entrypoint.split(".")[0]  # type: ignore
                    cis = CIS(
                        self.grpc_client,
                        contract_index,  # type: ignore
                        contract_subindex,  # type: ignore
                        entrypoint,  # type: ignore
                        NET(self.net),
                    )
                    supports_cis_standards: list[StandardIdentifiers] = (
                        self.find_cis_standards_support(cis, instance_address)  # type: ignore
                    )

                    events_to_loop_through = (
                        effect.updated.events if effect.updated else effect.interrupted.events  # type: ignore
                    )
                    for event_index, event in enumerate(events_to_loop_through):  # type: ignore
                        tag, recognized_event, event_name, cis_standard = cis.recognize_event(
                            event, supports_cis_standards, contract_name
                        )

                        cis_process_event_request = CISProcessEventRequest(
                            tx=tx,
                            event_index=event_index,
                            standard=cis_standard,
                            instance_address=instance_address,  # type: ignore
                            event=event,
                            event_name=event_name,
                            tag=tag,
                            recognized_event=recognized_event,
                            effect_index=effect_index,
                            effect_type=effect_type,  # type: ignore
                        )

                        logged_event_from_tx = self.formulate_logged_event(
                            cis_process_event_request
                        )

                        logged_events_from_tx.for_collection.append(
                            logged_event_from_tx.for_collection
                        )
                        logged_events_from_tx.for_ia.append(logged_event_from_tx.for_ia)

        return logged_events_from_tx

    def get_entrypoint_for_instance_address(self, db_to_use, instance_address) -> str | None:
        self.instances_cache: dict
        if instance_address not in self.instances_cache:
            try:
                result = db_to_use[Collections.instances].find_one({"_id": instance_address})
                instance = MongoTypeInstance(**result)
            except Exception as e:  # noqa: E722
                print(e)
                instance = None
                entrypoint = None

            if instance and instance.v1:
                entrypoint = instance.v1.name[5:] + ".supports"

            self.instances_cache[instance_address] = entrypoint  # type: ignore
        return self.instances_cache[instance_address]

    async def find_cis_standard_support(
        self, contract_index, cis: CIS
    ) -> StandardIdentifiers | None:
        if contract_index not in self.contract_supports:
            # Either this loops stops with the standard added to the dictionary
            # for this contract, or no contract is added, meaning that this contract
            # does not report it supporting any of the CIS-Standards.
            for standard in reversed(StandardIdentifiers):
                if cis.supports_standards([standard]):
                    self.contract_supports[contract_index] = standard
                    break

        supports_cis_standard: StandardIdentifiers | None = self.contract_supports.get(
            contract_index
        )

        return supports_cis_standard

    # @line_profiler.profile
    def find_cis_standards_support(
        self, cis: CIS, instance_address: str
    ) -> list[StandardIdentifiers]:
        # This lists all Standards that are said to be supported
        self.contract_supports: dict
        if instance_address not in self.contract_supports:
            standards_supported = []
            for standard in reversed(StandardIdentifiers):
                if cis.supports_standards([standard]):
                    standards_supported.append(standard)
            self.contract_supports[instance_address] = standards_supported
        return self.contract_supports[instance_address]

    def get_source_module_refs_from_instance(self, instance_address: str):
        self.source_module_refs: dict
        db: dict[Collections, Collection] = (
            self.mainnet if self.net.value == "mainnet" else self.testnet
        )
        if not self.source_module_refs.get(instance_address):
            result = db[Collections.instances].find_one({"_id": instance_address})
            if result.get("v0"):  # type: ignore
                source_module_result = {
                    "source_module_ref": result["v0"]["source_module"],  # type: ignore
                    "module_name": result["v0"]["name"][5:],  # type: ignore
                }
            else:
                source_module_result = {
                    "source_module_ref": result["v1"]["source_module"],  # type: ignore
                    "module_name": result["v1"]["name"][5:],  # type: ignore
                }

            self.source_module_refs[instance_address] = source_module_result
        return (
            self.source_module_refs[instance_address]["source_module_ref"],
            self.source_module_refs[instance_address]["module_name"],
        )

    async def send_logged_events_to_redis(self, logged_events_from_tx: LoggedEventsFromTX):
        send = False
        if len(logged_events_from_tx.for_ia) > 0:
            for le in logged_events_from_tx.for_ia:
                if le.event_info:
                    if le.event_info.standard == "CIS-2":
                        send = True

            if send:
                await publish_to_celery(
                    "token_accounting",
                    {"height": le.tx_info.block_height},  # type: ignore
                )

                print(
                    f"logged event for block {le.tx_info.block_height} to redis via celery."  # type: ignore
                )
