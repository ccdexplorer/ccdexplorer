import datetime as dt
from datetime import timezone

import httpx2 as httpx
from ccdexplorer.cis import (
    CIS,
)
from ccdexplorer.domain.mongo import (
    FailedAttempt,
    MongoTypeTokenAddress,
    TokenMetaData,
)
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_ContractAddress
from ccdexplorer.mongodb import Collections, net_db
from pydantic import BaseModel, ConfigDict
from pymongo.collection import Collection
from rich.console import Console

from .utils import Utils

console = Console()


class GetTokenMetadataRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    net: str
    contract_address: CCD_ContractAddress
    token_id: str
    module_name: str
    grpcclient: GRPCClient


class MetaData(Utils):
    def get_module_name_from_contract_address(
        self, db_to_use, contract_address: CCD_ContractAddress
    ) -> str:
        module_name = "module_name"
        instance_result = db_to_use[Collections.instances].find_one(
            {"_id": contract_address.to_str()}
        )
        if "v1" in instance_result:
            module_name = instance_result["v1"]["name"].replace("init_", "")
        elif "v0" in instance_result:
            module_name = instance_result["v0"]["name"].replace("init_", "")
        return module_name

    def save_token_address(
        self,
        token_address_as_class: MongoTypeTokenAddress,
        db: dict[Collections, Collection],
    ):
        dom_dict = token_address_as_class.model_dump(exclude_none=True)
        if "id" in dom_dict:
            del dom_dict["id"]
        _ = db[Collections.tokens_token_addresses_v2].replace_one(  # type:ignore
            {"_id": token_address_as_class.id},
            replacement=dom_dict,
            upsert=True,
        )

    def get_tokenMetadata(self, req: GetTokenMetadataRequest):
        """
        This function allows the api to get the metadata
        from the specified token.
        To make this call, we need the contract, the corresponding module name and token_id.
        """
        ci = CIS(
            req.grpcclient,
            req.contract_address.index,
            req.contract_address.subindex,
            f"{req.module_name}.tokenMetadata",
            NET(req.net),
        )
        response = ci.invoke_token_metadataUrl(req.token_id)
        if response:
            # A token that exists but has no URL yet parses as MetadataUrl(url=""),
            # so collapse blank onto None rather than handing "" to the caller.
            return response[0].url.strip() or None
        return None

    def split_into_url_slug(self, token_address: str):
        contract = CCD_ContractAddress.from_str(token_address.split("-")[0])
        token_id = token_address.split("-")[1]
        return f"{contract.index}/{contract.subindex}/{token_id}"

    def read_and_store_metadata(
        self,
        db_to_use: dict[Collections, Collection],
        token_address_str: str,
        httpx_client: httpx.Client,
        net: NET,
    ):
        """
        This method takes as input a typed Token address and, depending on previous attempts,
        tries to read the contents of the metadata Url. Failed attempts are stored in the collection
        itself, where the time to next try increases quadratically.
        """
        self.grpc_client: GRPCClient

        token_address_to_process = db_to_use[Collections.tokens_token_addresses_v2].find_one(
            {"_id": token_address_str}
        )
        if token_address_to_process is None:
            return None

        token_address_to_process = MongoTypeTokenAddress(**token_address_to_process)

        url = token_address_to_process.metadata_url
        error = None
        # A token can exist before its metadata URL does: CIS-8004 agents, for
        # instance, are created by `register` and only get a URL later, from a
        # separate `setAgentURI` transaction. That is a different situation from
        # a URL that is published but broken, and it should not be punished with
        # the same escalating backoff.
        awaiting_url = False

        try:
            if url is None:
                this_contract = CCD_ContractAddress.from_str(token_address_to_process.contract)
                request = GetTokenMetadataRequest(
                    net=net.value,
                    contract_address=this_contract,
                    token_id=token_address_to_process.token_id,
                    module_name=self.get_module_name_from_contract_address(
                        db_to_use, this_contract
                    ),
                    grpcclient=self.grpc_client,
                )
                url = self.get_tokenMetadata(request)

            url = (url or "").strip()
            # What the contract published, before the ipfs gateway rewrite below.
            # That rewrite is a fetch detail; pinning a gateway into the stored
            # document would outlive whichever gateway we happen to use today.
            published_url = url
            do_request = False

            if not url:
                awaiting_url = True
                error = f"Metadata error: no metadata URL published (yet) for token {token_address_str}."
                console.log(error)
            else:
                if url.startswith("ipfs://"):
                    url = f"https://ipfs.io/ipfs/{url[len('ipfs://') :]}"

                # httpx raises UnsupportedProtocol for anything else, which reads
                # like a network fault rather than a bad URL on chain.
                if url.startswith(("http://", "https://")):
                    do_request = True
                else:
                    error = (
                        f"Metadata error: unsupported URL scheme for token "
                        f"{token_address_str}: {url!r}."
                    )
                    console.log(error)

            if do_request:
                resp = httpx_client.get(url)
                resp.raise_for_status()
                t = resp.json()

                metadata = None
                if resp.status_code == 200:
                    try:
                        metadata = TokenMetaData(**t)
                        token_address_to_process.token_metadata = metadata
                        # Record where the metadata came from. Tokens whose URL
                        # is only reachable through tokenMetadata -- CIS-8004
                        # agents, which emit no CIS-2 TokenMetadata event -- had
                        # no metadata_url stored at all, so the token page
                        # rendered an empty href that resolved to itself.
                        token_address_to_process.metadata_url = published_url
                        token_address_to_process.failed_attempt = None
                        self.save_token_address(token_address_to_process, db_to_use)
                        console.log(f"URL parsed for token {token_address_str}.")

                    except Exception as e:
                        error = f"Metadata error: URL resolved, but metadata is malformed for token {token_address_str}. Error: {e}"
                        console.log(error)

                else:
                    error = f"Metadata error: URL {url} for token {token_address_str} resulted in status {resp.status_code}."
                    console.log(error)

        except Exception as e:
            error = f"{type(e).__name__}: {e.args!r}"
            console.log(f"Error fetching metadata for token {token_address_str}: {error}")

        if error is not None:
            failed_attempt = token_address_to_process.failed_attempt
            attempts = (failed_attempt.attempts + 1) if failed_attempt else 1

            if awaiting_url:
                # Nothing is wrong yet -- the URL simply has not been published.
                # Come back in minutes rather than hours, easing off so a token
                # that never gets one does not get polled forever.
                retry_in = dt.timedelta(minutes=5 * min(attempts, 12))
            elif attempts == 1:
                retry_in = dt.timedelta(hours=2)
            else:
                retry_in = dt.timedelta(hours=attempts * attempts)

            token_address_to_process.failed_attempt = FailedAttempt(
                attempts=attempts,
                do_not_try_before=dt.datetime.now().astimezone(tz=timezone.utc) + retry_in,
                last_error=error,
            )
            self.save_token_address(token_address_to_process, db_to_use)

        return error

    def fetch_token_metadata(
        self, net: NET, token_address: str, httpx_client: httpx.Client
    ) -> str | None:
        """
        The message contains the token address info from the `token_addresses_v2` collection
        for which we are going to fetch the metadata.

        Returns the error read_and_store_metadata recorded, or None on success --
        swallowing it meant a caller could not tell the two apart.
        """
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        db_to_use: dict[Collections, Collection] = net_db(self, net)
        return self.read_and_store_metadata(db_to_use, token_address, httpx_client, net)
