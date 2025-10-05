import datetime as dt
import io
import os
import re
import shutil
import subprocess
import tarfile
from datetime import timedelta
from pathlib import Path

import ccdexplorer.grpc_client.wadze as wadze
import httpx
from ccdexplorer.domain.generic import NET
from pymongo.asynchronous.collection import AsyncCollection
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.domain.mongo import ModuleVerification, MongoTypeInstance, MongoTypeModule
from ccdexplorer.mongodb import (
    Collections,
)
from ccdexplorer.tooter import Tooter
from dateutil.relativedelta import relativedelta
from pymongo import ReplaceOne
from pymongo.collection import Collection
from rich.console import Console
from sortedcontainers import SortedDict

from ccdexplorer.concordium_client import ConcordiumClient


console = Console()


class Module:
    async def process_block_for_module(self, db_to_use, block_height: int, net: NET):
        self.concordium_client: ConcordiumClient
        pipeline = [{"$match": {"block_info.height": block_height}}]
        txs = [
            CCD_BlockItemSummary(**x)
            for x in db_to_use[Collections.transactions].aggregate(pipeline)
        ]
        for tx in txs:
            print(f"Processing transaction: {tx}")
            if tx.account_transaction:
                if tx.account_transaction.effects.module_deployed:
                    module_ref = tx.account_transaction.effects.module_deployed
                    print(f"Found module_deployed: {module_ref}")
                    await self.process_new_module(net, module_ref)
                    print(f"Processed new module: {module_ref}, now verifying...")
                    await self.verify_module(net, self.concordium_client, module_ref)
                    print(f"Verified module: {module_ref}")
                    if net == NET.MAINNET:
                        await self.save_smart_contracts_overview(net)

    # Add this helper at class level
    def get_project_root(self):
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def get_module_metadata(self, net: NET, block_hash: str, module_ref: str) -> dict[str, str]:
        """
        Retrieves metadata for a specified module. Parses the web assembly source code.

        Args:
            net (NET): The network from which to retrieve the module.
            block_hash (str): The hash of the block containing the module.
            module_ref (str): The reference identifier for the module.

        Returns:
            dict[str, str]: A dictionary containing the module's metadata. The keys include:
                - "module_name": The name of the module (if found).
                - "methods": A list of method names exported by the module (if any).

        Raises:
            Exception: If there is an error parsing the module, an error message is sent to the tooter and an empty dictionary is returned.
        """
        self.grpc_client: GRPCClient
        ms = self.grpc_client.get_module_source(module_ref, block_hash, net)

        if ms.v0:
            bs = io.BytesIO(bytes.fromhex(ms.v0))
        else:
            bs = io.BytesIO(bytes.fromhex(ms.v1))  # type: ignore

        try:
            module = wadze.parse_module(bs.read())
        except Exception as e:
            tooter_message = f"{net.value}: New module get_module_metadata failed with error  {e}."
            self.tooter.send_to_tooter(tooter_message)
            return {}

        results = {}
        if "export" in module.keys():
            for line in module["export"]:
                split_line = str(line).split("(")
                if split_line[0] == "ExportFunction":
                    split_line = str(line).split("'")
                    name = split_line[1]

                    if name[:5] == "init_":
                        results["module_name"] = name[5:]
                    else:
                        method_name = name.split(".")[1] if "." in name else name
                        if "methods" in results:
                            results["methods"].append(method_name)
                        else:
                            results["methods"] = [method_name]

        return results

    async def process_new_module(self, net: NET, module_ref: str):
        """
        Processes a new module by fetching its metadata and updating the database.
        Args:
            net (NET): The network type (MAINNET or TESTNET).
            module_ref (str): The reference of the module to process.
        Returns:
            None
        Raises:
            Exception: If there is an error while fetching module metadata.
        The function performs the following steps:
        1. Determines the database to use based on the network type.
        2. Fetches the module metadata using the provided module reference.
        3. If an error occurs during metadata fetching, sends an error message to the tooter.
        4. Constructs a module dictionary with the fetched metadata.
        5. Updates the database with the new module information using a bulk write operation.
        6. Sends a success message to the tooter with the module reference and name.
        """

        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        self.grpc_client: GRPCClient
        self.tooter: Tooter

        db_to_use = self.mainnet if net == NET.MAINNET else self.testnet

        try:
            results = self.get_module_metadata(net, "last_final", module_ref)
        except Exception as e:
            tooter_message = f"{net.value}: New module failed with error  {e}."
            self.tooter.send_to_tooter(tooter_message)
            return

        module = {
            "_id": module_ref,
            "module_name": (results["module_name"] if "module_name" in results.keys() else None),
            "methods": results["methods"] if "methods" in results.keys() else [],
            "verification": ModuleVerification(verification_status="not_started").model_dump(
                exclude_none=True
            ),
        }

        _ = db_to_use[Collections.modules].bulk_write(
            [ReplaceOne({"_id": module_ref}, module, upsert=True)]
        )
        tooter_message = (
            f"{net.value}: New module processed {module_ref} with name {module['module_name']}."
        )
        print(tooter_message)
        self.tooter.send_to_tooter(tooter_message)

    async def verify_module(self, net: NET, concordium_client: ConcordiumClient, module_ref: str):
        """
        Verifies a module by checking its build information and source code.
        This asynchronous method performs the following steps:
        1. Determines the appropriate database to use based on the network (mainnet or testnet).
        2. Saves the module using the Concordium client.
        3. Runs a subprocess to print the build information of the module.
        4. Parses the build information to extract the build image, build command, and archive hash.
        5. Checks if the source code link is present in the build information.
        6. If the source code link is present, retrieves the source code from the link.
        7. Extracts the source code and verifies it against the module using a subprocess.
        8. Saves and sends the verification result.
        Args:
            net (NET): The network type (mainnet or testnet).
            concordium_client (ConcordiumClient): The Concordium client used to interact with the blockchain.
            module_ref (str): The reference ID of the module to verify.
        Returns:
            None: This method does not return any value. It performs actions and sends the verification result.
        Raises:
            httpx.HTTPError: If there is an HTTP error while retrieving the source code.
            Exception: If there is an error during the extraction or verification process.
        """
        self.motor_mainnet: dict[Collections, AsyncCollection]
        self.motor_testnet: dict[Collections, AsyncCollection]
        self.tooter: Tooter

        file_path = Path(f"tmp/{module_ref}.out")
        if file_path.exists():
            file_path.unlink()

        db_to_use = self.motor_mainnet if net == NET.MAINNET else self.motor_testnet

        try:
            concordium_client.save_module(net, module_ref)
        except Exception as e:
            print(f"EXCEPTION: {e}")
            verification = ModuleVerification(
                verified=False,
                verification_status="verified_failed",
                verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                explanation=e,  # type: ignore
                build_image_used=None,
                build_command_used=None,
                archive_hash=None,
                link_to_source_code=None,
                source_code_at_verification_time="",
            )
            await self.save_and_send(net, module_ref, db_to_use, verification)
            return None

        cargo_run = subprocess.run(
            [
                "cargo",
                "concordium",
                "print-build-info",
                "--module",
                f"tmp/{module_ref}.out",
            ],
            capture_output=True,
            text=True,
        )
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        result = ansi_escape.sub("", cargo_run.stderr)
        output_list = result.splitlines()
        verification = None

        if len(output_list) == 4:
            build_image_used = output_list[0].split("used: ")[1].strip()
            build_command_used = output_list[1].split("used: ")[1].strip()
            archive_hash = output_list[2].split("archive: ")[1].strip()

            if "source code: " not in output_list[3]:
                verification = ModuleVerification(
                    verified=False,
                    verification_status="verified_failed",
                    verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                    explanation="No source code found.",
                )
                await self.save_and_send(net, module_ref, db_to_use, verification)
                return None
            else:
                link_to_source_code = output_list[3].split("source code: ")[1].strip()
                source_code_at_verification_time = ""

                try:
                    response = await httpx.AsyncClient().get(
                        url=link_to_source_code, follow_redirects=True
                    )
                    response.raise_for_status()
                except httpx.HTTPError as exc:
                    print(f"HTTP Exception for {exc.request.url} - {exc}")
                    verification = ModuleVerification(
                        verified=False,
                        verification_status="verified_failed",
                        verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                        explanation=f"HTTP Exception for {exc.request.url} - {exc}",
                        build_image_used=build_image_used,
                        build_command_used=build_command_used,
                        archive_hash=archive_hash,
                        link_to_source_code=link_to_source_code,
                        source_code_at_verification_time="",
                    )
                    await self.save_and_send(net, module_ref, db_to_use, verification)
                    return None
                try:
                    source_code_at_verification_time = response.content
                    module_folder = tarfile.open(
                        fileobj=io.BytesIO(source_code_at_verification_time), mode="r:*"
                    )
                    print(f"{link_to_source_code=} retrieved.")
                    module_folder.extractall(path=f"tmp/source_{module_ref}")
                    module_name_on_disk = next(os.walk(f"tmp/source_{module_ref}"))[1][0]
                    with open(
                        f"tmp/source_{module_ref}/{module_name_on_disk}/src/lib.rs", "r"
                    ) as file:
                        source_code_at_verification_time = file.read()

                except Exception as e:  # noqa: E722
                    print(f"EXCEPTION: {e}")
                    verification = ModuleVerification(
                        verified=False,
                        verification_status="verified_failed",
                        verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                        explanation=e,  # type: ignore
                        build_image_used=build_image_used,
                        build_command_used=build_command_used,
                        archive_hash=archive_hash,
                        link_to_source_code=link_to_source_code,
                        source_code_at_verification_time="",
                    )
                    await self.save_and_send(net, module_ref, db_to_use, verification)
                    return None

                print(
                    f"{dt.datetime.now().astimezone(dt.UTC)}: Starting subprocess.run for verify-build..."
                )

                module_path = f"tmp/{module_ref}.out"
                project_root = self.get_project_root()
                module_path = os.path.join(project_root, "tmp", f"{module_ref}.out")

                source_dir = f"tmp/source_{module_ref}"
                if os.path.exists(source_dir):
                    shutil.rmtree(source_dir)
                os.makedirs(source_dir, exist_ok=True)

                try:
                    module_folder.extractall(path=source_dir)
                    module_name_on_disk = next(os.walk(source_dir))[1][0]
                    build_dir = os.path.join(source_dir, module_name_on_disk)

                    # Run verify-build from source directory
                    cargo_run = subprocess.run(
                        [
                            "cargo",
                            "concordium",
                            "verify-build",
                            "--module",
                            module_path,
                        ],
                        capture_output=True,
                        text=True,
                        cwd=build_dir,  # Run from source directory
                    )

                except Exception as e:
                    print(f"Build error: {str(e)}")
                    verification = ModuleVerification(
                        verified=False,
                        verification_status="verified_failed",
                        verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                        explanation=e,  # type: ignore
                        build_image_used=build_image_used,
                        build_command_used=build_command_used,
                        archive_hash=archive_hash,
                        link_to_source_code=link_to_source_code,
                        source_code_at_verification_time="",
                    )
                    await self.save_and_send(net, module_ref, db_to_use, verification)
                    return None

                if cargo_run.returncode != 0:
                    print(f"Error: {cargo_run.stderr}")
                    verification = ModuleVerification(
                        verified=False,
                        verification_status="verified_failed",
                        verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                        explanation="The source does not correspond to the module.",
                        build_image_used=build_image_used,
                        build_command_used=build_command_used,
                        archive_hash=archive_hash,
                        link_to_source_code=link_to_source_code,
                        source_code_at_verification_time="",
                    )
                    await self.save_and_send(net, module_ref, db_to_use, verification)
                    return None

                print(
                    f"{dt.datetime.now().astimezone(dt.UTC)}: Subprocess.run for verify-build done."
                )
                result = ansi_escape.sub("", cargo_run.stderr)
                output_list = result.splitlines()
                verified = output_list[-1] == "Source and module match."

                verification = ModuleVerification(
                    verified=verified,
                    verification_status="verified_success",
                    verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                    explanation=(
                        "Source and module match."
                        if verified
                        else "Source and module do not match."
                    ),
                    build_image_used=build_image_used,
                    build_command_used=build_command_used,
                    archive_hash=archive_hash,
                    link_to_source_code=link_to_source_code,
                    source_code_at_verification_time=source_code_at_verification_time,
                )
                await self.save_and_send(net, module_ref, db_to_use, verification)
        else:
            verification = ModuleVerification(
                verified=False,
                verification_status="verified_failed",
                verification_timestamp=dt.datetime.now().astimezone(dt.UTC),
                explanation="No embedded build information found.",
            )
            await self.save_and_send(net, module_ref, db_to_use, verification)
            print("No build info found.")
            return None

    async def save_and_send(self, net, module_ref, db_to_use, verification: ModuleVerification):
        """
        Asynchronously saves the module verification status to the database and sends a notification.
        Args:
            net (Network): The network instance.
            module_ref (str): The reference ID of the module.
            db_to_use (Database): The database instance to use for saving the verification status.
            verification (ModuleVerification): The verification object containing the verification status and explanation.
        Returns:
            None
        Side Effects:
            - Updates the module's verification status in the database.
            - Sends a notification message to the tooter service.
        Example:
            await save_and_send(net, module_ref, db_to_use, verification)
        """
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        db_to_use = self.mainnet if net == NET.MAINNET else self.testnet
        print(f"{module_ref=}: verified status {verification.verified=}")
        module_from_collection = db_to_use[Collections.modules].find_one({"_id": module_ref})
        assert module_from_collection is not None
        module_from_collection.update({"verification": verification.model_dump(exclude_none=True)})

        _ = db_to_use[Collections.modules].bulk_write(
            [ReplaceOne({"_id": module_ref}, module_from_collection, upsert=True)]
        )
        tooter_message = f"{net.value}: Module {module_ref} with name {module_from_collection['module_name']} added verification with status {verification.verified}. Explanation: {verification.explanation}."
        print(tooter_message)
        self.tooter.send_to_tooter(tooter_message)

    async def save_smart_contracts_overview(self, net: NET):
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        db_to_use = self.mainnet if net == NET.MAINNET else self.testnet
        modules_dict = {
            x["_id"]: MongoTypeModule(**x) for x in db_to_use[Collections.modules].find()
        }
        mm = [
            CCD_BlockItemSummary(**x)
            for x in db_to_use[Collections.transactions].find({"type.contents": "module_deployed"})
        ]

        inits_dict = {
            x.account_transaction.effects.module_deployed: x.block_info.slot_time  # type: ignore
            for x in mm
            if x.account_transaction is not None
        }

        # update modules_dict with init dates
        modules_sorted_by_date = SortedDict()
        for module_ref, module in modules_dict.items():
            init_date = inits_dict.get(module_ref, None)
            if init_date:
                module.init_date = init_date
                modules_dict[module_ref] = module
                modules_sorted_by_date[init_date] = module.model_dump(exclude_none=True)
                instances = [
                    MongoTypeInstance(**x).model_dump(exclude_none=True)
                    for x in db_to_use[Collections.instances].find({"source_module": module_ref})
                ]
                modules_sorted_by_date[init_date].update(
                    {"instances": [x["id"] for x in instances]}
                )

                verification = modules_sorted_by_date[init_date]["verification"]
                verification.update({"source_code_at_verification_time": None})
                modules_sorted_by_date[init_date]["verification"] = verification

        # now do an ugly hack to get the modules into year/month buckets
        max_date = max(modules_sorted_by_date.keys())
        min_date = min(modules_sorted_by_date.keys())
        months_list = []
        cur_date = min_date
        while cur_date < max_date:
            end_of_day = dt.datetime.combine(cur_date, dt.time.max)
            next_month = end_of_day.replace(day=28) + timedelta(days=4)
            res = next_month - timedelta(days=next_month.day)
            # print(f"Last date of month is:", res.date())
            months_list.append(
                {
                    "display_string": f"{cur_date.year}-{cur_date.month:02}",
                    "end_of_month_date": res,
                }
            )
            cur_date += relativedelta(months=1)

        the_dict = {}
        for init_date in reversed(modules_sorted_by_date):
            display_string_for_module = f"{init_date.year}-{init_date.month:02}"
            already_found_display_dates = the_dict.get(display_string_for_module, None)
            if not already_found_display_dates:
                the_dict[display_string_for_module] = [modules_sorted_by_date[init_date]]
            else:
                the_dict[display_string_for_module].append(modules_sorted_by_date[init_date])

        # save to mongo collection
        queue = []
        analysis_type = "statistics_modules_overview"
        for year_month in the_dict:
            queue.append(
                ReplaceOne(
                    {"_id": f"{year_month}-{analysis_type}"},
                    {
                        "_id": f"{year_month}-{analysis_type}",
                        "date": f"{year_month}-01",
                        "year_month": year_month,
                        "net": net.value,
                        "type": analysis_type,
                        "modules": the_dict[year_month],
                    },
                    upsert=True,
                ),
            )

        _ = db_to_use[Collections.statistics].bulk_write(queue)
