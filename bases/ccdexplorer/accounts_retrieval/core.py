from __future__ import annotations


import datetime as dt
import pickle
import pandas as pd
import math
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.domain.generic import NET, AccountInfoStable
from ccdexplorer.grpc_client.CCD_Types import CCD_AccountInfo
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
)
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from git import Repo
from rich import print
from rich.console import Console
from pymongo import ReplaceOne


console = Console()


class Account:
    def __init__(self, account, block_hash, grpcclient: GRPCClient):
        """
        The Account class holds all relevant information for an account on chain.
        """
        self.account = account
        self.block_hash = block_hash
        self.grpcclient: GRPCClient = grpcclient
        self.tokens: dict | None = None
        self.process_grpc_account_info()

    def process_grpc_account_info(self):
        ai: CCD_AccountInfo = self.grpcclient.get_account_info(self.block_hash, self.account)
        self.ai_stable: AccountInfoStable = AccountInfoStable.from_account_info(ai)
        if ai.tokens is not None:
            self.tokens = {}
            self.tokens["account_address"] = self.account
            self.tokens["account_index"] = ai.index

            for token in ai.tokens:
                self.tokens[token.token_id] = int(token.token_account_state.balance.value) * (
                    math.pow(10, -token.token_account_state.balance.decimals)
                )

        self.index = ai.index
        self.total_balance = int(ai.amount)
        self.locked_balance = int(ai.schedule.total) if ai.schedule else 0
        self.unlocked_balance = self.total_balance - self.locked_balance
        self.nonce = ai.sequence_number
        if ai.credentials["0"].initial:
            self.credential_creation_date = f"{ai.credentials['0'].initial.policy.created_at.year}{ai.credentials['0'].initial.policy.created_at.month}"
            self.credential_valid_to_date = f"{ai.credentials['0'].initial.policy.valid_to.year}{ai.credentials['0'].initial.policy.valid_to.month}"
            self.ip_identity = ai.credentials["0"].initial.ip_id
        else:
            assert ai.credentials["0"].normal is not None
            self.credential_creation_date = f"{ai.credentials['0'].normal.policy.created_at.year}{ai.credentials['0'].normal.policy.created_at.month}"
            self.credential_valid_to_date = f"{ai.credentials['0'].normal.policy.valid_to.year}{ai.credentials['0'].normal.policy.valid_to.month}"
            self.ip_identity = ai.credentials["0"].normal.ip_id

        self.credential_count = len(ai.credentials.keys())

        if ai.stake:
            if ai.stake.baker:
                self.baker = True
                self.baker_id = str(ai.stake.baker.baker_info.baker_id)
                self.staked_amount = int(ai.stake.baker.staked_amount)
                self.restake_earnings = ai.stake.baker.restake_earnings
                self.pool_status = ai.stake.baker.pool_info.open_status
                self.pool_metadata_url = ai.stake.baker.pool_info.url
                self.pool_transaction_commission = (
                    ai.stake.baker.pool_info.commission_rates.transaction
                )
                self.pool_finalization_commission = (
                    ai.stake.baker.pool_info.commission_rates.finalization
                )
                self.pool_baking_commission = ai.stake.baker.pool_info.commission_rates.baking
                self.is_suspended = ai.stake.baker.is_suspended

            else:
                self.baker = False

            if ai.stake.delegator:
                self.delegator = True
                self.staked_amount = int(ai.stake.delegator.staked_amount)
                self.restake_earnings = ai.stake.delegator.restake_earnings
                if ai.stake.delegator.target.passive_delegation:
                    self.delegation_target = "passiveDelegation"
                else:
                    self.delegation_target = ai.stake.delegator.target.baker

            else:
                self.delegator = False
        else:
            self.baker = None
            self.delegator = None


class Daily:
    def __init__(
        self,
        date: str,
        block_hash: str,
        block_height: int,
        grpcclient: GRPCClient,
        repo: Repo,
        new_dir: str,
        mongodb: MongoDB,
        tooter: Tooter,
    ):
        self.date = date
        self.new_dir = new_dir
        self.block_hash = block_hash
        self.block_height = block_height
        self.grpcclient: GRPCClient = grpcclient
        self.repo = repo
        self.mongodb = mongodb
        self.tooter = tooter
        # self.get_accounts_for_day()
        self.get_plts()
        self.get_accounts_for_day_from_blocks_collection()
        self.retrieve_account_info_for_day_on_server()
        self.save_accounts_for_day()
        self.git_push()

    def get_plts(self):
        self.plts = self.grpcclient.get_token_list(self.block_hash, net=NET.MAINNET)

    def get_accounts_for_day_from_blocks_collection(self):
        console.log(self.date, "Get accounts from blocks collection...")
        pipeline = [
            {
                "$match": {
                    "account_creation": {"$exists": True},
                    "block_info.height": {"$lte": self.block_height},
                }
            },
            {"$project": {"_id": 0, "address": "$account_creation.address"}},
        ]

        result = [
            x["address"] for x in self.mongodb.mainnet[Collections.transactions].aggregate(pipeline)
        ]
        # as genesis accounts do not have an account_creation transaction,
        # we need to add them manually.
        genesis_accounts = [
            "3XSLuJcXg6xEua6iBPnWacc3iWh93yEDMCqX8FbE3RDSbEnT9P",
            "32jTsKCtpGKr56WLweiPJB6jvLoCgFAHvfXtexHeoovJWu2PBD",
            "49SJ6R6T9zo1C5cLVyxbwAuZC3EcDB9a78vSQYm3ZLA2y2eojM",
            "3eUA4NnWufEqTBXR2QtTwjPxHZRGZvoqHaVjybmzZSqbuG32vJ",
            "38T2PSXK6JVqNmoGqzjtnYsyb76uuNYKSpEoA8vPdtmRmpZhAv",
            "4LH62AZmugKXFA2xXZhpoNbt2fFhAn8182kdHgxCu8cyiZGo2c",
            "47xTHwtFra1d4Mq4DYZuZYJEYrDXY34C4CGkTTzT6eiwjEczuT",
            "4CqVcmNi9F5V53YdJZ9U5sLaqaWt7Uxrf8VYk5WCLDYwbLL62Y",
            "4d13WVDNKVGDUxRUb1PRQAJyTwWSVcjWS7uwZ1oqmDm5icQEPT",
            "3CbvrNVpcHpL7tyT2mhXxQwNWHiPNYEJRgp3CMgEcMyXivms6B",
            "4MPJybKC9Kz7kw9KNyLHhuAEt4ZTxLsd3DBDbxtKdUiv4fXqVN",
            "3ofwYFAkgV59BsHqzmiWyRmmKRB5ZzrPfbmx5nup24cE53jNX5",
            "44bxoGippBqpgseaiYPFnYgi5J5q58bQKfpQFeGbY9DHmDPD78",
            "3EctbG8WaQkTqZb1NTJPAFnqmuhvW62pQbywvqb9VeyqaFZdzN",
        ]
        result.extend(genesis_accounts)
        self.accounts = result

    def get_accounts_for_day(self):
        console.log(self.date, "Get accounts...")
        self.accounts = self.grpcclient.get_account_list(self.block_hash)

    def read_downloaded_accounts_from_disk(self):
        try:
            f = open(f"{self.block_hash}.pickle", "rb")
            pickle_contents = pickle.load(f)
            self.processed_accounts = pickle_contents
            f.close()
        except Exception as _:
            console.log("No pickle found for this day.")
            self.processed_accounts = {}

    def retrieve_account_info_for_day_on_server(self):
        console.log(self.date, f"Retrieve account info at {self.block_hash}...")
        console.log(
            self.date,
            f"{dt.datetime.now()}: Start of loop through all {len(self.accounts)} accounts...",
        )
        self.read_downloaded_accounts_from_disk()
        if self.processed_accounts == {}:
            results = []
            plt_results = []
            ai_stable_results = []
            for index, account in enumerate(self.accounts):
                if (index % 10_000) == 0:
                    console.log(f"Working on account_index {index:,.0f}...")
                result, plt_result, ai_stable_result = self._perform_account_action(account)
                results.append(result)
                ai_stable_results.append(ai_stable_result)
                if plt_result:
                    plt_results.append(plt_result)

            self.processed_accounts = results
            self.plt_results = plt_results
            self.ai_stable_results = ai_stable_results

    def _perform_account_action(self, account):
        # after this step, acc holds the accountInfo for this account at the relevant block.
        acc = Account(account, self.block_hash, self.grpcclient)

        dd = {}
        dd["account"] = acc.account
        dd["nonce"] = acc.nonce
        dd["index"] = acc.index

        dd["credential_creation_date"] = dt.date(
            int(acc.credential_creation_date[:4]),
            int(acc.credential_creation_date[4:]),
            1,
        )
        dd["credential_valid_to_date"] = dt.date(
            int(acc.credential_valid_to_date[:4]),
            int(acc.credential_valid_to_date[4:]),
            1,
        )
        dd["credential_count"] = acc.credential_count
        dd["total_balance"] = acc.total_balance / 1_000_000
        dd["unlocked_balance"] = acc.unlocked_balance / 1_000_000
        dd["locked_balance"] = acc.locked_balance / 1_000_000

        dd["baker_id"] = acc.baker_id if acc.baker else None
        dd["staked_amount"] = acc.staked_amount / 1_000_000 if acc.baker else None
        dd["restake_earnings"] = acc.restake_earnings if acc.baker else None

        dd["pool_status"] = acc.pool_status if acc.baker else None
        dd["pool_metadata_url"] = acc.pool_metadata_url if acc.baker else None
        dd["pool_transaction_commission"] = acc.pool_transaction_commission if acc.baker else None
        dd["pool_finalization_commission"] = acc.pool_finalization_commission if acc.baker else None
        dd["pool_baking_commission"] = acc.pool_baking_commission if acc.baker else None
        dd["is_suspended"] = acc.is_suspended if acc.baker else None

        if not (acc.baker):
            dd["staked_amount"] = acc.staked_amount / 1_000_000 if acc.delegator else None
            dd["restake_earnings"] = acc.restake_earnings if acc.delegator else None
            dd["delegation_target"] = acc.delegation_target if acc.delegator else None

        dd["ip_identity_credential_0"] = acc.ip_identity

        return dd, acc.tokens, acc.ai_stable

    def save_downloaded_accounts_to_disk(self):
        try:
            f = open(f"{self.block_hash}.pickle", "wb")
            save_object = self.processed_accounts
            pickle.dump(save_object, f)
            f.close()
        except Exception as e:
            print(e)

    def send_queue_to_mongodb_stable_address_info(self, queue: list[dict]):
        try:
            if len(queue) > 0:
                self.mongodb.mainnet[Collections.stable_address_info].bulk_write(
                    [
                        ReplaceOne(
                            {"_id": rec["_id"]},
                            rec,
                            upsert=True,
                        )
                        for rec in queue
                    ]
                )
        except Exception as e:
            print(e)

    def save_accounts_for_day(self):
        """
        This is the final step from the account retrieval and enrichment process,
        storing the results on disk in 'accounts.csv'.
        """
        console.log(self.date, "Save accounts...")
        df = pd.DataFrame(self.processed_accounts)
        self.df_accounts = df

        df.to_csv(f"{self.new_dir}/accounts.csv", index=False)

        console.log(self.date, "Save PLT...")
        df_plt = pd.DataFrame(self.plt_results).fillna(0.0)
        self.df_plt = df_plt

        df_plt.to_csv(f"{self.new_dir}/plts.csv", index=False)

        # now also save to Mongodb...
        df["_id"] = df["account"]
        df["credential_creation_date"] = pd.to_datetime(df["credential_creation_date"])
        df["credential_valid_to_date"] = pd.to_datetime(df["credential_valid_to_date"])
        # all_records = df.to_dict('records')
        all_records = [
            {k: v for k, v in m.items() if pd.notnull(v)} for m in df.to_dict(orient="records")
        ]

        try:
            # self.mongodb.mainnet[Collections.nightly_accounts].delete_many(
            #     {"account": {"$exists": True}}
            # )
            # self.mongodb.mainnet[Collections.nightly_accounts].insert_many(all_records)

            queue = []
            for account in self.ai_stable_results:
                record = account.to_collection()
                queue.append(record)
                if len(queue) >= 1000:
                    self.send_queue_to_mongodb_stable_address_info(queue)
                    queue = []
            if len(queue) > 0:
                self.send_queue_to_mongodb_stable_address_info(queue)

            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"Accounts Retrieval: Stable addresses saved to MongoDB for {self.date}.",
                notifier_type=TooterType.INFO,
            )

        except Exception as e:
            print(e)

    def git_push(self):
        console.log(self.date, "Git add, commit, push...")
        remote = "origin"

        try:
            self.repo.git.add("-A")
            self.repo.index.commit(self.date)
            origin = self.repo.remote(name=remote)

            origin.push()
            query = {"_id": "last_known_nightly_accounts"}
            self.mongodb.mainnet[Collections.helpers].replace_one(
                query, {"date": self.date}, upsert=True
            )
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"Accounts Retrieval: Stable addresses pushed and helper updated for {self.date}.",
                notifier_type=TooterType.INFO,
            )
        except Exception as e:
            print(f"Some error occured while pushing the code: {e}")
