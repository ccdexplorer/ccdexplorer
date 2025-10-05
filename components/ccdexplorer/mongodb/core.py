from enum import Enum
from typing import Dict

from pymongo import AsyncMongoClient, MongoClient, ReadPreference
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.collection import Collection
from rich.console import Console

from ccdexplorer.env import MONGO_URI
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType

console = Console()


class Collections(Enum):
    """
    Enum class representing various MongoDB collection names used in the application.

    Attributes:
        all_account_addresses: Collection for all account addresses.
        blocks: Collection for blocks.
        blocks_log: Collection for blocks log.
        transactions: Collection for transactions.
        special_events: Collection for special events.
        instances: Collection for instances.
        modules: Collection for modules.
        paydays: Collection for paydays.
        paydays_performance: Collection for paydays performance.
        paydays_rewards: Collection for paydays rewards.
        paydays_apy_intermediate: Collection for paydays APY intermediate.
        paydays_current_payday: Collection for current payday.
        paydays_helpers: Collection for payday helpers.
        involved_accounts_transfer: Collection for involved accounts transfer.
        nightly_accounts: Collection for nightly accounts.
        blocks_at_end_of_day: Collection for blocks at end of day.
        blocks_per_day: Collection for blocks per day.
        helpers: Collection for helpers.
        memo_transaction_hashes: Collection for memo transaction hashes.
        cns_domains: Collection for CNS domains.
        dashboard_nodes: Collection for dashboard nodes.
        tokens_accounts: Collection for token accounts.
        tokens_links_v2: Collection for token links version 2.
        tokens_links_v3: Collection for token links version 3.
        tokens_token_addresses_v2: Collection for token addresses version 2.
        tokens_tags: Collection for token tags.
        tokens_logged_events: Collection for token logged events.
        tokens_logged_events_v2: Collection for token logged events version 2.
        tokens_token_addresses: Collection for token addresses.
        memos_to_hashes: Collection for memos to hashes.
        credentials_issuers: Collection for credential issuers.
        impacted_addresses: Collection for impacted addresses.
        impacted_addresses_pre_payday: Collection for impacted addresses pre-payday.
        impacted_addresses_all_top_list: Collection for all top list of impacted addresses.
        pre_tokens_overview: Collection for pre-rendered tokens overview.
        pre_addresses_by_contract_count: Collection for pre-rendered addresses by contract count.
        pre_tokens_by_address: Collection for pre-rendered tokens by address.
        statistics: Collection for statistics.
        pre_render: Collection for pre-rendered data.
        cis5_public_keys_contracts: Collection for CIS5 public keys contracts.
        cis5_public_keys_info: Collection for CIS5 public keys info.
        projects: Collection for projects.
        usecases: Collection for use cases.
        tokens_impacted_addresses: Collection for tokens impacted addresses.
        tnt_logged_events: Collection for TNT logged events.
        queue_todo: Collection for queue to-do items.
        tx_types_count: Collection for transaction types count.
    """

    all_account_addresses = "all_account_addresses"
    blocks = "blocks"
    blocks_log = "blocks_log"
    transactions = "transactions"
    special_events = "special_events"
    instances = "instances"
    modules = "modules"
    paydays = "paydays"
    paydays_performance = "paydays_performance"
    paydays_rewards = "paydays_rewards"
    paydays_apy_intermediate = "paydays_apy_intermediate"
    paydays_current_payday = "paydays_current_payday"
    paydays_v2 = "paydays_v2"
    paydays_v2_performance = "paydays_v2_performance"
    paydays_v2_rewards = "paydays_v2_rewards"
    paydays_v2_apy = "paydays_v2_apy"
    paydays_v2_current_payday = "paydays_v2_current_payday"
    paydays_helpers = "paydays_helpers"
    involved_accounts_transfer = "involved_accounts_transfer"
    nightly_accounts = "nightly_accounts"
    blocks_at_end_of_day = "blocks_at_end_of_day"
    blocks_per_day = "blocks_per_day"
    helpers = "helpers"
    memo_transaction_hashes = "memo_transaction_hashes"
    cns_domains = "cns_domains"
    dashboard_nodes = "dashboard_nodes"
    tokens_accounts = "tokens_accounts"
    tokens_links_v2 = "tokens_links_v2"
    tokens_links_v3 = "tokens_links_v3"
    tokens_token_addresses_v2 = "tokens_token_addresses_v2"
    tokens_tags = "tokens_tags"
    tokens_logged_events = "tokens_logged_events"
    tokens_logged_events_v2 = "tokens_logged_events_v2"
    tokens_token_addresses = "tokens_token_addresses"
    memos_to_hashes = "memos_to_hashes"
    credentials_issuers = "credentials_issuers"
    impacted_addresses = "impacted_addresses"
    impacted_addresses_pre_payday = "impacted_addresses_pre_payday"
    impacted_addresses_all_top_list = "impacted_addresses_all_top_list"
    # statistics and pre-renders
    pre_tokens_overview = "pre_tokens_overview"
    pre_addresses_by_contract_count = "pre_addresses_by_contract_count"
    pre_tokens_by_address = "pre_tokens_by_address"
    statistics = "statistics"
    pre_render = "pre_render"
    cis5_public_keys_contracts = "cis5_public_keys_contracts"
    cis5_public_keys_info = "cis5_public_keys_info"
    tx_types_count = "tx_types_count"
    # addresses and contracts per net per usecase
    projects = "projects"
    usecases = "usecases"
    tokens_impacted_addresses = "tokens_impacted_addresses"
    tnt_logged_events = "tnt_logged_events"
    queue_todo = "queue_todo"
    validator_logs = "validator_logs"
    blocks_with_only_chain_txs = "blocks_with_only_chain_txs"
    # PLT
    plts_links = "plts_links"
    plts_tags = "plts_tags"
    celery_taskmeta = "celery_taskmeta"


class CollectionsUtilities(Enum):
    """
    Enum class representing various MongoDB collection names used in the application.

    Attributes:
        labeled_accounts (str): Collection name for labeled accounts.
        labeled_accounts_metadata (str): Collection name for labeled accounts metadata.
        exchange_rates (str): Collection name for exchange rates.
        exchange_rates_historical (str): Collection name for historical exchange rates.
        users_v2_prod (str): Collection name for production users (version 2).
        users_v2_dev (str): Collection name for development users (version 2).
        message_log (str): Collection name for message logs.
        preferences_explanations (str): Collection name for preferences explanations.
        release_notes (str): Collection name for release notes.
        token_api_translations (str): Collection name for token API translations.
        projects (str): Collection name for projects (use case management).
        usecases (str): Collection name for use cases (use case management).
        helpers (str): Collection name for helpers (use case management).
        api_api_keys (str): Collection name for API keys.
        api_users (str): Collection name for API users.
    """

    labeled_accounts = "labeled_accounts"
    labeled_accounts_metadata = "labeled_accounts_metadata"
    exchange_rates = "exchange_rates"
    exchange_rates_historical = "exchange_rates_historical"
    users_v2_prod = "users_v2_prod"
    users_v2_dev = "users_v2_dev"
    message_log = "message_log"
    preferences_explanations = "preferences_explanations"
    release_notes = "release_notes"
    token_api_translations = "token_api_translations"
    # use case management
    projects = "projects"
    usecases = "usecases"
    helpers = "helpers"
    # api
    api_api_keys = "api_api_keys"
    # api
    api_users = "api_users"


class MongoDB:
    def __init__(self, tooter: Tooter, nearest: bool = False):
        self.tooter: Tooter = tooter
        try:
            if nearest:
                con = MongoClient(MONGO_URI, read_preference=ReadPreference.NEAREST)
            else:
                con = MongoClient(MONGO_URI)
            self.connection: MongoClient = con

            self.mainnet_db = con["concordium_mainnet"]
            self.mainnet: Dict[Collections, Collection] = {}
            for collection in Collections:
                self.mainnet[collection] = self.mainnet_db[collection.value]

            self.testnet_db = con["concordium_testnet"]
            self.testnet: Dict[Collections, Collection] = {}
            for collection in Collections:
                self.testnet[collection] = self.testnet_db[collection.value]

            self.devnet_db = con["concordium_devnet"]
            self.devnet: Dict[Collections, Collection] = {}
            for collection in Collections:
                self.devnet[collection] = self.devnet_db[collection.value]

            self.utilities_db = con["concordium_utilities"]
            self.utilities: Dict[CollectionsUtilities, Collection] = {}
            for collection in CollectionsUtilities:
                self.utilities[collection] = self.utilities_db[collection.value]

            console.log(con.server_info()["version"])
        except Exception as e:
            print(e)
            tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"BOT ERROR! Cannot connect to MongoDB, with error: {e}",
                notifier_type=TooterType.MONGODB_ERROR,
            )


class MongoMotor:
    def __init__(self, tooter: Tooter, nearest: bool = False):
        self.tooter: Tooter = tooter
        try:
            if nearest:
                con = AsyncMongoClient(MONGO_URI, read_preference=ReadPreference.NEAREST)
            else:
                con = AsyncMongoClient(MONGO_URI)
            self.connection = con

            self.mainnet_db = con["concordium_mainnet"]
            self.mainnet: Dict[Collections, AsyncCollection] = {}
            for collection in Collections:
                self.mainnet[collection] = self.mainnet_db[collection.value]

            self.testnet_db = con["concordium_testnet"]
            self.testnet: Dict[Collections, AsyncCollection] = {}
            for collection in Collections:
                self.testnet[collection] = self.testnet_db[collection.value]

            self.utilities_db: AsyncDatabase = con["concordium_utilities"]
            self.utilities: Dict[CollectionsUtilities, AsyncCollection] = {}
            for collection in CollectionsUtilities:
                self.utilities[collection] = self.utilities_db[collection.value]
            # console.log(f'Motor: {con.server_info()["version"]}')
        except Exception as e:
            print(e)
            tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"BOT ERROR! Cannot connect to Motor MongoDB, with error: {e}",
                notifier_type=TooterType.MONGODB_ERROR,
            )


def build_collection_identifier(namespace: str, collection: Collections):
    return f"{namespace}.{collection.value}"
