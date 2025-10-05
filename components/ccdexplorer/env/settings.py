import os
import ast
from dotenv import load_dotenv


load_dotenv()


BRANCH = os.environ.get("BRANCH", "dev")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")
NOTIFIER_API_TOKEN = os.environ.get("NOTIFIER_API_TOKEN")
SITE_URL = os.environ.get("SITE_URL")
API_TOKEN = os.environ.get("API_TOKEN", "api_token")
FASTMAIL_TOKEN = os.environ.get("FASTMAIL_TOKEN")
CCDEXPLORER_API_KEY = os.environ.get("CCDEXPLORER_API_KEY")
COIN_MARKET_CAP_API_KEY = os.environ.get("COIN_MARKET_CAP_API_KEY")
FALLBACK_URI = os.environ.get("FALLBACK_URI")
MONGO_URI = os.environ.get("MONGO_URI")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")
MAILTO_LINK = os.environ.get("MAILTO_LINK")
MAILTO_USER = os.environ.get("MAILTO_USER")
API_NET = os.environ.get("API_NET", "mainnet")
API_URL = os.environ.get("API_URL")
LOGIN_SECRET = os.environ.get("LOGIN_SECRET")
API_KEY_HEADER = "x-ccdexplorer-key"
API_ACCOUNT_TESTNET = "4NkwL9zPsZF6Y8VDztVtBv38fmgoY8GneDsGZ6zRpTZJgyX29E"
API_ACCOUNT_MAINNET = "3GjqwYXv5sGY1QZdhx3uBdNz1LWUofQAn4tyV6wQu8cg9592Ur"
if not os.environ.get("GRPC_MAINNET"):
    GRPC_MAINNET = []
else:
    GRPC_MAINNET = ast.literal_eval(os.environ["GRPC_MAINNET"])

if not os.environ.get("GRPC_TESTNET"):
    GRPC_TESTNET = []
else:
    GRPC_TESTNET = ast.literal_eval(os.environ["GRPC_TESTNET"])
RUN_ON_NET = os.environ.get("RUN_ON_NET", "mainnet")
RUN_LOCAL_STR = os.environ.get("RUN_LOCAL_STR", "local")
REDIS_URL = os.environ.get("REDIS_URL")
SENTRY_DSN = os.environ.get("SENTRY_DSN")
SENTRY_ENVIRONMENT = os.environ.get("SENTRY_ENVIRONMENT")
HEARTBEAT_PROGRESS_DOCUMENT_ID = os.environ.get(
    "HEARTBEAT_PROGRESS_DOCUMENT_ID", "heartbeat_last_processed_block"
)
HEARTBEAT_SPECIAL_PURPOSE = os.environ.get(
    "HEARTBEAT_SPECIAL_PURPOSE", "special_purpose_block_request"
)
MAX_BLOCKS_PER_RUN = int(os.environ.get("MAX_BLOCKS_PER_RUN", 100))
DEBUG = False if os.environ.get("DEBUG", False) == "False" else True
BLOCK_COUNT_SPECIALS_CHECK = int(os.environ.get("BLOCK_COUNT_SPECIALS_CHECK", 2000))
TX_REQUEST_LIMIT_DISPLAY = int(os.environ.get("TX_REQUEST_LIMIT_DISPLAY", 4999))
COIN_API_KEY = os.environ.get("COIN_API_KEY")
REPO_DIR = os.environ.get("REPO_DIR", "/Users/sander/Developer/open_source/ccdexplorer-accounts")
ON_SERVER = os.environ.get("ON_SERVER", False)

environment = {
    "SITE_URL": SITE_URL,
    "CCDEXPLORER_API_KEY": CCDEXPLORER_API_KEY,
    "API_ACCOUNT_TESTNET": API_ACCOUNT_TESTNET,
    "API_ACCOUNT_MAINNET": API_ACCOUNT_MAINNET,
    "API_NET": API_NET,
    "API_URL": API_URL,
    "SENTRY_ENVIRONMENT": SENTRY_ENVIRONMENT,
    "REDIS_URL": REDIS_URL,
    "SENTRY_DSN": SENTRY_DSN,
    "TX_REQUEST_LIMIT_DISPLAY": TX_REQUEST_LIMIT_DISPLAY,
    "NET": "mainnet",
}
