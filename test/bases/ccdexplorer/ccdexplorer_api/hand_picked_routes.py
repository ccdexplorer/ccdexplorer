# tests/route_samples.py

# Values used to fill in {path} params automatically
SAMPLE_PATH_VALUES = {
    "net": "mainnet",  # or "testnet"
    "account_address": "4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp",
    "account_id": "4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp",
    "tx_hash": "e45cd77c71275c5d2c1e2a7aeacc6fd75870e2729511c6455dbd92a34aa976b5",
    "skip": "0",
    "limit": "10",
    "index_or_hash": 72723,
    "block": "170eae39728a75cdf1eeff82e3d00e8ca48e46ff2b8b5b06cd5b0758baef9330",
    "block_hash": "170eae39728a75cdf1eeff82e3d00e8ca48e46ff2b8b5b06cd5b0758baef9330",
    "index": 72723,
    "contract_index": 9882,
    "contract_subindex": 0,
    "token_id": "00001739",
    "height_or_hash": 30_000_000,
    "direction": "asc",
    "sort_key": "height",
    "start_date": "2025-10-01",
    "end_date": "2025-10-05",
    "since_index": 103_000,
    "since": 37_000_000,
    "wallet_contract_index": 9645,
    "wallet_contract_subindex": 0,
    "public_key": "844ad4197a47afec6481d41472c49336209d8b3d762efd2b3e88c2587c60c1a7",
    "value": "search_me",
    "module_ref": "8e31feffb4502800993e9efafa046e7c1244494dcc299eebd5e6d814a0d9d55f",
    "period": "h1",
    "genesis_index": 8,
    "epoch": 16,
    "project_id": "aesirx",
    "node_id": "8175412dde32cfab",
    "analysis": "statistics_plt",
    "date": "2025-10-01",
    "year": 2025,
    "month": 10,
    "day": 5,
    "cis_standard": "CIS-2",
    # add more as needed
}

# Optional query params (by route name or path)
SAMPLE_QUERY_PARAMS = {
    "/api/v1/tokens": {"limit": 5},
    "/api/v1/blocks": {"page": 1},
    # route names or paths as keys are fine
}

# Optional minimal JSON bodies for POST/PUT/PATCH routes
SAMPLE_JSON_BODIES = {
    "/auth/login": {"email": "test@example.com", "password": "secret"},
    "/api/v1/user": {"name": "Tester"},
}
