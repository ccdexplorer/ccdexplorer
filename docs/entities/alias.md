# Alias Addresses

Concordium accounts can expose multiple *alias* addresses. An alias shares the same account index but appends an alias suffix,
allowing wallets to compartmentalize activity without creating a new account.

## Canonical vs alias form
- **Canonical component** – the first 29 characters of an account address; identical across every alias belonging to that account.
- **Alias suffix** – the trailing characters that differentiate alias `0`, `1`, etc.

Example:
```
46Pu3wVfURgihzAXoDxMxWucyFo5irXvaEmacNgeK7i49MKyiD   # canonical + alias suffix
46Pu3wVfURgihzAXoDxMxWucyFo5i                         # canonical portion only
```

## How CCDExplorer uses aliases
- [`ms_accounts`](../projects/every_block/new_address.md) stores every new address twice: once with the full address and once under the canonical `_id` so aliases deduplicate to a single document.
- Collections such as `tokens_links_v3` and `transactions_impacted_addresses` always record `*_canonical` fields (e.g. `account_address_canonical`, `impacted_address_canonical`) so you can query “all activity for this account” regardless of which alias was used on-chain.
- The Explorer API/site expose helpers like `account_address_is_alias` and `/account/{address}/aliases-in-use` to determine whether an address is currently routed through an alias and to show all aliases that have ever been seen.
- When displaying links, the site appends `/alias/{suffix}` so users can distinguish alias-specific URLs while still landing on the primary account page.

