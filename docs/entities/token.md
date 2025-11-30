Tokens on the Concordium blockchain follow the [CIS-2 specification](https://proposals.concordium.com/CIS/cis-2.html) and [CIS-7 specification](https://proposals.concordium.com/CIS/cis-7.html) for PLTs
.
CCDExplorer tracks three categories:

- **Fungible tokens** – CIS-2 tokens with divisible supply (e.g. EUROe).
- **Non-fungible tokens** – ERC-721 style CIS-2 tokens where each `token_id` is unique.
- **Protocol-level tokens (PLTs)** – CIS-7 system-issued tokens where supply changes are encoded in protocol events.

All CIS-2 token activity is sourced from logged events emitted by `contract_initialized` or `contract_update_issued` transactions. CIS-7 (PLT) token activity is tracked through `account_transaction.token_update` transaction type.

# CIS-2
## Token address format
- `<contract_index,contract_subindex>-<token_id>`
- Stored as `_id` in the `tokens_token_addresses_v2` collection via [`MongoTypeTokenAddress`](../components/domain.md).
- Always accompanied by canonical account references to ensure aliases roll up correctly (see [Alias Addresses](alias.md)).

## Initialization flow
1. [`ms_events_and_impacts`](../projects/every_block/events_and_impacted.md) parses every transaction and normalizes logged events plus impacted addresses.
2. [`ms_token_accounting`](../projects/every_block/token_accounting.md) groups those events per token address and creates/updates documents in:
   - `tokens_token_addresses_v2` – aggregate supply, metadata URL, verification info, last processed block height.
   - `tokens_links_v3` – holdings per account/contract/public key (with canonical IDs).
3. [`ms_metadata`](../projects/every_block/metadata.md) enqueues newly discovered metadata URLs for retrieval.

Token addresses are created lazily: the first observed transfer/mint/burn event for a `(contract, token_id)` pair results in a new document.

## Verification tiers
- **Verified Fungible** / **Verified Non-Fungible** – token addresses listed in `tokens_tags`, curated by CCDExplorer operators.  
  These often include issuer metadata, price feeds, and icons.
- **Unverified** – any active token not yet tagged. Still fully tracked, but presented with an “unverified” badge in the Explorer.

Verification feeds into the Explorer API via `/tokens/fungible` and `/tokens/non-fungible` endpoints, allowing clients to filter by trust level.

## Metadata retrieval
- CIS-2 metadata lives off-chain (typically JSON over HTTPS).
- [`ms_metadata`](../projects/every_block/metadata.md) fetches each URL, validates the payload, and writes it into the `token_metadata` field.
- Failed fetches use exponential backoff, and the [`dagster_recurring`](../projects/dagster_recurring.md) jobs can force a retry when an issuer updates their metadata.

# CIS-7 Protocol-level tokens (PLTs)
Transactions can emit PLT events (e.g. release amounts). [`ms_plt`](../projects/every_block/plt.md) processes those events and
feeds dashboards plus on-chain analytics. PLTs share the same canonical storage (token addresses + links) so APIs treat them uniformly.

## Downstream usage
- Explorer account pages leverage `tokens_links_v3` to show balances on the Fungible/NFT tabs.
- For PLTs we maintain `plts_tags` and `plts_links`.
- Notification services watch token events to alert users about transfers impacting their canonical address.
- Dagster analytics jobs (realized prices, holders, transaction fees) query the token collections to track supply, holder counts, and verified issuer stats.

## Example documents
See [`ms_token_accounting`](../projects/every_block/token_accounting.md#examples) for sample MongoDB documents representing token addresses and account links.***
