Tokens on the Concordium blockchain are [CIS-2 tokens](https://proposals.concordium.com/CIS/cis-2.html).
On CCDExplorer.io, we make a distinction between `fungible` and `non-fungible` tokens. All activity on tokens is taken from `logged events`, the events that `contract_update_issued` or `contract_initialized` transaction types can emit. 

## Initialization
A token will be created in the backend in the collection `token_addresses_v2` if and when needed.
This creation can occur in either a `contract_update_issued` or `contract_initialized` transaction type (CHECK). 

## Verification
Tokens are displayed on the Explorer either as `Verified Fungible Tokens`, `Verfied Non-Fungible Tokens` or `Unverified Tokens`. 
The distinction depends on whether the contract is recognized in the collection `tokens_tags`, where we store information on tokens from recognized issuers. 

## Example


## Metadata
The token metadata is stored off-chain and must be a json file. We try to parse metata urls on a schedule, see ...
When metadata cannot be parsed, we delay the request of the next try exponentially each time. To ask the service to redo a specific token address for metadata processing, use the following api call....
