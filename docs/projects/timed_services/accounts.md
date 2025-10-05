## Goal
The goal of this service ([Accounts Retrieval](https://github.com/ccdexplorer/ccdexplorer-accounts-retrieval)) is to have a daily log of all accounts existing on chain at the end of the day, with a defined set of properties for each account. 

## Accounts
At the last block from the day, the node is queried using `GetAccountsList`. This returns a list of account addresses.

## Properties
The following list of properties is stored for each account:


### Delegator Example
| Property                        | Value (example delegator)|
|---------------------------------|--------------------------------------------|
| account                         | [2wopuh881hkTjGvJwZgSDYidTQwSdTPEQJ5TbMf8dqyGxHi52z](https://ccdexplorer.io/mainnet/account/2wopuh881hkTjGvJwZgSDYidTQwSdTPEQJ5TbMf8dqyGxHi52z) |
| nonce                           | 3                                          |
| index                           | 72581                                      |
| credential_creation_date        | 2022-01-01                                 |
| credential_valid_to_date        | 2027-01-01                                 |
| credential_count                | 1                                          |
| total_balance                   | 3361.718826                                |
| unlocked_balance                | 3361.718826                                |
| locked_balance                  | 0.0                                        |
| baker_id                        |                                            |
| staked_amount                   | 3351.770846                                |
| restake_earnings                | True                                       |
| pool_status                     |                                            |
| pool_metadata_url               |                                            |
| pool_transaction_commission     |                                            |
| pool_finalization_commission    |                                            |
| pool_baking_commission          |                                            |
| delegation_target               | 1916                                       |


### Validator Example
| Property                        | Value (example validator) |
|---------------------------------|--------------------------------------------|
| account                         | [35RR849AEGr2tgwBKRmnDYJW8ZWtvF5hBtVZYWeodqVYbWYH9P](https://ccdexplorer.io/mainnet/account/35RR849AEGr2tgwBKRmnDYJW8ZWtvF5hBtVZYWeodqVYbWYH9P) |
| nonce                           | 89                                         |
| index                           | 84476                                      |
| credential_creation_date        | 2022-02-01                                 |
| credential_valid_to_date        | 2027-02-01                                 |
| credential_count                | 1                                          |
| total_balance                   | 13749202.704514                            |
| unlocked_balance                | 13749202.704514                            |
| locked_balance                  | 0.0                                        |
| baker_id                        | 84476                                      |
| staked_amount                   | 13733625.159497                            |
| restake_earnings                | False                                      |
| pool_status                     | open_for_all                               |
| pool_metadata_url               |                                            |
| pool_transaction_commission     | 0.00999                                    |
| pool_finalization_commission    | 1.0                                        |
| pool_baking_commission          | 0.00999                                    |
| delegation_target               |                                            |


## Where is this data used?
The results are stored in the [Accounts](https://github.com/ccdexplorer/ccdexplorer-accounts) repo. This repo is used to compile a large variety of statistics.