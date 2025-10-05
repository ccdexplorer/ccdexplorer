
The micro service MS Accounts listens to the Celery topic `account_creation` and starts processing when a new message arrives. The message contains the `account address`.

## Steps

1. From the message, take the account address and retrieve account info by calling `GetAccountInfo` on the `last_final` block. 
2. Store a new document in the collection `all_account_addresses` containing the `account address`, as well as the `account index`. 

