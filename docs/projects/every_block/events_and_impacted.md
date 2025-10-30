## Goal
The goal of this service is to extract relevant information from transactions for every block. Information includes logged events and impacted addresses from these and from the transactions itself. 

## Data used
As source we use blocks from the `blocks` collection and transactions from the `transactions` collection, as filled by the [Heartbeat](heartbeat.md) service.

For every transaction, we find which addresses (account, contract or public key) are **impacted** by this transaction and add this to the collection. 
