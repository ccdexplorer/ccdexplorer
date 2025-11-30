# Block Analyzer (Project: `ms_block_analyser`)

The Block Analyzer service watches for new blocks with transactions in the `blocks` collection (the result of the `heartbeat` service) and parses them.***


## PubSub (Celery + Redis)
Many parts of the CCE Explorer Universe work together through a pub/sub service, using Celery + Redis.

The `block analyzer` service sends out messages on various topics, depending on the type of transaction(s) found. Other services listen to their specific topics and act on them. 

The following topics are published when they occur in a block.

| Topic|Subscriber|
|------|---------|
|`indexers`|Triggered for every transaction, see [MS Indexers](indexers.md)|
|`events_and_impacted`|Triggered for every transaction, see [MS Events and Impacted](events_and_impacted.md)|
|`account_creation`|Triggered for a new acccount, see [MS Accounts](new_address.md)|
|`contract`|Triggered for a new or upgrade smart contract (also called instance), see [MS Instances](new_contract.md)|
|`module_deployed`|Triggered for a new module deployed, see [MS Modules](new_module.md)|
|`plt`|Triggered for for a new PLT deployed and/or a token update transaction, see [MS PLT](plt.md)|
|`token_accounting`|Triggered for CIS-2 transactions, see [MS Token Accounting](token_accounting.md)|
|`metadata`|Triggered _by `token_accounting`_ for CIS-2 transactions, see [MS Metadata](metadata.md)|


