# MS Instances (Project: `ms_instances`)

The micro service listens to the Celery topic `contract` and starts processing when a new message arrives. The message contains the `block_height`.

## Steps
1. Retrieve all transactions for the given block. 
2. If the transaction effect is `contract_initialized`, then process a new contract. On `mainnet` we will try to see if the module is linked to a recognized project and add the new contract to the `projects` collection.
2. If the transaction effect is `contract_update_issued` with additional effect `upgraded`, then process a contract upgrade. This moves a contract to a new module.  
