# Contract (Instance)

On Concordium, a *contract* refers to a deployed instance of a module. Each instance has its own address `<index,subindex>`
and can maintain state independent of other deployments of the same module.

## Representation
- gRPC: [`CCD_ContractAddress`](../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_ContractAddress) and
  [`CCD_InstanceInfo`](../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_InstanceInfo).
- MongoDB: [`MongoTypeInstance`](../components/domain.md) documents stored in the `instances` collection.
- Canonical ID: `<index,subindex>` string that is also used when composing token addresses (`<index,subindex>-token_id`).

Each instance records:
- Source module reference (with versioning between `v0`/`v1` structures).
- Named entrypoints (`receive` functions) and current state hash.
- Optional links to recognized projects (set by [`ms_instances`](../projects/every_block/new_contract.md)).

## Lifecycle
1. A user submits an account transaction with effect `contract_initialized`, referencing a module hash.
2. The `ms_block_analyser` service detects the effect and publishes a `contract` task.
3. [`ms_instances`](../projects/every_block/new_contract.md) consumes the task, resolves full instance info via gRPC, persists it to MongoDB, and tries to associate it with a recognized project/module.
4. Subsequent `contract_update_issued` transactions trigger upgrades; `ms_instances` records the new module linkage and emits notifications.

## Where contracts are used
- The Explorer UI links instances to their parent modules and displays their receive methods.
- Token accounting (`ms_token_accounting`) uses contract addresses to derive CIS-2 token addresses and track holders.
- Alias-aware lookups collapse contract references via the canonical `<index,subindex>` format, aligning with how we process account aliases.***
