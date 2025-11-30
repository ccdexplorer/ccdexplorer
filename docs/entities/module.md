Modules are the starting point for all `contracts` and `tokens` on the blockchain. Modules can be deployed through an 
[Account Transaction](transaction.md#account-transaction) with effect `module_deployed`. Each module bundles the Wasm bytecode plus metadata describing its entrypoints.

Modules are stored in the collection `modules` in the db and have the following class:


::: ccdexplorer.domain.mongo.MongoTypeModule
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 




## How is a module stored in the system?
1. [`ms_block_analyser`](../projects/every_block/block_analyzer.md) notices a `module_deployed` effect and publishes a `module_deployed` task.
2. The [New Module Service](../projects/every_block/new_module.md) (`ms_modules`) consumes the task, fetches the Wasm bytes via gRPC, parses the export section (using [`wasm_decoder`](../components/wasmdecoder.md)), and writes a `MongoTypeModule` document into the `modules` collection.
3. If the module exposes verification metadata, the service invokes the [`concordium_client`](../components/concordium-client.md) CLI to run `cargo concordium verify-build`. Results are stored alongside the module and surfaced in the Explorer UI.
4. Downstream services map modules to recognized projects; [`ms_instances`](../projects/every_block/new_contract.md) uses this link whenever a new contract is initialized.***
