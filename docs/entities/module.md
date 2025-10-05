Modules 

Modules are the starting point for all `contracts` and `tokens` on the blockchain. Modules can be deployed through an 
[Account Transaction](../entities/transaction.md/#account-transaction) with effect `module_deployed`.

Modules are stored in the collection `modules` in the db and have the following class:


::: ccdexplorer.domain.mongo.MongoTypeModule
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 




## How is a module stored in the system?
This is performed in the [New Module Service](../services/messages/new_module.md), that works on receiving the appropriate MQTT message. When this service runs, it stores the module data in the corresponding collection, and also starts the module `verification process`. 