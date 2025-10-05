The heartbeat service is the `hearbeat` of CCDExplorer Universe. It watches for new finalized blocks and parses them. 

## PubSub (MQTT)
Many parts of the CCE Explorer Universe work together through a pub/sub service. We have chosen MQTT for this. 
The `heartbeat` service sends out various messages, depending on the type of transaction, block, account, etc. Other services listen to their specific messages and 
act on them. 

The following messages are published when they occur, either through a block or a transaction, or other event. 

| Type|Message|
|------|---------|
|New block|`heartbeat/block/new`|
|New address|`heartbeat/address/new`|
|New payday|`heartbeat/payday/new`|
|New update tx|`heartbeat/update/new`|
|Failed tx|`heartbeat/tx/failed`|
|New instance|`heartbeat/instance/new`|
|Upgraded instance|`heartbeat/instance/upgraded`|
|New module|`heartbeat/module/new`|


On a schedule, the following messages are published:

| Schedule|Message|
|------|---------|
|Every 60 min|`services/cleanup`|
|Every 5 min|`currency/refresh`|

