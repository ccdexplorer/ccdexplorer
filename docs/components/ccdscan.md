# CCDScan component

`ccdexplorer.ccdscan` provides a thin wrapper around CCDScan's public GraphQL API so other
bricks can reuse a single throttled client whenever they need to resolve block-level release
statistics. The `CCDScan` class initializes sensible defaults—like the maximum of 20 requests
per node window and a 10-second cooldown between GraphQL calls—then exposes helpers such as
`ql_request_block_for_release()` to fetch the `balanceStatistics` for any Concordium block hash
on mainnet or testnet. Because it injects a `Tooter` instance, callers can share the same
notification/logging utilities that other components use.

## Fetching block release data
```py
from ccdexplorer.ccdscan import CCDScan
from ccdexplorer.tooter import Tooter
from ccdexplorer.domain.generic import NET

ccdscan = CCDScan(Tooter())
data = ccdscan.ql_request_block_for_release(block_hash, net=NET.MAINNET)
```

The helper crafts the `blockByBlockHash` query shown above, posts it against the proper CCDScan
GraphQL endpoint based on the requested network, and returns the decoded JSON `data` payload when
the HTTP response succeeds.

::: ccdexplorer.ccdscan
