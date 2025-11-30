# Concordium Client Wrapper (`components/ccdexplorer/concordium_client`)

This component provides a resilient wrapper around the official `concordium-client` CLI.  
While most services talk to the chain via gRPC, module verification and diagnostics still require CLI commands;
the wrapper standardizes how we invoke them.

## Features
- Maintains a list of preferred nodes (`GRPC_MAINNET`, `GRPC_TESTNET`) and automatically retries against the next node if one fails.
- Exposes `Requestor` helpers for commands such as `GetBlockInfo`, `module show`, and `verify-build`, handling stdout parsing and
  transient failure retries.
- Tracks node health (`check_nodes_with_heights`) so services like `ms_modules` can verify they operate on up-to-date peers before
  running an expensive verification.
- Respects the `CONCORDIUM_CLIENT_PREFIX` env var, enabling deployments where the CLI binary lives in a non-standard path.

## Example
```python
from ccdexplorer.concordium_client import Requestor, RequestorType
from ccdexplorer.domain.generic import NET

req = Requestor(
    args=["module", "show", "fb2f...modref", "--out", "tmp/module.wasm"],
    net=NET.MAINNET,
)
stdout = req.result.stdout.decode()
```

The wrapper is heavily used by [`ms_modules`](../projects/every_block/new_module.md) to fetch module sources and verify builds,
and by diagnostic scripts that need CLI-only endpoints.***
