"""GRPC client package.

Avoid eager imports to prevent circular dependencies when submodules are imported.
"""

__all__ = ["GRPCClient"]


def __getattr__(name: str):
    if name == "GRPCClient":
        from .core import GRPCClient

        return GRPCClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
