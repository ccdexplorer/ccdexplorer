from importlib import import_module as _import_module

from ._generated_imports import ensure_grpc_gen_on_path

ensure_grpc_gen_on_path()
_mod = _import_module("kernel_pb2")
globals().update(_mod.__dict__)
