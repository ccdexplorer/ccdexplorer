"""Working out which CIS standards a contract instance supports.

Answering this from the node costs one `invoke_instance` per standard, and
there are nine of them. That is the whole reason the answer gets cached on the
instance document -- see `CISSupport` in `ccdexplorer.domain.mongo`.

Two facts make the cache cheap to fill:

* **Most contracts can be answered without asking the node at all.** ms_modules
  parses the wasm exports when a module is deployed, so `modules.methods` says
  whether a `supports` entrypoint exists. If it does not, no CIS standard can
  be supported, and on mainnet that covers the large majority of instances.
* **The answer only changes when the instance is upgraded to another module**,
  which ms_instances already observes.

This module holds the logic both writers share, so the indexer and the API
cannot drift apart on what "supports CIS-2" means.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from ccdexplorer.domain.generic import NET, StandardIdentifiers

from .core import CIS

#: The method name ms_modules records for a `supports` entrypoint. It stores
#: the bare method, having split `<contract>.supports` on the dot.
SUPPORTS_METHOD = "supports"


def contract_name_from_instance(instance: dict[str, Any]) -> str | None:
    """The contract name an instance runs, without the `init_` prefix.

    Entrypoints are addressed as `<contract name>.<method>`, so this is what
    turns an instance into something invocable.
    """
    for version in ("v1", "v0"):
        block = instance.get(version)
        if isinstance(block, dict) and block.get("name"):
            return str(block["name"]).replace("init_", "")
    return None


def source_module_from_instance(instance: dict[str, Any]) -> str | None:
    """The module an instance currently runs. Changes when it is upgraded."""
    if instance.get("source_module"):
        return str(instance["source_module"])
    for version in ("v1", "v0"):
        block = instance.get(version)
        if isinstance(block, dict) and block.get("source_module"):
            return str(block["source_module"])
    return None


def module_can_support(module: dict[str, Any] | None) -> bool:
    """Whether a module exports a `supports` entrypoint at all.

    Only the negative is reliable. ms_modules flattens `methods` across every
    contract in a module, so a hit means *some* contract exports `supports`,
    not necessarily this one -- which is fine, because a hit only means "ask
    the node". A miss means no contract in the module exports it, so no
    instance of it can support anything, and that is decidable here for free.
    """
    if not module:
        # No module document is not evidence of absence -- fall back to asking.
        return True
    methods = module.get("methods")
    if not isinstance(methods, list):
        return True
    return SUPPORTS_METHOD in methods


def query_supported_standards(
    grpc_client, index: int, subindex: int, contract_name: str, net: NET
) -> list[str]:
    """Ask the node which standards this instance reports supporting.

    Blocking: one gRPC round trip per standard. Callers on an event loop must
    push this to a thread.
    """
    cis = CIS(grpc_client, index, subindex, f"{contract_name}.supports", net)
    supported = []
    for standard in StandardIdentifiers:
        if cis.supports_standard(standard):
            supported.append(standard.value)
    return supported


def build_cis_support(
    grpc_client,
    net: NET,
    instance: dict[str, Any],
    module: dict[str, Any] | None,
    index: int,
    subindex: int,
) -> dict[str, Any] | None:
    """Resolve CIS support for one instance, ready to store on its document.

    Returns None when it cannot be determined -- an instance with no contract
    name, or a node that would not answer. Storing nothing means the next
    reader retries, which is the right outcome; storing an empty list would
    cache a failure as a fact.
    """
    contract_name = contract_name_from_instance(instance)
    if not contract_name:
        return None

    source_module = source_module_from_instance(instance)
    now = dt.datetime.now().astimezone(dt.timezone.utc)

    if not module_can_support(module):
        return {
            "standards": [],
            "source_module": source_module,
            "has_supports_entrypoint": False,
            "checked_at": now,
        }

    try:
        standards = query_supported_standards(grpc_client, index, subindex, contract_name, net)
    except Exception:
        return None

    return {
        "standards": standards,
        "source_module": source_module,
        "has_supports_entrypoint": True,
        "checked_at": now,
    }


def cached_standards(
    instance: dict[str, Any] | None,
) -> list[str] | None:
    """Standards from an instance document, or None when the cache cannot serve.

    A cached answer is only usable if it was obtained from the module the
    instance runs now: an upgrade can change the answer, and this is the check
    that catches one the indexer missed.
    """
    if not instance:
        return None
    cached = instance.get("cis_support")
    if not isinstance(cached, dict):
        return None
    if cached.get("source_module") != source_module_from_instance(instance):
        return None
    standards = cached.get("standards")
    return standards if isinstance(standards, list) else None
