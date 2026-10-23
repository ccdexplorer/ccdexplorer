"""Reconciling stable_address_info against the chain's own account list.

The collection is what turns an address into an index everywhere -- the
site's boot map and /accounts/get-indexes both read it, and nothing else
knows an index. It was only ever written one account at a time, by
ms_accounts reacting to an account_creation transaction, so an account that
never had one on the indexed chain can never appear.

Measured on 2026-10-23: devnet had 85 accounts on chain and 29 in the
collection, those 29 being exactly its 29 account_creation transactions at
indices 56-84. Indices 0-55 -- the genesis set of the current devnet, which
is replaced periodically -- were absent, so every link to one rendered as an
address. The transaction that surfaced it was sent by index 4.

mainnet does not have the hole because a separate mainnet-only job walks the
whole account list and carries a hardcoded list of its 14 genesis addresses.
This asset is that walk, for every net, and it is left alone by design.
"""

from types import SimpleNamespace

import pytest

from ccdexplorer.dagster_recurring.recurring.update_stable_address_info import (
    reconcile_stable_address_info,
)
from ccdexplorer.mongodb import Collections

A = "4t1efAP22FSeKShc4WSfkyPDxyBhCqtQqEHCePWSAMts8Xq8G3"
B = "3mUxfvc1Rdt4muJfCLtjvnygwL3kaXypDNSm52oprXoZwwfkLK"
C = "4GzSLW8VM2hrzTkFJKzMKrVMsHMh4edeziBCSTkBwFoDGGTZ6q"


class _Collection:
    """Just enough of a pymongo collection for this one query and write."""

    def __init__(self, docs=None):
        self.docs = {d["_id"]: d for d in (docs or [])}
        self.writes = []
        self.deletes = 0

    def find(self, _filter=None, _projection=None):
        return [dict(d) for d in self.docs.values()]

    def bulk_write(self, ops):
        self.writes.append(ops)
        for op in ops:
            self.docs[op._filter["_id"]] = op._doc
        return SimpleNamespace(upserted_count=len(ops))

    def delete_many(self, _filter):
        self.deletes += 1


class _Mongo:
    def __init__(self, docs=None):
        col = _Collection(docs)
        for net in ("mainnet", "testnet", "devnet"):
            setattr(self, net, {Collections.stable_address_info: col})
        self.col = col


class _Grpc:
    """Returns an account whose index is its position in the chain list."""

    def __init__(self, on_chain, failing=()):
        self.on_chain = list(on_chain)
        self.failing = set(failing)
        self.list_calls = []
        self.info_calls = []

    def get_account_list(self, block_hash, net=None):
        self.list_calls.append((block_hash, net))
        return list(self.on_chain)

    def get_account_info(self, block_hash, hex_address=None, account_index=None, net=None):
        self.info_calls.append(hex_address)
        if hex_address in self.failing:
            raise RuntimeError("node said no")
        return SimpleNamespace(address=hex_address, index=self.on_chain.index(hex_address))


class _Stable:
    """Stands in for AccountInfoStable: _id is the 29-character prefix."""

    def __init__(self, ai):
        self.ai = ai

    @classmethod
    def from_account_info(cls, ai):
        return cls(ai)

    def to_collection(self):
        return {
            "_id": self.ai.address[:29],
            "account_address": self.ai.address,
            "account_index": self.ai.index,
        }


@pytest.fixture(autouse=True)
def _stable(monkeypatch):
    """The real one needs a full CCD_AccountInfo; the shape is what matters."""
    from ccdexplorer.dagster_recurring.recurring import update_stable_address_info as mod

    monkeypatch.setattr(mod, "AccountInfoStable", _Stable)


def _context():
    return SimpleNamespace(
        log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
    )


def _run(on_chain, known=(), net="devnet", failing=()):
    mongo = _Mongo(
        [{"_id": a[:29], "account_address": a, "account_index": i} for i, a in enumerate(known)]
    )
    grpc = _Grpc(on_chain, failing=failing)
    result = reconcile_stable_address_info(_context(), mongo, grpc, net)
    return result, mongo, grpc


# --- the hole it exists to close ------------------------------------------


def test_an_account_the_collection_never_saw_is_added():
    """The devnet case: on chain, absent from the collection."""
    _, mongo, _ = _run(on_chain=[A, B, C], known=[A])

    assert B[:29] in mongo.col.docs
    assert mongo.col.docs[B[:29]]["account_index"] == 1


def test_only_the_missing_ones_are_fetched():
    """One get_account_info per missing account, and none for the rest. On
    mainnet that is the difference between 0 calls and 104,000."""
    _, _, grpc = _run(on_chain=[A, B, C], known=[A, C])

    assert grpc.info_calls == [B]


def test_nothing_missing_costs_no_account_lookups():
    """This runs on a schedule against every net, mainnet included, so the
    steady state has to be close to free."""
    result, mongo, grpc = _run(on_chain=[A, B], known=[A, B])

    assert grpc.info_calls == []
    assert mongo.col.writes == []
    assert result["added"] == 0


def test_the_chain_list_is_asked_for_once():
    _, _, grpc = _run(on_chain=[A, B], known=[A])

    assert len(grpc.list_calls) == 1


def test_it_reads_the_last_final_block():
    """A devnet is replaced periodically; a stored block hash would be from
    a chain that no longer exists."""
    _, _, grpc = _run(on_chain=[A], known=[])

    assert grpc.list_calls[0][0] == "last_final"


# --- the key the rest of the system looks up by ---------------------------


def test_the_document_is_keyed_on_the_canonical_prefix():
    """29 characters. The site's map, get-indexes and ms_accounts all agree
    on this key, and a document keyed any other way is invisible to them."""
    _, mongo, _ = _run(on_chain=[B], known=[])

    (op,) = mongo.col.writes[0]
    assert op._filter == {"_id": B[:29]}
    assert len(op._filter["_id"]) == 29


def test_an_account_already_known_is_not_rewritten():
    """Reconciling is not re-indexing: untouched rows keep whatever
    ms_accounts or the mainnet job wrote, including fields this does not
    know about."""
    _, mongo, _ = _run(on_chain=[A], known=[A])

    assert mongo.col.writes == []


# --- it must never remove anything ---------------------------------------


def test_nothing_is_ever_deleted():
    """An account cannot leave the chain, so a row with no match is a row
    this walk failed to see -- never a row to drop."""
    _, mongo, _ = _run(on_chain=[A], known=[A, B, C])

    assert mongo.col.deletes == 0
    assert B[:29] in mongo.col.docs and C[:29] in mongo.col.docs


# --- a node that answers badly -------------------------------------------


def test_one_failed_account_does_not_cost_the_others():
    """85 accounts on a devnet that may restart underneath the walk."""
    result, mongo, _ = _run(on_chain=[A, B, C], known=[], failing=[B])

    assert A[:29] in mongo.col.docs
    assert C[:29] in mongo.col.docs
    assert B[:29] not in mongo.col.docs
    assert result["failed"] == 1
    assert result["added"] == 2


def test_an_empty_chain_is_not_an_error():
    """A freshly replaced devnet answers with nothing for a while."""
    result, mongo, grpc = _run(on_chain=[], known=[])

    assert result["added"] == 0
    assert grpc.info_calls == []
    assert mongo.col.writes == []


# --- what it reports ------------------------------------------------------


def test_the_result_counts_both_sides():
    result, _, _ = _run(on_chain=[A, B, C], known=[A])

    assert result["on_chain"] == 3
    assert result["already_known"] == 1
    assert result["missing"] == 2
    assert result["added"] == 2
    assert result["net"] == "devnet"


# --- the net it was asked about ------------------------------------------


@pytest.mark.parametrize("net", ["mainnet", "testnet", "devnet"])
def test_the_net_reaches_both_the_node_and_the_database(net):
    """One asset, three partitions. Writing devnet's accounts into mainnet
    would be worse than the hole it is closing."""
    _, _, grpc = _run(on_chain=[A], known=[], net=net)

    assert grpc.list_calls[0][1].value == net
    assert grpc.info_calls == [A]


def test_an_unknown_net_is_refused():
    with pytest.raises(ValueError):
        _run(on_chain=[A], known=[], net="nonsense")


# --- batching -------------------------------------------------------------


def test_a_large_backfill_is_written_in_batches():
    """mainnet's first run would be one bulk_write of 104,000 operations."""
    from ccdexplorer.dagster_recurring.recurring.update_stable_address_info import BATCH_SIZE

    chain = [f"{i:029d}{'x' * 21}" for i in range(BATCH_SIZE * 2 + 5)]
    result, mongo, _ = _run(on_chain=chain, known=[])

    assert result["added"] == len(chain)
    assert len(mongo.col.writes) == 3
    assert [len(w) for w in mongo.col.writes] == [BATCH_SIZE, BATCH_SIZE, 5]


# --- the asset around it --------------------------------------------------


def test_the_asset_is_partitioned_per_net():
    """One run per net, so devnet can be launched on its own the moment it
    is replaced without waiting on mainnet's 104,000."""
    from ccdexplorer.dagster_recurring.src import stable_address_info as src

    keys = src.stable_address_info.partitions_def.get_partition_keys()
    assert sorted(keys) == ["devnet", "mainnet", "testnet"]


def test_the_asset_is_registered():
    """An asset not in the repository is an asset nothing can run."""
    from ccdexplorer.dagster_recurring.repository import defs

    names = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "stable_address_info" in names


def test_there_is_a_job_to_launch_by_hand():
    from ccdexplorer.dagster_recurring.repository import defs

    assert "j_stable_address_info" in {j.name for j in defs.resolve_all_job_defs()}


def _fire_schedule(runs_in_flight):
    """The decorated ScheduleDefinition type-checks its context, so this
    drives the function underneath it with a stand-in instance."""
    from ccdexplorer.dagster_recurring.src import stable_address_info as src

    class _Instance:
        def get_run_records(self, _filters):
            return runs_in_flight

    fn = src.schedule._execution_fn.decorated_fn
    return fn(
        SimpleNamespace(
            instance=_Instance(),
            scheduled_execution_time=SimpleNamespace(isoformat=lambda: "2026-10-23T04:30:00"),
        )
    )


def test_the_schedule_asks_for_every_net():
    requests = _fire_schedule([])

    assert sorted(r.partition_key for r in requests) == ["devnet", "mainnet", "testnet"]


def test_the_schedule_skips_while_a_run_is_in_flight():
    """A walk of mainnet's account list overlapping itself buys nothing."""
    import dagster as dg

    assert isinstance(_fire_schedule(["a run"]), dg.SkipReason)


def test_accounts_retrieval_is_not_touched():
    """It is the mainnet job that already does this walk, and it is out of
    scope by instruction. Asserted so a later edit has to be deliberate."""
    import pathlib

    here = pathlib.Path(__file__).resolve()
    # test/ carries its own bases/ tree, so the justfile is the repo root.
    root = next(p for p in here.parents if (p / "justfile").is_file())
    base = root / "bases/ccdexplorer/dagster_recurring"
    src = (base / "src/stable_address_info.py").read_text()
    logic = (base / "recurring/update_stable_address_info.py").read_text()

    for text in (src, logic):
        assert "from ccdexplorer.accounts_retrieval" not in text
        assert "import ccdexplorer.accounts_retrieval" not in text
