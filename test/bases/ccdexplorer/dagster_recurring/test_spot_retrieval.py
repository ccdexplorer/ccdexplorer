from datetime import datetime, timezone
from types import SimpleNamespace

import dagster as dg
import pytest
from ccdexplorer.dagster_recurring.recurring import update_spot_retrieval
from ccdexplorer.dagster_recurring.recurring.update_spot_retrieval import (
    perform_spot_retrieval_update,
)
from ccdexplorer.dagster_recurring.src import spot_retrieval as spot_retrieval_src
from ccdexplorer.mongodb import CollectionsUtilities


class _Collection:
    """Stands in for one Mongo collection, recording what was asked of it."""

    def __init__(self, documents=None):
        self.documents = documents or []
        self.find_calls = 0
        self.bulk_writes = []

    def find(self, *args, **kwargs):
        self.find_calls += 1
        return list(self.documents)

    def bulk_write(self, queue):
        self.bulk_writes.append(queue)


def _mongodb(translations=("BTC", "ETH", "EUROe")):
    translation_docs = [{"token": t, "translation": t.lower()} for t in translations]
    return SimpleNamespace(
        utilities={
            CollectionsUtilities.token_api_translations: _Collection(translation_docs),
            CollectionsUtilities.exchange_rates: _Collection(),
        }
    )


def _context():
    return SimpleNamespace(
        log=SimpleNamespace(info=lambda *a, **k: None, error=lambda *a, **k: None)
    )


@pytest.fixture(autouse=True)
def _no_pacing(monkeypatch):
    """The pacing sleep is real time; tests do not need to wait it out."""
    monkeypatch.setattr(update_spot_retrieval, "PACING_SECONDS", 0)


def _rate_from(source: str):
    def _fake(token, *args, **kwargs):
        return 200, {"_id": f"USD/{token}", "token": token, "rate": 1.0, "source": source}

    return _fake


def _no_rate(status: int):
    def _fake(token, *args, **kwargs):
        return status, None

    return _fake


def test_every_token_lands_in_a_single_bulk_write(monkeypatch):
    """The point of the batch: one write for the cycle, not one per token."""
    monkeypatch.setattr(update_spot_retrieval, "coinapi", _rate_from("CoinAPI"))
    mongodb = _mongodb()

    written, failed = perform_spot_retrieval_update(_context(), ["BTC", "ETH", "EUROe"], mongodb)

    rates = mongodb.utilities[CollectionsUtilities.exchange_rates]
    assert written == ["BTC", "ETH", "EUROe"]
    assert failed == []
    assert len(rates.bulk_writes) == 1
    assert len(rates.bulk_writes[0]) == 3


def test_translations_are_read_once_for_the_whole_batch(monkeypatch):
    """This query used to run once per token, every ten minutes."""
    monkeypatch.setattr(update_spot_retrieval, "coinapi", _rate_from("CoinAPI"))
    mongodb = _mongodb()

    perform_spot_retrieval_update(_context(), ["BTC", "ETH", "EUROe"], mongodb)

    assert mongodb.utilities[CollectionsUtilities.token_api_translations].find_calls == 1


def test_a_token_without_a_rate_is_reported_and_the_rest_still_store(monkeypatch):
    """One dead token must not cost the others their prices."""
    monkeypatch.setattr(update_spot_retrieval, "coinapi", _no_rate(429))

    def only_eth(token, translation, client):
        if token == "ETH":
            return 200, {"_id": "USD/ETH", "token": "ETH", "rate": 2.0, "source": "CoinGecko"}
        return 404, None

    monkeypatch.setattr(update_spot_retrieval, "coingecko", only_eth)
    mongodb = _mongodb()

    written, failed = perform_spot_retrieval_update(_context(), ["BTC", "ETH"], mongodb)

    assert written == ["ETH"]
    assert failed == ["BTC"]
    assert len(mongodb.utilities[CollectionsUtilities.exchange_rates].bulk_writes) == 1


def test_coingecko_is_the_fallback_when_coinapi_has_nothing(monkeypatch):
    monkeypatch.setattr(update_spot_retrieval, "coinapi", _no_rate(500))
    monkeypatch.setattr(update_spot_retrieval, "coingecko", _rate_from("CoinGecko"))
    mongodb = _mongodb()

    written, failed = perform_spot_retrieval_update(_context(), ["BTC"], mongodb)

    assert (written, failed) == (["BTC"], [])
    stored = mongodb.utilities[CollectionsUtilities.exchange_rates].bulk_writes[0]
    assert len(stored) == 1


def test_a_raising_coinapi_still_falls_through_to_coingecko(monkeypatch):
    """An expired CoinAPI key alone must not stop prices being stored."""

    def boom(token, client):
        raise RuntimeError("CoinAPI unreachable")

    monkeypatch.setattr(update_spot_retrieval, "coinapi", boom)
    monkeypatch.setattr(update_spot_retrieval, "coingecko", _rate_from("CoinGecko"))
    mongodb = _mongodb()

    written, failed = perform_spot_retrieval_update(_context(), ["BTC"], mongodb)

    assert (written, failed) == (["BTC"], [])


def test_nothing_is_written_when_no_token_yields_a_rate(monkeypatch):
    monkeypatch.setattr(update_spot_retrieval, "coinapi", _no_rate(500))
    monkeypatch.setattr(update_spot_retrieval, "coingecko", _no_rate(500))
    mongodb = _mongodb()

    written, failed = perform_spot_retrieval_update(_context(), ["BTC", "ETH"], mongodb)

    assert written == []
    assert failed == ["BTC", "ETH"]
    assert mongodb.utilities[CollectionsUtilities.exchange_rates].bulk_writes == []


SCHEDULED_AT = datetime(2026, 9, 16, 7, 0, tzinfo=timezone.utc)


def _schedule_context(instance):
    return dg.build_schedule_context(instance=instance, scheduled_execution_time=SCHEDULED_AT)


def test_schedule_asks_for_one_run_covering_every_token(monkeypatch):
    """One run per cycle rather than one per token: the range spans all of them."""
    tokens = ["BTC", "ETH", "EUROe", "USDR"]
    monkeypatch.setattr(spot_retrieval_src, "current_token_keys", lambda: tokens)

    with dg.instance_for_test() as instance:
        result = spot_retrieval_src.schedule(_schedule_context(instance))

        assert isinstance(result, dg.RunRequest)
        # The range is what makes one run cover every token; start and end are
        # the ends of the stored partition order, so new tokens are included
        # without the schedule having to name them.
        assert result.tags[spot_retrieval_src.PARTITION_RANGE_START_TAG] == "BTC"
        assert result.tags[spot_retrieval_src.PARTITION_RANGE_END_TAG] == "USDR"
        assert (
            instance.get_dynamic_partitions(spot_retrieval_src.partitions_def_tokens.name) == tokens
        )


def test_schedule_still_skips_while_a_run_is_in_flight(monkeypatch):
    """Unchanged guard: a slow cycle must not stack runs on top of each other."""
    monkeypatch.setattr(spot_retrieval_src, "current_token_keys", lambda: ["BTC", "ETH"])
    job_def = spot_retrieval_src.defs.resolve_job_def("j_spot_retrieval")

    with dg.instance_for_test() as instance:
        instance.create_run_for_job(job_def=job_def, status=dg.DagsterRunStatus.STARTED)

        result = spot_retrieval_src.schedule(_schedule_context(instance))

        assert isinstance(result, dg.SkipReason)


def test_schedule_skips_when_no_token_has_a_price_source(monkeypatch):
    monkeypatch.setattr(spot_retrieval_src, "current_token_keys", lambda: [])

    with dg.instance_for_test() as instance:
        result = spot_retrieval_src.schedule(_schedule_context(instance))

        assert isinstance(result, dg.SkipReason)


def test_the_asset_covers_a_whole_partition_range_in_one_run():
    """The backfill policy is what lets a single run span every token."""
    assert spot_retrieval_src.spot_retrieval.backfill_policy == dg.BackfillPolicy.single_run()
