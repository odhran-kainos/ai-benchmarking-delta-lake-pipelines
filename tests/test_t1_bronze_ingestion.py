"""Acceptance tests for the bronze transactions ingestion pipeline."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, Iterable
from uuid import uuid4

import pytest
import yaml
from delta import DeltaTable
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline


def _write_transactions_file(target_path: Path, records: Iterable[Dict[str, object]]) -> str:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    return str(target_path)


def _write_config(tmp_path: Path, source_path: str) -> Dict[str, object]:
    config = {
        "database": {"bronze_path": str(tmp_path / "bronze")},
        "data_quality": {"enable_validation": True, "fail_on_error": False},
        "logging": {"level": "INFO", "format": "%(message)s"},
        "data_sources": {
            "transactions": {
                "path": source_path,
                "format": "json",
                "schema_enforcement": True,
            }
        },
        "quarantine": {
            "transactions_path": str(tmp_path / "quarantine"),
            "export_path": str(tmp_path / "quarantine" / "export"),
            "retention_days": 30,
        },
    }

    config_path = tmp_path / "pipeline_config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    config["_path"] = str(config_path)
    return config


def _run_pipeline(spark_session, config: Dict[str, object]):
    pipeline = BronzeTransactionsPipeline(spark_session, config_path=config["_path"])  # type: ignore[index]
    return pipeline.run(run_id=f"bronze-run-{uuid4()}")


@pytest.mark.usefixtures("cleanup_delta_tables")
def test_bronze_pipeline_writes_partitioned_delta_table(tmp_path, spark_session, cleanup_delta_tables):
    records = [
        {"transaction_id": "T-100", "customer_id": "C1", "event_timestamp": "2025-11-17T10:00:00", "amount": 125.5, "currency": "EUR"},
        {"transaction_id": "T-100", "customer_id": "C1", "event_timestamp": "2025-11-17T10:01:00", "amount": 150.5, "currency": "EUR"},
        {"transaction_id": "T-200", "customer_id": "C2", "event_timestamp": "2025-11-17T11:00:00", "amount": 20.0, "currency": "EUR"},
        {"customer_id": "C3", "event_timestamp": "2025-11-17T12:00:00", "amount": 99.0, "currency": "EUR"},
    ]

    source_file = _write_transactions_file(tmp_path / "inputs" / "transactions.json", records)
    config = _write_config(tmp_path, source_file)

    cleanup_delta_tables(str(Path(config["database"]["bronze_path"]) / "transactions"))  # type: ignore[index]
    cleanup_delta_tables(config["quarantine"]["transactions_path"])  # type: ignore[index]
    cleanup_delta_tables(config["quarantine"]["export_path"])  # type: ignore[index]

    metrics = _run_pipeline(spark_session, config)

    bronze_path = Path(config["database"]["bronze_path"]) / "transactions"  # type: ignore[index]
    assert DeltaTable.isDeltaTable(spark_session, str(bronze_path))

    bronze_df = spark_session.read.format("delta").load(str(bronze_path))
    expected_columns = {
        "transaction_id",
        "customer_id",
        "event_timestamp",
        "amount",
        "currency",
        "_ingest_ts",
        "_ingest_date",
        "_pipeline_run_id",
        "_source_file",
    }
    assert expected_columns.issubset(set(bronze_df.columns))

    assert metrics["rows_raw"] == len(records)
    assert metrics["rows_invalid"] == 1
    assert metrics["dedupe_dropped"] == 1
    assert metrics["rows_loaded"] == bronze_df.count() == 2
    assert all(row["_pipeline_run_id"] == metrics["pipeline_run_id"] for row in bronze_df.select("_pipeline_run_id").distinct().collect())

    ingest_dates = [row["_ingest_date"] for row in bronze_df.select("_ingest_date").distinct().collect()]
    assert len(ingest_dates) == 1, "Bronze output must be partitioned by a single ingest date per run"


@pytest.mark.usefixtures("cleanup_delta_tables")
def test_quarantine_persistence_and_export(tmp_path, spark_session, cleanup_delta_tables):
    records = [
        {"transaction_id": "TX-1", "customer_id": "C1", "event_timestamp": "2025-11-17T10:00:00", "amount": 5.0, "currency": "USD"},
        {"customer_id": "C2", "event_timestamp": "2025-11-17T10:05:00", "amount": 10.0, "currency": "USD"},
    ]

    source_file = _write_transactions_file(tmp_path / "inputs" / "transactions.json", records)
    config = _write_config(tmp_path, source_file)

    bronze_transactions_path = Path(config["database"]["bronze_path"]) / "transactions"  # type: ignore[index]
    quarantine_path = Path(config["quarantine"]["transactions_path"])  # type: ignore[index]
    export_root = Path(config["quarantine"]["export_path"])  # type: ignore[index]

    cleanup_delta_tables(str(bronze_transactions_path))
    cleanup_delta_tables(str(quarantine_path))
    cleanup_delta_tables(str(export_root))

    metrics = _run_pipeline(spark_session, config)

    assert metrics["rows_invalid"] == 1
    assert metrics["rows_loaded"] == 1
    assert metrics["quarantine_ready_within_5_min"] is True
    assert metrics["quarantine_export_latency_seconds"] is not None

    quarantine_df = spark_session.read.format("delta").load(str(quarantine_path))
    quarantine_columns = {
        "transaction_id",
        "rejection_reason",
        "validation_rule_id",
        "_pipeline_run_id",
        "_source_file",
        "_quarantine_ts",
        "_expires_at",
        "status",
    }
    assert quarantine_columns.issubset(set(quarantine_df.columns))
    assert quarantine_df.count() == 1
    assert quarantine_df.first().status == "quarantined"

    export_target = export_root / metrics["pipeline_run_id"]
    assert DeltaTable.isDeltaTable(spark_session, str(export_target))
    export_df = spark_session.read.format("delta").load(str(export_target))
    assert export_df.count() == metrics["rows_invalid"]
    assert metrics["sla_15_min_passed"] is True


@pytest.mark.usefixtures("cleanup_delta_tables")
def test_metrics_align_with_table_state(tmp_path, spark_session, cleanup_delta_tables):
    records = [
        {"transaction_id": "TX-2", "customer_id": "C1", "event_timestamp": "2025-11-17T09:00:00", "amount": 12.0, "currency": "USD"},
        {"transaction_id": "TX-3", "customer_id": "C2", "event_timestamp": "2025-11-17T09:05:00", "amount": 40.0, "currency": "USD"},
    ]

    source_file = _write_transactions_file(tmp_path / "inputs" / "transactions.json", records)
    config = _write_config(tmp_path, source_file)

    bronze_transactions_path = Path(config["database"]["bronze_path"]) / "transactions"  # type: ignore[index]
    quarantine_path = Path(config["quarantine"]["transactions_path"])  # type: ignore[index]
    export_root = Path(config["quarantine"]["export_path"])  # type: ignore[index]

    cleanup_delta_tables(str(bronze_transactions_path))
    cleanup_delta_tables(str(quarantine_path))
    cleanup_delta_tables(str(export_root))

    metrics = _run_pipeline(spark_session, config)

    bronze_df = spark_session.read.format("delta").load(str(bronze_transactions_path))
    assert metrics["rows_loaded"] == bronze_df.count()
    assert metrics["rows_invalid"] == 0
    assert metrics["dedupe_dropped"] == 0
    assert metrics["rows_raw"] == len(records)
    assert metrics["quarantine_ready_within_5_min"] is True

    # Count reconciliation check
    assert metrics["rows_loaded"] + metrics["rows_invalid"] == metrics["rows_raw"]
    assert metrics["ingestion_duration_seconds"] is not None
    assert metrics["ingestion_duration_seconds"] >= 0.0

    # Metrics should include config snapshot for observability
    snapshot = metrics["config_snapshot"]
    assert snapshot["transactions_source"]["path"] == source_file
    assert snapshot["quarantine"]["transactions_path"] == str(quarantine_path)
