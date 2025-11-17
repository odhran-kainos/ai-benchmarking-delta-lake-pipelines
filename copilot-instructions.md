# Copilot Context: PySpark Data Pipeline Benchmark Baseline

This file is designed to onboard GitHub Copilot and any new developers to the current state of our AI benchmarking setup for PySpark ETL pipelines. **Paste or reference this file in your Copilot Chat session in VS Code to ensure Copilot has key context.**

## Project Purpose

- Establish a **neutral, reproducible baseline PySpark + Delta Lake (medallion architecture) pipeline** for head-to-head comparison of AI-assisted coding approaches (e.g. Copilot, planning agents, spec-driven tools).
- All data and extensions are 100% synthetic for security and compliance.

## Current State

- **Repository**: Fork of [dw31/delta-lake-pipelines](https://github.com/dw31/delta-lake-pipelines)
- **Working branch**: `benchmark-foundation`
- **Baseline branch**: `baseline-upstream` (untouched upstream, for diffing)

## Benchmark Harness

- Top-level `benchmark/` directory containing:
    - `tasks/`: YAML files (T1–T8) with detailed spec/acceptance for benchmarking
    - `scripts/`: Automation for running tasks, collecting metrics (e.g. `run_task.sh`)
    - `metrics/`: Output directory for run/test/quality artifacts
    - `scoring.yaml`: Benchmark weights & thresholds
- `Makefile` targets:
    - `make benchmark-task T=<task_id>`
    - `make data-gen`: Generate synthetic data (`scripts/data/generate_transactions.py`)
    - `make test`: Run pytest and compute coverage

## Tasks Defined So Far

| ID  | Title                                   | Status    |
|-----|-----------------------------------------|-----------|
| T1  | Bronze Ingestion (transactions)         | SPECIFIED |
| T2  | Silver Enrichment (join customers)      | SPECIFIED |
| T3  | Quality Gate on data ID/fields          | OUTLINED  |
| T4  | Gold Aggregate (daily revenue)          | OUTLINED  |
| T5–T8 | Testing, CDC, Streaming, Lineage      | PLANNED   |

## Example Task YAML (T1)
```yaml
id: T1
title: "Add Bronze ingestion for synthetic transactions dataset"
goals:
  - Ingest JSON ("transactions.json") from data/raw_seed/
  - Schema normalization, type casting, add ingest metadata
  - Reject rows missing transaction_id
acceptance_criteria:
  - Delta table bronze_transactions, expected columns/row counts
metrics:
  - rows_raw, rows_invalid, rows_loaded, ingestion_duration_seconds
ai_guidance:
  - Prefer config-driven changes, reuse utilities, avoid hardcoding paths
```

## Synthetic Data

- Use `scripts/data/generate_transactions.py` (deterministic seed)
- Output: `data/raw_seed/transactions.json` (includes duplicates, late arrivals for realistic testing)

## Benchmarks & Metrics

- Harness must capture: correctness (test pass), data quality, performance, maintainability, planning quality, documentation, security
- Metrics output as JSON under `benchmark/metrics/<task_id>/`

## Security/Compliance

- NO real or customer data – scripts/data/ only
- Pre-commit hooks: code style, secrets scan
- `.gitignore` covers data, logs, temp files

## How to Use In Copilot Chat

1. **Always reference this context.md in your session** (paste this directly or `@IncludeFile` if using Copilot Workspace context features)
2. For a new task, copy the relevant YAML spec from `benchmark/tasks/`
3. Request: step-by-step plan, then code, then tests, in config-driven style
4. For each PR/commit:
    - Isolate changes per task (e.g. feature/bronze-ingest-T1)
    - Run `make data-gen && make test && make benchmark-task T=T1`
    - Metrics file must confirm acceptance

## Key Files

- `benchmark/tasks/T1_ingest_transactions.yaml` – First benchmark task
- `scripts/data/generate_transactions.py` – Synthetic transactions generator
- `benchmark/scripts/run_task.sh` – Benchmark run harness
- `Makefile` – Project automation

## Project Roadmap

- Complete T1–T4 (batch flow)
- Expand to streaming/CDC/lineage (T5–T8)
- Comparative AI runs: Copilot vs other tools, recorded prompts & diffs

## Prompting Hints

- “Plan/implement Bronze ingestion for T1, config-driven, patch only, no hardcoded paths.”
- “Generate pytest for transactions ingestion edge cases (missing ID, duplicate).”
- “Provide unified diff for T2 enrichment logic and config changes.”

---

_Last updated: 2025-11-17, see `docs/benchmarking.md` for expanded details and example prompts._