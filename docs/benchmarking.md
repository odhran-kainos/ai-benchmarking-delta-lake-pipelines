# Benchmarking Framework Overview

## Purpose
Establish a neutral, synthetic, and reproducible PySpark + Delta Lake pipeline environment to evaluate AI-assisted coding tools across planning, implementation, and quality dimensions.

## Underlying Repository
Fork of: dw31/delta-lake-pipelines (baseline medallion-style, config-driven).

## Task Suite
| ID | Title | Summary |
|----|-------|---------|
| T1 | Bronze Ingestion (transactions) | Introduce new raw dataset + normalization |
| T2 | Silver Enrichment | Join transactions with customers; handle late arrivals |
| T3 | Quality Gate | Null %, duplicates, blocking promotion to Gold |
| T4 | Gold Aggregate | Daily revenue by segment |
| T5 | Testing Upgrade | Add unit + integration coverage |
| T6 | CDC Merge | Delta merge semantics for updates/deletes |
| T7 | Streaming Variant | Structured Streaming ingestion path |
| T8 | Lineage Metadata | Emit run-level transformation manifest |

## Metrics
- Correctness: test pass %, coverage
- Data Quality: null ratio, duplicates removed
- Performance: wall-clock per stage
- Maintainability: complexity, lint issues
- Documentation: docstring coverage
- Planning: task decomposition score (manual rubric)
- Security: vulnerability count
- Productivity: human edits post-AI

## Running a Task
```
make benchmark-task T=T1
```

## Directory Layout (Benchmark Additions)
```
benchmark/
  tasks/
  scripts/
  metrics/
  reports/
  scoring.yaml
```

## Interaction Protocol with AI Tools
Each tool is given a single task spec and repository context; generated code is applied on an isolated branch `tool/<name>/task-<ID>`.

## Scoring
Weights configured in `benchmark/scoring.yaml`. See `reports/benchmark_report.ipynb` for aggregation.

## Synthetic Data
Created via scripts under `scripts/data/`; deterministic seed ensures reproducibility.

## Guardrails
- No real client data.
- Pre-commit hooks for secrets & style.
- Branch protection on `main`.

## Roadmap
Add drift detection, multi-tool comparative dashboards, streaming stress tests.