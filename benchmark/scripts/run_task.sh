#!/usr/bin/env bash
# NOTE: This is a placeholder implementation for MVP (Option B)
# Actual pipeline execution will be added in future iterations
set -euo pipefail

TASK_ID="${T:-${1:-}}"
if [[ -z "${TASK_ID}" ]]; then
  echo "Usage: T=<task_id> make benchmark-task OR ./benchmark/scripts/run_task.sh <task_id>"
  exit 1
fi

echo "[INFO] Running benchmark for task ${TASK_ID}"

# 1. Load task spec
shopt -s nullglob
MATCHES=(benchmark/tasks/${TASK_ID}{_*,}.yaml)
SPEC="${MATCHES[0]:-}"
if [[ -z "${SPEC}" ]]; then
  echo "[ERROR] No spec found matching: ${TASK_ID}"
  exit 2
fi
echo "[INFO] Using spec: ${SPEC}"

# 2. (Placeholder) Invoke pipeline job(s) - adapt to project specifics
# Example: python -m src.jobs.${TASK_ID}_job
START=$(date +%s)
# Simulate pipeline logic placeholder:
sleep 1
END=$(date +%s)
DURATION=$((END-START))

# 3. Collect metrics (extend with real logic)
METRICS_DIR="benchmark/metrics/${TASK_ID}"
mkdir -p "${METRICS_DIR}"
cat > "${METRICS_DIR}/run_metrics.json" <<EOF
{
  "task_id": "${TASK_ID}",
  "duration_seconds": ${DURATION},
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

echo "[INFO] Metrics written to ${METRICS_DIR}/run_metrics.json"