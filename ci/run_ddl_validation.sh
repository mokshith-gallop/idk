#!/usr/bin/env bash
# ===========================================================================
# DDL Full Schema Validation — CI entry point
#
# Applies all 10 DDL files to a scratch BQ project (staging/ods/dm datasets)
# and validates all 9 acceptance criteria via live INFORMATION_SCHEMA reads.
#
# Prerequisites:
#   - .env file with BQ_PROJECT, BQ_LOCATION, GOOGLE_APPLICATION_CREDENTIALS
#   - gcloud auth application-default login (or service account key)
#
# Usage:
#   ci/run_ddl_validation.sh [--with-perf]
#
# Flags:
#   --with-perf   Also run scan-reduction benchmarks (AC9).
#                 Requires seeding synthetic data first (adds ~5min).
# ===========================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Source env vars
if [ -f .env ]; then
  set -a; source .env; set +a
fi

PY="${PYTHON:-.venv/bin/python}"
[ -x "$PY" ] || PY="python3"

WITH_PERF="${1:-}"

echo "== DDL Validation: BigQuery Physical Schema =="
echo "   Project: ${BQ_PROJECT:-<not set>}"
echo "   Date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

# ── Phase 1: Apply DDL and validate AC1-AC8 ──────────────────────────────
echo "== Phase 1/2: DDL application + schema verification (AC1-AC8) =="
"$PY" -m pytest tests/schema/test_ddl_full_e2e.py \
    -v --tb=long -m live_bq \
    --junit-xml=.report/ddl_validation.xml \
    2>&1 | tee .report/ddl_validation.log

PHASE1_EXIT=${PIPESTATUS[0]}

# ── Phase 2 (optional): Scan reduction benchmarks (AC9) ──────────────────
if [ "$WITH_PERF" = "--with-perf" ]; then
    echo ""
    echo "== Phase 2/2: Scan reduction benchmarks (AC9) =="
    echo "   Seeding synthetic data..."
    "$PY" -m lib.synth fixtures/synth_ddl_perf.yaml
    "$PY" -m lib.synth fixtures/synth_ddl_perf_ods.yaml

    echo "   Running scan-reduction tests..."
    "$PY" -m pytest tests/perf/test_scan_reduction.py \
        -v --tb=long -m live_bq \
        --junit-xml=.report/ddl_perf.xml \
        2>&1 | tee .report/ddl_perf.log

    PHASE2_EXIT=${PIPESTATUS[0]}
else
    echo ""
    echo "== Phase 2/2: Skipped (pass --with-perf to run AC9 benchmarks) =="
    PHASE2_EXIT=0
fi

# ── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "== Results =="
echo "   Phase 1 (AC1-AC8): $([ $PHASE1_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "   Phase 2 (AC9):     $([ $PHASE2_EXIT -eq 0 ] && echo 'PASS' || echo 'SKIP/FAIL')"

EXIT_CODE=$(( PHASE1_EXIT + PHASE2_EXIT ))
exit $EXIT_CODE
