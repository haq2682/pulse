#!/usr/bin/env bash
# =============================================================================
# test_streaming_modes.sh — Smoke-test for DB and API streaming pipeline modes
# =============================================================================
#
# WHAT THIS TESTS
# ---------------
#  1. Infrastructure health  — MinIO, Kafka, Debezium, Airflow all reachable
#  2. Airflow DAG inventory  — db_streaming, api_streaming, streaming_downstream,
#                              batch_downstream, ml_retrain all present
#  3. DB mode (end-to-end)   — trigger db_streaming DAG, wait for mapped/ output
#  4. API mode (end-to-end)  — trigger api_streaming DAG, wait for mapped/ output
#  5. Downstream pipeline    — trigger streaming_downstream manually, check cleaned/
#
# PREREQUISITES
# -------------
#  • docker compose stack is running  (docker compose up -d)
#  • Airflow is initialised           (airflow-init has completed)
#  • mc (MinIO CLI) is installed      (brew install minio/stable/mc  or snap install mc)
#  • curl, jq are installed
#
# USAGE
# -----
#  chmod +x test_streaming_modes.sh
#  ./test_streaming_modes.sh [OPTIONS]
#
# OPTIONS
#  --bucket NAME          Business bucket to use (default: pulse-test-bucket)
#  --db-uri URI           Source DB URI for CDC test
#                         (default: postgresql://postgres:postgres@10.5.0.5:5432/pulse)
#  --db-tables TABLES     Comma-separated table list (default: orders,customers,products)
#  --api-url URL          Frontend API URL to poll (default: http://10.5.0.9:8000/api/ingest/stream)
#  --airflow-url URL      Airflow webserver base URL (default: http://localhost:8090)
#  --airflow-user USER    Airflow admin username (default: admin)
#  --airflow-pass PASS    Airflow admin password (default: admin)
#  --minio-url URL        MinIO API URL (default: http://localhost:9000)
#  --minio-user USER      MinIO access key (default: minioadmin)
#  --minio-pass PASS      MinIO secret key (default: minioadmin)
#  --skip-db              Skip the DB mode test
#  --skip-api             Skip the API mode test
#  --skip-downstream      Skip the streaming_downstream trigger test
#  --wait SECONDS         How long to wait for mapped/ output (default: 120)
#  --help                 Show this message
#
# =============================================================================

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

pass()  { echo -e "${GREEN}[PASS]${RESET} $*"; }
fail()  { echo -e "${RED}[FAIL]${RESET} $*"; FAILURES=$((FAILURES + 1)); }
warn()  { echo -e "${YELLOW}[WARN]${RESET} $*"; }
info()  { echo -e "${CYAN}[INFO]${RESET} $*"; }
header(){ echo -e "\n${BOLD}${CYAN}══ $* ══${RESET}"; }

FAILURES=0

# ── Default config ─────────────────────────────────────────────────────────────
BUCKET="pulse-test-bucket"
DB_URI="postgresql://postgres:postgres@10.5.0.5:5432/pulse"
DB_TABLES="orders,customers,products"
API_URL="http://10.5.0.9:8000/api/ingest/stream"
AIRFLOW_URL="http://localhost:8090"
AIRFLOW_USER="admin"
AIRFLOW_PASS="admin"
MINIO_URL="http://localhost:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin"
SKIP_DB=false
SKIP_API=false
SKIP_DOWNSTREAM=false
WAIT_SECS=120

# ── Parse CLI arguments ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)          BUCKET="$2";        shift 2 ;;
    --db-uri)          DB_URI="$2";        shift 2 ;;
    --db-tables)       DB_TABLES="$2";     shift 2 ;;
    --api-url)         API_URL="$2";       shift 2 ;;
    --airflow-url)     AIRFLOW_URL="$2";   shift 2 ;;
    --airflow-user)    AIRFLOW_USER="$2";  shift 2 ;;
    --airflow-pass)    AIRFLOW_PASS="$2";  shift 2 ;;
    --minio-url)       MINIO_URL="$2";     shift 2 ;;
    --minio-user)      MINIO_USER="$2";    shift 2 ;;
    --minio-pass)      MINIO_PASS="$2";    shift 2 ;;
    --skip-db)         SKIP_DB=true;       shift   ;;
    --skip-api)        SKIP_API=true;      shift   ;;
    --skip-downstream) SKIP_DOWNSTREAM=true; shift ;;
    --wait)            WAIT_SECS="$2";     shift 2 ;;
    --help)
      grep '^#' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

AIRFLOW_AUTH="${AIRFLOW_USER}:${AIRFLOW_PASS}"
AIRFLOW_API="${AIRFLOW_URL}/api/v1"

# ── Helper: Airflow REST call ─────────────────────────────────────────────────
airflow_get()  { curl -s -u "$AIRFLOW_AUTH" "${AIRFLOW_API}$1"; }
airflow_post() { curl -s -u "$AIRFLOW_AUTH" -X POST -H "Content-Type: application/json" \
                      -d "$2" "${AIRFLOW_API}$1"; }
airflow_patch(){ curl -s -u "$AIRFLOW_AUTH" -X PATCH -H "Content-Type: application/json" \
                      -d "$2" "${AIRFLOW_API}$1"; }

# ── Helper: wait for a DAG run to reach a terminal state ─────────────────────
wait_for_dag_run() {
  local dag_id="$1" run_id="$2" timeout_secs="$3"
  local elapsed=0 state=""
  info "Waiting up to ${timeout_secs}s for ${dag_id}/${run_id} ..."
  while [[ $elapsed -lt $timeout_secs ]]; do
    state=$(airflow_get "/dags/${dag_id}/dagRuns/${run_id}" | jq -r '.state // "unknown"')
    case "$state" in
      success) return 0 ;;
      failed)  return 1 ;;
      running|queued) ;;
      *) warn "Unexpected state '${state}' for ${dag_id}"; ;;
    esac
    sleep 10; elapsed=$((elapsed + 10))
    info "  … ${state} (${elapsed}s elapsed)"
  done
  warn "Timed out after ${timeout_secs}s (last state: ${state})"
  return 2
}

# ── Helper: wait for a MinIO prefix to have ≥1 objects ───────────────────────
wait_for_minio_prefix() {
  local bucket="$1" prefix="$2" timeout_secs="$3"
  local elapsed=0 count=0
  info "Waiting up to ${timeout_secs}s for objects at ${bucket}/${prefix} ..."
  while [[ $elapsed -lt $timeout_secs ]]; do
    count=$(mc ls "pulse/${bucket}/${prefix}" 2>/dev/null | wc -l || echo 0)
    if [[ $count -gt 0 ]]; then
      info "  Found ${count} object(s) at ${bucket}/${prefix}"
      return 0
    fi
    sleep 10; elapsed=$((elapsed + 10))
    info "  … no objects yet (${elapsed}s elapsed)"
  done
  warn "No objects found at ${bucket}/${prefix} after ${timeout_secs}s"
  return 1
}

# ── Helper: trigger a DAG and return the run_id ───────────────────────────────
trigger_dag() {
  local dag_id="$1" conf="$2"
  local result
  result=$(airflow_post "/dags/${dag_id}/dagRuns" "{\"conf\": ${conf}}")
  local run_id
  run_id=$(echo "$result" | jq -r '.dag_run_id // empty')
  if [[ -z "$run_id" ]]; then
    echo "$result" >&2
    echo ""
  else
    echo "$run_id"
  fi
}

# =============================================================================
# SECTION 1: Infrastructure health checks
# =============================================================================
header "1. Infrastructure health"

# 1a. MinIO
MINIO_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${MINIO_URL}/minio/health/live" 2>/dev/null || echo "000")
if [[ "$MINIO_STATUS" == "200" ]]; then
  pass "MinIO is healthy (${MINIO_URL})"
else
  fail "MinIO health check failed (HTTP ${MINIO_STATUS})"
fi

# 1b. Kafka — check via docker exec
KAFKA_OK=$(docker exec kafka bash -c "kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1" | head -1 || echo "ERROR")
if echo "$KAFKA_OK" | grep -qvE "ERROR|Connection refused"; then
  pass "Kafka broker is reachable"
else
  fail "Kafka broker health check failed: ${KAFKA_OK}"
fi

# 1c. Debezium
DEB_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:8083/" 2>/dev/null || echo "000")
if [[ "$DEB_STATUS" == "200" ]]; then
  pass "Debezium Connect is healthy (http://localhost:8083)"
else
  fail "Debezium health check failed (HTTP ${DEB_STATUS})"
fi

# 1d. Airflow webserver
AF_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${AIRFLOW_URL}/health" 2>/dev/null || echo "000")
if [[ "$AF_STATUS" == "200" ]]; then
  AF_HEALTH=$(curl -s "${AIRFLOW_URL}/health" | jq -r '.metadatabase.status // "unknown"')
  pass "Airflow webserver healthy — metadatabase: ${AF_HEALTH}"
else
  fail "Airflow webserver not reachable at ${AIRFLOW_URL} (HTTP ${AF_STATUS})"
fi

# 1e. Airflow scheduler
AF_SCHED=$(curl -s -u "$AIRFLOW_AUTH" "${AIRFLOW_URL}/health" | jq -r '.scheduler.status // "unknown"')
if [[ "$AF_SCHED" == "healthy" ]]; then
  pass "Airflow scheduler is healthy"
else
  fail "Airflow scheduler status: ${AF_SCHED}"
fi

# =============================================================================
# SECTION 2: DAG inventory check
# =============================================================================
header "2. DAG inventory"

REQUIRED_DAGS=("db_streaming" "api_streaming" "streaming_downstream" "batch_downstream" "ml_retrain")
for dag in "${REQUIRED_DAGS[@]}"; do
  STATE=$(airflow_get "/dags/${dag}" | jq -r '.dag_id // empty')
  if [[ "$STATE" == "$dag" ]]; then
    IS_PAUSED=$(airflow_get "/dags/${dag}" | jq -r '.is_paused')
    pass "DAG '${dag}' found (paused: ${IS_PAUSED})"
    # Unpause if paused (needed for triggers to work)
    if [[ "$IS_PAUSED" == "true" ]]; then
      airflow_patch "/dags/${dag}" '{"is_paused": false}' > /dev/null
      info "  Unpaused DAG '${dag}'"
    fi
  else
    fail "DAG '${dag}' not found in Airflow — check scheduler logs"
  fi
done

# =============================================================================
# SECTION 3: MinIO setup — create test bucket
# =============================================================================
header "3. MinIO test bucket setup"

# Configure mc alias
mc alias set pulse "${MINIO_URL}" "${MINIO_USER}" "${MINIO_PASS}" --quiet 2>/dev/null || true

if mc ls pulse 2>/dev/null | grep -q "${BUCKET}"; then
  info "Bucket '${BUCKET}' already exists"
else
  mc mb "pulse/${BUCKET}" --quiet
  info "Created bucket '${BUCKET}'"
fi
pass "MinIO bucket '${BUCKET}' is ready"

# =============================================================================
# SECTION 4: DB mode test
# =============================================================================
if [[ "$SKIP_DB" == "true" ]]; then
  warn "Skipping DB mode test (--skip-db)"
else
  header "4. DB mode — db_streaming DAG"

  # 4a. Check Debezium connectors list
  CONNECTORS=$(curl -s "http://localhost:8083/connectors" | jq -r '.[]?' 2>/dev/null || echo "[]")
  info "Existing Debezium connectors: ${CONNECTORS:-none}"

  # 4b. Trigger db_streaming DAG
  DB_CONF=$(cat <<EOF
{
  "bucket":    "${BUCKET}",
  "db_uri":    "${DB_URI}",
  "db_tables": "${DB_TABLES}"
}
EOF
)
  info "Triggering db_streaming DAG..."
  DB_RUN_ID=$(trigger_dag "db_streaming" "$(echo "$DB_CONF" | jq -c '.')")

  if [[ -z "$DB_RUN_ID" ]]; then
    fail "Failed to trigger db_streaming DAG — see output above"
  else
    pass "db_streaming DAG triggered (run_id: ${DB_RUN_ID})"

    # 4c. Wait for deploy_debezium_connector task to succeed (finite task)
    info "Waiting 30s for deploy_debezium_connector task..."
    sleep 30
    CONNECTOR_TASK=$(airflow_get "/dags/db_streaming/dagRuns/${DB_RUN_ID}/taskInstances/deploy_debezium_connector" \
      | jq -r '.state // "unknown"')
    if [[ "$CONNECTOR_TASK" == "success" ]]; then
      pass "deploy_debezium_connector: success"
    else
      fail "deploy_debezium_connector state: ${CONNECTOR_TASK} (expected: success)"
      warn "Check Airflow task log at ${AIRFLOW_URL}/dags/db_streaming/grid"
    fi

    # 4d. Verify connector is RUNNING in Debezium
    CONNECTOR_NAME="pulse-${BUCKET}-connector"
    DEB_CONNECTOR_STATE=$(curl -s "http://localhost:8083/connectors/${CONNECTOR_NAME}/status" \
      | jq -r '.connector.state // "NOT_FOUND"')
    if [[ "$DEB_CONNECTOR_STATE" == "RUNNING" ]]; then
      pass "Debezium connector '${CONNECTOR_NAME}' is RUNNING"
    else
      fail "Debezium connector '${CONNECTOR_NAME}' state: ${DEB_CONNECTOR_STATE}"
      info "  Registered connectors: $(curl -s 'http://localhost:8083/connectors' | jq -r '.[]?')"
    fi

    # 4e. Check run_db_mapping_stream task is running (long-running — should be 'running', not 'success')
    info "Checking run_db_mapping_stream task state..."
    sleep 10
    STREAM_TASK=$(airflow_get "/dags/db_streaming/dagRuns/${DB_RUN_ID}/taskInstances/run_db_mapping_stream" \
      | jq -r '.state // "unknown"')
    if [[ "$STREAM_TASK" == "running" ]]; then
      pass "run_db_mapping_stream is 'running' (expected — this is the continuous streaming task)"
    elif [[ "$STREAM_TASK" == "queued" ]]; then
      pass "run_db_mapping_stream is 'queued' (waiting for executor slot — OK)"
    else
      fail "run_db_mapping_stream state: ${STREAM_TASK} (expected: running or queued)"
      warn "Check logs at ${AIRFLOW_URL}/dags/db_streaming/grid"
    fi

    # 4f. Wait for MinIO mapped/ output (CDC events flowing through)
    info "Waiting up to ${WAIT_SECS}s for mapped/ objects (requires real CDC traffic)..."
    if wait_for_minio_prefix "${BUCKET}" "mapped/" "${WAIT_SECS}"; then
      pass "mapped/ objects found in bucket '${BUCKET}' — CDC pipeline is flowing"
    else
      warn "No mapped/ objects yet — this is expected if the source DB has no recent changes"
      warn "Manually make a change to the source DB to trigger a CDC event:"
      warn "  docker exec postgresql psql -U postgres -d pulse -c \"UPDATE orders SET updated_at = NOW() LIMIT 1;\""
      # Don't count this as a failure since it needs real DB traffic
    fi
  fi
fi

# =============================================================================
# SECTION 5: API mode test
# =============================================================================
if [[ "$SKIP_API" == "true" ]]; then
  warn "Skipping API mode test (--skip-api)"
else
  header "5. API mode — api_streaming DAG"

  # 5a. Check if the frontend API endpoint exists
  API_HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    "$(echo "$API_URL" | sed 's|/api/ingest/stream||')/health" 2>/dev/null || echo "000")
  INGEST_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}" 2>/dev/null || echo "000")

  if [[ "$INGEST_STATUS" == "200" || "$INGEST_STATUS" == "422" ]]; then
    pass "Frontend ingest endpoint is reachable (HTTP ${INGEST_STATUS})"
  elif [[ "$API_HEALTH_STATUS" == "200" ]]; then
    warn "Main ingest endpoint returned HTTP ${INGEST_STATUS}, but /health is OK"
    warn "The api_streaming DAG will still trigger; verify your ingest endpoint response format"
  else
    warn "Frontend API at ${API_URL} is not yet reachable (HTTP ${INGEST_STATUS})"
    warn "The api container endpoint GET /api/ingest/stream needs to be implemented"
    warn "The check_api_health task in the DAG will fail — this is expected until the endpoint is built"
  fi

  # 5b. Trigger api_streaming DAG
  API_CONF=$(cat <<EOF
{
  "bucket":        "${BUCKET}",
  "api_url":       "${API_URL}",
  "poll_interval": 10
}
EOF
)
  info "Triggering api_streaming DAG..."
  API_RUN_ID=$(trigger_dag "api_streaming" "$(echo "$API_CONF" | jq -c '.')")

  if [[ -z "$API_RUN_ID" ]]; then
    fail "Failed to trigger api_streaming DAG — see output above"
  else
    pass "api_streaming DAG triggered (run_id: ${API_RUN_ID})"

    # 5c. Wait for check_api_health task
    info "Waiting 30s for check_api_health task..."
    sleep 30
    HEALTH_TASK=$(airflow_get "/dags/api_streaming/dagRuns/${API_RUN_ID}/taskInstances/check_api_health" \
      | jq -r '.state // "unknown"')
    if [[ "$HEALTH_TASK" == "success" ]]; then
      pass "check_api_health: success"
    elif [[ "$HEALTH_TASK" == "failed" || "$HEALTH_TASK" == "upstream_failed" ]]; then
      fail "check_api_health: ${HEALTH_TASK}"
      warn "The /api/ingest/stream endpoint is not implemented yet."
      warn "Expected until the endpoint is built (see api/routers/ for where to add it)."
      warn "All downstream streaming tasks will be skipped."
    else
      info "check_api_health state: ${HEALTH_TASK}"
    fi

    # 5d. Check run_api_mapping_stream task (only if health check passed)
    if [[ "$HEALTH_TASK" == "success" ]]; then
      sleep 15
      API_STREAM_TASK=$(airflow_get "/dags/api_streaming/dagRuns/${API_RUN_ID}/taskInstances/run_api_mapping_stream" \
        | jq -r '.state // "unknown"')
      if [[ "$API_STREAM_TASK" == "running" || "$API_STREAM_TASK" == "queued" ]]; then
        pass "run_api_mapping_stream is '${API_STREAM_TASK}' (continuous polling active)"
      else
        fail "run_api_mapping_stream state: ${API_STREAM_TASK}"
      fi

      # 5e. Wait for mapped/ output
      info "Waiting up to ${WAIT_SECS}s for api-sourced mapped/ objects..."
      if wait_for_minio_prefix "${BUCKET}" "mapped/" "${WAIT_SECS}"; then
        pass "mapped/ objects found — API ingestion pipeline is flowing"
      else
        warn "No mapped/ objects yet — check whether the API endpoint returns data"
      fi
    fi
  fi
fi

# =============================================================================
# SECTION 6: Streaming downstream pipeline test
# =============================================================================
if [[ "$SKIP_DOWNSTREAM" == "true" ]]; then
  warn "Skipping streaming_downstream test (--skip-downstream)"
else
  header "6. Streaming downstream pipeline"

  # 6a. Check whether mapped/ has any objects (needed for downstream to do anything)
  MAPPED_COUNT=$(mc ls "pulse/${BUCKET}/mapped/" 2>/dev/null | wc -l || echo 0)
  if [[ "$MAPPED_COUNT" -eq 0 ]]; then
    warn "No mapped/ objects in '${BUCKET}' — streaming_downstream will have nothing to process"
    warn "Run the DB or API mode test first to populate mapped/"
  fi

  # 6b. Manually trigger streaming_downstream (it normally runs on schedule)
  DS_CONF="{\"bucket\": \"${BUCKET}\"}"
  info "Triggering streaming_downstream DAG..."
  DS_RUN_ID=$(trigger_dag "streaming_downstream" "$DS_CONF")

  if [[ -z "$DS_RUN_ID" ]]; then
    fail "Failed to trigger streaming_downstream DAG"
  else
    pass "streaming_downstream triggered (run_id: ${DS_RUN_ID})"

    # 6c. Wait for all tasks to complete (clean → transform → analyze → ensure_specific → ml_infer → drift)
    DOWNSTREAM_TIMEOUT=$((WAIT_SECS * 3))   # give it 3× the standard wait
    if wait_for_dag_run "streaming_downstream" "${DS_RUN_ID}" "${DOWNSTREAM_TIMEOUT}"; then
      pass "streaming_downstream DAG run completed successfully"

      # 6d. Verify output at each stage
      for prefix in "cleaned/" "transformed/" "analytics/"; do
        COUNT=$(mc ls "pulse/${BUCKET}/${prefix}" 2>/dev/null | wc -l || echo 0)
        if [[ "$COUNT" -gt 0 ]]; then
          pass "  ${prefix}: ${COUNT} object(s) found"
        else
          warn "  ${prefix}: no objects — check if upstream stages produced output"
        fi
      done

      # 6e. Check specific model baselines saved
      BASELINE_COUNT=$(mc ls "pulse/${BUCKET}/models/drift_baselines/" 2>/dev/null | wc -l || echo 0)
      if [[ "$BASELINE_COUNT" -gt 0 ]]; then
        pass "  models/drift_baselines/: ${BASELINE_COUNT} baseline(s) saved"
      else
        info "  models/drift_baselines/: no baselines yet (specific model training may still be running)"
      fi
    else
      fail "streaming_downstream DAG run did NOT succeed within ${DOWNSTREAM_TIMEOUT}s"
      warn "Check task-level logs at ${AIRFLOW_URL}/dags/streaming_downstream/grid"

      # Show which tasks failed
      TASK_STATES=$(airflow_get "/dags/streaming_downstream/dagRuns/${DS_RUN_ID}/taskInstances" \
        | jq -r '.task_instances[]? | "\(.task_id): \(.state)"' 2>/dev/null || echo "  (could not fetch)")
      info "Task states:\n${TASK_STATES}"
    fi
  fi
fi

# =============================================================================
# SUMMARY
# =============================================================================
header "Test Summary"

if [[ $FAILURES -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}All checks passed.${RESET}"
else
  echo -e "${RED}${BOLD}${FAILURES} check(s) failed.${RESET}"
  echo ""
  echo "Useful debugging commands:"
  echo "  Airflow UI:            ${AIRFLOW_URL}"
  echo "  Airflow scheduler log: docker logs airflow-scheduler --tail 100 -f"
  echo "  Python container log:  docker logs python --tail 100"
  echo "  Debezium connectors:   curl http://localhost:8083/connectors"
  echo "  MinIO bucket contents: mc ls pulse/${BUCKET}/ --recursive"
  echo "  Kafka topics:          docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list"
fi

exit $FAILURES
