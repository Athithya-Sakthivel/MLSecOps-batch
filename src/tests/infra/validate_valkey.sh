#!/usr/bin/env bash
# Bulletproof Valkey Validation Script
# Fixes: Explicitly passes VALKEY_HOST to pods and uses -h flag in CLI commands.

set -euo pipefail

# --- Configuration ---
NAMESPACE="${NAMESPACE:-valkey-prod}"
CLIENT_SVC="${CLIENT_SVC:-valkey}"
SECRET_NAME="${SECRET_NAME:-valkey-auth}"
VALKEY_PORT="${VALKEY_PORT:-6379}"
TEST_IMAGE="${TEST_IMAGE:-valkey/valkey:9.0.3-alpine}"
TEST_TIMEOUT="${TEST_TIMEOUT:-120}"

# --- State Tracking ---
declare -A TEST_RESULTS
declare -A TEST_DURATIONS
total_passed=0
total_failed=0

# --- Logging ---
timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log_info()  { printf '[%s] [INFO] %s\n' "$(timestamp)" "$*"; }
log_step()  { printf '[%s] [STEP] %s\n' "$(timestamp)" "$*"; }
log_success(){ printf '[%s] [SUCCESS] %s\n' "$(timestamp)" "$*"; }
log_error() { printf '[%s] [ERROR] %s\n' "$(timestamp)" "$*" >&2; }
log_raw()   { while IFS= read -r line; do printf '[%s] [RAW] %s\n' "$(timestamp)" "$line"; done; }

# --- Get Credentials ---
get_credentials() {
  log_step "Retrieving Valkey password from secret '${SECRET_NAME}'"
  
  if ! kubectl -n "${NAMESPACE}" get secret "${SECRET_NAME}" >/dev/null 2>&1; then
    log_error "Secret '${SECRET_NAME}' not found in namespace '${NAMESPACE}'"
    return 1
  fi
  
  export VALKEY_PASSWORD
  VALKEY_PASSWORD=$(kubectl -n "${NAMESPACE}" get secret "${SECRET_NAME}" -o jsonpath='{.data.VALKEY_PASSWORD}' | base64 -d)
  
  # CRITICAL FIX: Define the full DNS name explicitly
  export VALKEY_HOST="${CLIENT_SVC}.${NAMESPACE}.svc.cluster.local"
  
  log_info "Credentials loaded: Host='${VALKEY_HOST}', Port='${VALKEY_PORT}'"
}

# --- Single Test Execution ---
run_valkey_test() {
  local test_name="$1"
  local cli_command="$2"
  
  local pod_name="val-test-${test_name//_/-}-$RANDOM"
  local start_time end_time duration
  
  log_step "Running test: ${test_name}"
  start_time=$(date +%s)
  
  # CRITICAL FIX: Pass BOTH VALKEY_PASSWORD and VALKEY_HOST into the pod env
  if ! kubectl run "${pod_name}" \
      -n "${NAMESPACE}" \
      --restart=Never \
      --image="${TEST_IMAGE}" \
      --env="VALKEY_PASSWORD=${VALKEY_PASSWORD}" \
      --env="VALKEY_HOST=${VALKEY_HOST}" \
      --env="VALKEY_PORT=${VALKEY_PORT}" \
      --command -- sh -c "${cli_command}" > "/tmp/val_${pod_name}.log" 2>&1; then
    
    log_error "Failed to launch pod '${pod_name}'"
    cat "/tmp/val_${pod_name}.log" 2>/dev/null | log_raw || true
    ((total_failed++))
    TEST_RESULTS["${test_name}"]="FAIL"
    rm -f "/tmp/val_${pod_name}.log"
    return 1
  fi
  
  # Wait for Completion
  local elapsed=0
  local pod_status=""
  
  while [[ $elapsed -lt ${TEST_TIMEOUT} ]]; do
    pod_status=$(kubectl -n "${NAMESPACE}" get pod "${pod_name}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    
    if [[ "${pod_status}" == "Succeeded" ]]; then
      break
    elif [[ "${pod_status}" == "Failed" || "${pod_status}" == "Error" ]]; then
      log_error "Pod '${pod_name}' failed (Status: ${pod_status})"
      kubectl -n "${NAMESPACE}" logs "${pod_name}" --tail=50 | log_raw
      ((total_failed++))
      TEST_RESULTS["${test_name}"]="FAIL"
      kubectl -n "${NAMESPACE}" delete pod "${pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
      rm -f "/tmp/val_${pod_name}.log"
      return 1
    fi
    
    sleep 2
    ((elapsed+=2))
  done
  
  if [[ "${pod_status}" != "Succeeded" ]]; then
    log_error "Timeout waiting for pod '${pod_name}' (Status: ${pod_status})"
    kubectl -n "${NAMESPACE}" logs "${pod_name}" --tail=50 | log_raw
    ((total_failed++))
    TEST_RESULTS["${test_name}"]="FAIL"
    kubectl -n "${NAMESPACE}" delete pod "${pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    rm -f "/tmp/val_${pod_name}.log"
    return 1
  fi
  
  end_time=$(date +%s)
  duration=$((end_time - start_time))
  
  log_success "Test '${test_name}' PASSED (duration: ${duration}s)"
  cat "/tmp/val_${pod_name}.log" 2>/dev/null | log_raw || true
  
  ((total_passed++))
  TEST_RESULTS["${test_name}"]="PASS"
  TEST_DURATIONS["${test_name}"]="${duration}"
  
  kubectl -n "${NAMESPACE}" delete pod "${pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  rm -f "/tmp/val_${pod_name}.log"
  return 0
}

# --- Summary Report ---
print_summary() {
  echo ""
  echo "=================================================================="
  echo "                 VALKEY VALIDATION SUMMARY                        "
  echo "=================================================================="
  printf "%-25s | %-8s | %-10s\n" "TEST NAME" "STATUS" "DURATION"
  echo "-------------------------|----------|------------"
  
  for test_name in "${!TEST_RESULTS[@]}"; do
    local status="${TEST_RESULTS[$test_name]}"
    local duration="${TEST_DURATIONS[$test_name]:-N/A}"
    printf "%-25s | %-8s | %-10s\n" "${test_name}" "${status}" "${duration}s"
  done
  
  echo "=================================================================="
  printf "Total Passed: %d | Total Failed: %d\n" "${total_passed}" "${total_failed}"
  echo "=================================================================="
  
  if [[ ${total_failed} -eq 0 ]]; then
    log_success "All validation tests passed successfully."
    return 0
  else
    log_error "Validation failed. Check logs above."
    return 1
  fi
}

# --- Main Workflow ---
main() {
  command -v kubectl >/dev/null 2>&1 || { log_error "kubectl not found in PATH"; exit 1; }
  
  log_info "Starting Valkey Validation Workflow"
  log_info "Target: Service='${CLIENT_SVC}', Namespace='${NAMESPACE}'"
  
  # Pre-flight checks
  if ! kubectl cluster-info >/dev/null 2>&1; then
    log_error "Cannot connect to Kubernetes cluster."
    exit 1
  fi
  
  if ! kubectl -n "${NAMESPACE}" get svc "${CLIENT_SVC}" >/dev/null 2>&1; then
    log_error "Service '${CLIENT_SVC}' not found in namespace '${NAMESPACE}'."
    exit 1
  fi
  
  get_credentials || exit 1
  
  # Define Tests
  # CRITICAL FIX: All commands explicitly use -h "\${VALKEY_HOST}"
  
  local cmd_auth_ping="valkey-cli -h \"\${VALKEY_HOST}\" -p \"\${VALKEY_PORT}\" -a \"\${VALKEY_PASSWORD}\" --no-auth-warning ping"
  
  local cmd_auth_check="valkey-cli -h \"\${VALKEY_HOST}\" -p \"\${VALKEY_PORT}\" ping 2>&1 | grep -q 'NOAUTH' && echo 'AUTH_REQUIRED_OK' || echo 'AUTH_CHECK_FAILED'"
  
  local cmd_crud_setget="valkey-cli -h \"\${VALKEY_HOST}\" -p \"\${VALKEY_PORT}\" -a \"\${VALKEY_PASSWORD}\" --no-auth-warning SET validate_key success_value && valkey-cli -h \"\${VALKEY_HOST}\" -p \"\${VALKEY_PORT}\" -a \"\${VALKEY_PASSWORD}\" --no-auth-warning GET validate_key"
  
  local cmd_crud_del="valkey-cli -h \"\${VALKEY_HOST}\" -p \"\${VALKEY_PORT}\" -a \"\${VALKEY_PASSWORD}\" --no-auth-warning DEL validate_key"

  # Run Tests
  run_valkey_test "auth_ping" "${cmd_auth_ping}" || true
  run_valkey_test "auth_enforcement" "${cmd_auth_check}" || true
  run_valkey_test "crud_set_get" "${cmd_crud_setget}" || true
  run_valkey_test "crud_delete" "${cmd_crud_del}" || true
  
  print_summary
}

case "${1:-}" in
  --run) main ;;
  --help|-h)
    echo "Usage: $0 [--run]"
    echo "Env vars: NAMESPACE, CLIENT_SVC, SECRET_NAME, TEST_TIMEOUT"
    exit 0 ;;
  *) main ;;
esac