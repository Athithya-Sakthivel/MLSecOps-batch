#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

export PYTHONPATH="${REPO_ROOT}/src:${PYTHONPATH:-}"

REMOTE_PROJECT="${REMOTE_PROJECT:-flytesnacks}"
REMOTE_DOMAIN="${REMOTE_DOMAIN:-development}"
TASK_NAMESPACE="${TASK_NAMESPACE:-${REMOTE_PROJECT}-${REMOTE_DOMAIN}}"
SPARK_SERVICE_ACCOUNT="${SPARK_SERVICE_ACCOUNT:-spark}"

FLYTE_ADMIN_NAMESPACE="${FLYTE_ADMIN_NAMESPACE:-flyte}"
FLYTE_ADMIN_HOST="${FLYTE_ADMIN_HOST:-127.0.0.1}"
FLYTE_ADMIN_PORT="${FLYTE_ADMIN_PORT:-30081}"
PORT_FORWARD_TARGET_PORT="${PORT_FORWARD_TARGET_PORT:-81}"
PORT_FORWARD_PID_FILE="${PORT_FORWARD_PID_FILE:-/tmp/flyteadmin-portforward.pid}"
PORT_FORWARD_LOG="${PORT_FORWARD_LOG:-/tmp/flyteadmin-portforward.log}"

USE_PORT_FORWARD="${USE_PORT_FORWARD:-1}"

VENV_DIR="${VENV_DIR:-.venv_elt}"
if [[ ! -f "${VENV_DIR}/bin/activate" && -f ".venv/bin/activate" ]]; then
  VENV_DIR=".venv"
fi

log() { printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
fatal() { log "FATAL: $*"; exit 1; }

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"
}

activate_venv_if_present() {
  if [[ -f "${VENV_DIR}/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "${VENV_DIR}/bin/activate"
  else
    fatal "virtual environment not found: ${VENV_DIR}"
  fi
}

cleanup() {
  if [[ "${USE_PORT_FORWARD}" == "1" && -f "${PORT_FORWARD_PID_FILE}" ]]; then
    local old_pid
    old_pid="$(cat "${PORT_FORWARD_PID_FILE}")"
    if kill -0 "${old_pid}" >/dev/null 2>&1; then
      kill "${old_pid}" >/dev/null 2>&1 || true
      wait "${old_pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${PORT_FORWARD_PID_FILE}"
  fi
}
trap cleanup EXIT

start_port_forward() {
  [[ "${USE_PORT_FORWARD}" == "1" ]] || return 0

  if [[ -f "${PORT_FORWARD_PID_FILE}" ]]; then
    local old_pid
    old_pid="$(cat "${PORT_FORWARD_PID_FILE}")"
    if kill -0 "${old_pid}" >/dev/null 2>&1; then
      kill "${old_pid}" >/dev/null 2>&1 || true
      wait "${old_pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${PORT_FORWARD_PID_FILE}"
  fi

  nohup kubectl -n "${FLYTE_ADMIN_NAMESPACE}" port-forward "svc/flyteadmin" "${PORT_FORWARD_PORT}:${PORT_FORWARD_TARGET_PORT}" >"${PORT_FORWARD_LOG}" 2>&1 &
  echo $! > "${PORT_FORWARD_PID_FILE}"

  for _ in $(seq 1 60); do
    if (echo >"/dev/tcp/${FLYTE_ADMIN_HOST}/${PORT_FORWARD_PORT}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  fatal "flyteadmin port-forward did not become ready"
}

init_flytectl() {
  [[ "${USE_PORT_FORWARD}" == "1" ]] || return 0

  flytectl config init \
    --host="${FLYTE_ADMIN_HOST}:${PORT_FORWARD_PORT}" \
    --insecure \
    --force >/dev/null
}

flyte_exec_get() {
  local exec_id="$1"

  flytectl get execution "${exec_id}" \
    -p "${REMOTE_PROJECT}" \
    -d "${REMOTE_DOMAIN}" \
    --details || true
}

flyte_exec_yaml() {
  local exec_id="$1"

  flytectl get execution "${exec_id}" \
    -p "${REMOTE_PROJECT}" \
    -d "${REMOTE_DOMAIN}" \
    --details \
    -o yaml || true
}

list_matching_names() {
  local kind="$1"
  local exec_id="$2"

  kubectl get "${kind}" -n "${TASK_NAMESPACE}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | grep -F "${exec_id}" || true
}

list_matching_pods() {
  local exec_id="$1"

  local pods=""
  pods="$(kubectl get pods -n "${TASK_NAMESPACE}" -l "execution-id=${exec_id}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  if [[ -n "${pods}" ]]; then
    printf '%s' "${pods}"
    return 0
  fi

  list_matching_names "pods" "${exec_id}"
}

list_matching_sparkapplications() {
  local exec_id="$1"

  local apps=""
  apps="$(kubectl get sparkapplications -n "${TASK_NAMESPACE}" -l "execution-id=${exec_id}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  if [[ -n "${apps}" ]]; then
    printf '%s' "${apps}"
    return 0
  fi

  list_matching_names "sparkapplications" "${exec_id}"
}

print_pod_signal() {
  local pod="$1"

  echo "=== POD: ${pod} ==="
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o wide || true

  echo "--- SUMMARY ---"
  printf 'SERVICE_ACCOUNT='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{.spec.serviceAccountName}{"\n"}' 2>/dev/null || true
  printf 'IMAGE='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{.spec.containers[0].image}{"\n"}' 2>/dev/null || true
  printf 'IMAGE_PULL_SECRETS='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{.spec.imagePullSecrets[*].name}{"\n"}' 2>/dev/null || true
  printf 'ENV_FROM_SECRETS='
  kubectl get pod "${pod}" -n "${TASK_NAMESPACE}" -o jsonpath='{range .spec.containers[0].envFrom[*]}{.secretRef.name}{" "}{end}{"\n"}' 2>/dev/null || true

  echo "--- DESCRIBE ---"
  kubectl describe pod "${pod}" -n "${TASK_NAMESPACE}" || true

  echo "--- LOGS ---"
  kubectl logs "${pod}" -n "${TASK_NAMESPACE}" --all-containers=true --tail=300 || true
}

print_sparkapplication_signal() {
  local app="$1"

  echo "=== SPARKAPPLICATION: ${app} ==="
  kubectl get sparkapplication "${app}" -n "${TASK_NAMESPACE}" -o wide || true

  echo "--- YAML ---"
  kubectl get sparkapplication "${app}" -n "${TASK_NAMESPACE}" -o yaml || true

  echo "--- RELATED PODS ---"
  local pods=""
  pods="$(kubectl get pods -n "${TASK_NAMESPACE}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -F "${app}" || true)"
  if [[ -n "${pods}" ]]; then
    printf '%s\n' "${pods}"
  else
    echo "No pod names matched ${app}"
  fi
}

diagnose_execution() {
  local exec_id="$1"

  start_port_forward
  init_flytectl

  echo "=== EXECUTION TREE ==="
  flyte_exec_get "${exec_id}"

  echo "=== EXECUTION YAML ==="
  flyte_exec_yaml "${exec_id}"

  echo "=== NAMESPACE PODS ==="
  kubectl get pods -n "${TASK_NAMESPACE}" -o wide || true

  echo "=== NAMESPACE SPARKAPPLICATIONS ==="
  kubectl get sparkapplications -n "${TASK_NAMESPACE}" -o wide || true

  local apps pods
  apps="$(list_matching_sparkapplications "${exec_id}")"
  if [[ -n "${apps}" ]]; then
    while IFS= read -r app; do
      [[ -n "${app}" ]] || continue
      print_sparkapplication_signal "${app}"
    done <<< "${apps}"
  else
    echo "No SparkApplication matched execution ${exec_id}"
  fi

  pods="$(list_matching_pods "${exec_id}")"
  if [[ -n "${pods}" ]]; then
    while IFS= read -r pod; do
      [[ -n "${pod}" ]] || continue
      print_pod_signal "${pod}"
    done <<< "${pods}"
  else
    echo "No pod matched execution ${exec_id}"
  fi

  echo "=== NAMESPACE EVENTS (focused) ==="
  kubectl get events -n "${TASK_NAMESPACE}" --sort-by=.lastTimestamp \
    | grep -E "${exec_id}|Warning|Failed|BackOff|ErrImagePull|ImagePullBackOff|CrashLoopBackOff|OOMKilled|Unschedulable|CreateContainerConfigError|FailedMount" \
    || true

  echo "=== CLUSTER EVENTS (focused) ==="
  kubectl get events -A --sort-by=.lastTimestamp \
    | grep -E "${exec_id}|Warning|Failed|BackOff|ErrImagePull|ImagePullBackOff|CrashLoopBackOff|OOMKilled|Unschedulable|CreateContainerConfigError|FailedMount" \
    || true
}

delete_runtime_resources() {
  local exec_id="$1"
  local failures=0

  log "Deleting SparkApplication resources for execution ${exec_id}"
  if ! kubectl delete sparkapplication -n "${TASK_NAMESPACE}" -l "execution-id=${exec_id}" --ignore-not-found=true; then
    failures=1
  fi

  log "Deleting pods for execution ${exec_id}"
  if ! kubectl delete pod -n "${TASK_NAMESPACE}" -l "execution-id=${exec_id}" --ignore-not-found=true; then
    failures=1
  fi

  local apps pods
  apps="$(list_matching_sparkapplications "${exec_id}")"
  if [[ -n "${apps}" ]]; then
    while IFS= read -r app; do
      [[ -n "${app}" ]] || continue
      log "Deleting leftover SparkApplication ${app}"
      if ! kubectl delete sparkapplication "${app}" -n "${TASK_NAMESPACE}" --ignore-not-found=true; then
        failures=1
      fi
    done <<< "${apps}"
  fi

  pods="$(list_matching_pods "${exec_id}")"
  if [[ -n "${pods}" ]]; then
    while IFS= read -r pod; do
      [[ -n "${pod}" ]] || continue
      log "Deleting leftover pod ${pod}"
      if ! kubectl delete pod "${pod}" -n "${TASK_NAMESPACE}" --ignore-not-found=true; then
        failures=1
      fi
    done <<< "${pods}"
  fi

  return "${failures}"
}

delete_execution() {
  local exec_id="$1"
  [[ -n "${exec_id}" ]] || fatal "execution id is required for delete"

  start_port_forward
  init_flytectl

  local failures=0

  if ! delete_runtime_resources "${exec_id}"; then
    failures=1
  fi

  log "Deleting Flyte execution ${exec_id}"
  if ! flytectl delete execution "${exec_id}" -p "${REMOTE_PROJECT}" -d "${REMOTE_DOMAIN}"; then
    log "WARNING: flytectl delete execution returned non-zero for ${exec_id}"
    failures=1
  fi

  return "${failures}"
}

usage() {
  cat <<EOF
Usage:
  $0 --diagnose <exec_id>
  $0 --delete <exec_id>

Optional environment variables:
  REMOTE_PROJECT=${REMOTE_PROJECT}
  REMOTE_DOMAIN=${REMOTE_DOMAIN}
  TASK_NAMESPACE=${TASK_NAMESPACE}
  SPARK_SERVICE_ACCOUNT=${SPARK_SERVICE_ACCOUNT}
  FLYTE_ADMIN_NAMESPACE=${FLYTE_ADMIN_NAMESPACE}
  FLYTE_ADMIN_HOST=${FLYTE_ADMIN_HOST}
  FLYTE_ADMIN_PORT=${FLYTE_ADMIN_PORT}
  PORT_FORWARD_TARGET_PORT=${PORT_FORWARD_TARGET_PORT}
  USE_PORT_FORWARD=${USE_PORT_FORWARD}
  VENV_DIR=${VENV_DIR}
EOF
}

main() {
  activate_venv_if_present

  require_bin kubectl
  require_bin flytectl
  require_bin git

  case "${1:-}" in
    --diagnose)
      [[ $# -ge 2 ]] || fatal "--diagnose requires an execution id"
      shift
      diagnose_execution "$1"
      ;;
    --delete)
      [[ $# -ge 2 ]] || fatal "--delete requires an execution id"
      shift
      delete_execution "$1"
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      fatal "unknown command: ${1}"
      ;;
  esac
}

main "$@"