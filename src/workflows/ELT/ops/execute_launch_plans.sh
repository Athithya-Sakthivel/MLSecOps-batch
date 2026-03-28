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
USE_LATEST="${USE_LATEST:-0}"
LAUNCH_PLAN_NAME="${LAUNCH_PLAN_NAME:-}"
LAUNCH_PLAN_VERSION="${LAUNCH_PLAN_VERSION:-$(git rev-parse HEAD 2>/dev/null || echo latest)}"
TARGET="${TARGET:-elt}"

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

resolve_launch_plan_name() {
  if [[ -n "${LAUNCH_PLAN_NAME}" ]]; then
    printf '%s' "${LAUNCH_PLAN_NAME}"
    return 0
  fi

  case "${TARGET}" in
    elt)
      printf '%s' "elt_workflow_lp"
      ;;
    maintenance-daily)
      printf '%s' "iceberg_maintenance_daily_lp"
      ;;
    maintenance-weekly)
      printf '%s' "iceberg_maintenance_weekly_lp"
      ;;
    *)
      fatal "unknown TARGET=${TARGET}; use elt, maintenance-daily, maintenance-weekly, or pass --launchplan"
      ;;
  esac
}

require_preflight() {
  kubectl get namespace "${TASK_NAMESPACE}" >/dev/null

  kubectl get serviceaccount "${SPARK_SERVICE_ACCOUNT}" -n "${TASK_NAMESPACE}" >/dev/null

  if ! kubectl auth can-i create pods -n "${TASK_NAMESPACE}" \
    --as="system:serviceaccount:${TASK_NAMESPACE}:${SPARK_SERVICE_ACCOUNT}" >/dev/null 2>&1; then
    fatal "service account ${SPARK_SERVICE_ACCOUNT} cannot create pods in namespace ${TASK_NAMESPACE}"
  fi

  kubectl get resourcequota -n "${TASK_NAMESPACE}" -o wide >/dev/null 2>&1 || true
  log "Preflight OK for namespace=${TASK_NAMESPACE} serviceaccount=${SPARK_SERVICE_ACCOUNT}"
}

fetch_launch_plan_exec_spec() {
  local launch_plan_name="$1"
  local exec_spec_file="$2"

  if [[ "${USE_LATEST}" == "1" ]]; then
    flytectl get launchplan \
      -p "${REMOTE_PROJECT}" \
      -d "${REMOTE_DOMAIN}" \
      "${launch_plan_name}" \
      --latest \
      --execFile "${exec_spec_file}"
  else
    flytectl get launchplan \
      -p "${REMOTE_PROJECT}" \
      -d "${REMOTE_DOMAIN}" \
      "${launch_plan_name}" \
      --version "${LAUNCH_PLAN_VERSION}" \
      --execFile "${exec_spec_file}"
  fi
}

create_execution_from_spec() {
  local exec_spec_file="$1"

  flytectl create execution \
    -p "${REMOTE_PROJECT}" \
    -d "${REMOTE_DOMAIN}" \
    --execFile "${exec_spec_file}"
}

usage() {
  cat <<EOF
Usage:
  $0 [--target elt|maintenance-daily|maintenance-weekly]
  $0 --launchplan <launch_plan_name> [--version <version>] [--latest]

Optional environment variables:
  REMOTE_PROJECT=${REMOTE_PROJECT}
  REMOTE_DOMAIN=${REMOTE_DOMAIN}
  TASK_NAMESPACE=${TASK_NAMESPACE}
  SPARK_SERVICE_ACCOUNT=${SPARK_SERVICE_ACCOUNT}
  FLYTE_ADMIN_NAMESPACE=${FLYTE_ADMIN_NAMESPACE}
  FLYTE_ADMIN_HOST=${FLYTE_ADMIN_HOST}
  FLYTE_ADMIN_PORT=${FLYTE_ADMIN_PORT}
  USE_PORT_FORWARD=${USE_PORT_FORWARD}
  USE_LATEST=${USE_LATEST}
  LAUNCH_PLAN_NAME=${LAUNCH_PLAN_NAME}
  LAUNCH_PLAN_VERSION=${LAUNCH_PLAN_VERSION}
  TARGET=${TARGET}
  VENV_DIR=${VENV_DIR}
EOF
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --launchplan|-l)
        [[ $# -ge 2 ]] || fatal "--launchplan requires a name"
        LAUNCH_PLAN_NAME="$2"
        shift 2
        ;;
      --version|-v)
        [[ $# -ge 2 ]] || fatal "--version requires a version"
        LAUNCH_PLAN_VERSION="$2"
        shift 2
        ;;
      --latest)
        USE_LATEST=1
        shift
        ;;
      --target|-t)
        [[ $# -ge 2 ]] || fatal "--target requires one of: elt, maintenance-daily, maintenance-weekly"
        TARGET="$2"
        shift 2
        ;;
      -h|--help|help)
        usage
        exit 0
        ;;
      *)
        fatal "unknown argument: $1"
        ;;
    esac
  done

  activate_venv_if_present

  require_bin kubectl
  require_bin flytectl
  require_bin git

  start_port_forward
  init_flytectl
  require_preflight

  local launch_plan_name exec_spec_file
  launch_plan_name="$(resolve_launch_plan_name)"
  exec_spec_file="$(mktemp "/tmp/${launch_plan_name}.XXXXXX.yaml")"

  log "Fetching launch plan ${launch_plan_name} for project=${REMOTE_PROJECT} domain=${REMOTE_DOMAIN}"
  fetch_launch_plan_exec_spec "${launch_plan_name}" "${exec_spec_file}"

  log "Creating execution from ${exec_spec_file}"
  create_execution_from_spec "${exec_spec_file}"

  rm -f "${exec_spec_file}"
}

main "$@"