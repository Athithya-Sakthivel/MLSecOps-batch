#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

export PYTHONPATH="${REPO_ROOT}/src:${PYTHONPATH:-}"
export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-27-09-56--e4e99ab"

REMOTE_PROJECT="${REMOTE_PROJECT:-flytesnacks}"
REMOTE_DOMAIN="${REMOTE_DOMAIN:-development}"
TASK_NAMESPACE="${TASK_NAMESPACE:-${REMOTE_PROJECT}-${REMOTE_DOMAIN}}"

K8S_CLUSTER="${K8S_CLUSTER:-kind}"
if [[ -z "${ELT_PROFILE:-}" ]]; then
  case "${K8S_CLUSTER}" in
    kind|minikube|docker-desktop|local) ELT_PROFILE="dev" ;;
    *) ELT_PROFILE="prod" ;;
  esac
fi

SPARK_SERVICE_ACCOUNT="${SPARK_SERVICE_ACCOUNT:-spark}"
WORKFLOW_MODULE="${WORKFLOW_MODULE:-workflows.ELT.launch_plans}"
WORKFLOW_SOURCE_FILE="${WORKFLOW_SOURCE_FILE:-src/workflows/ELT/launch_plans.py}"

RAW_OUTPUT_DATA_PREFIX="${RAW_OUTPUT_DATA_PREFIX:-}"

USE_PORT_FORWARD="${USE_PORT_FORWARD:-1}"
FLYTE_ADMIN_NAMESPACE="${FLYTE_ADMIN_NAMESPACE:-flyte}"
FLYTE_ADMIN_HOST="${FLYTE_ADMIN_HOST:-127.0.0.1}"
FLYTE_ADMIN_PORT="${FLYTE_ADMIN_PORT:-30081}"
PORT_FORWARD_TARGET_PORT="${PORT_FORWARD_TARGET_PORT:-81}"
PORT_FORWARD_PID_FILE="${PORT_FORWARD_PID_FILE:-/tmp/flyteadmin-portforward.pid}"
PORT_FORWARD_LOG="${PORT_FORWARD_LOG:-/tmp/flyteadmin-portforward.log}"

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

lint_sources() {
  require_bin ruff
  log "Running ruff on src/workflows/ELT"
  ruff check src/workflows/ELT --fix
}

import_check() {
  python - <<'PY'
import importlib
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "src"))
importlib.import_module("workflows.ELT.launch_plans")
print("import_ok")
PY
}

bootstrap_k8s() {
  log "Applying namespace and Spark RBAC bootstrap for ${TASK_NAMESPACE}"

  kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${TASK_NAMESPACE}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SPARK_SERVICE_ACCOUNT}
  namespace: ${TASK_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${SPARK_SERVICE_ACCOUNT}
  namespace: ${TASK_NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log", "services", "configmaps", "events"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${SPARK_SERVICE_ACCOUNT}
  namespace: ${TASK_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: ${SPARK_SERVICE_ACCOUNT}
    namespace: ${TASK_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${SPARK_SERVICE_ACCOUNT}
EOF

  if kubectl get resourcequota -n "${TASK_NAMESPACE}" >/dev/null 2>&1; then
    log "Existing ResourceQuota objects in ${TASK_NAMESPACE}:"
    kubectl get resourcequota -n "${TASK_NAMESPACE}" -o wide || true
  else
    log "WARNING: no ResourceQuota found in ${TASK_NAMESPACE}"
  fi

  if ! kubectl auth can-i create pods -n "${TASK_NAMESPACE}" \
      --as="system:serviceaccount:${TASK_NAMESPACE}:${SPARK_SERVICE_ACCOUNT}" >/dev/null 2>&1; then
    fatal "service account ${SPARK_SERVICE_ACCOUNT} cannot create pods in ${TASK_NAMESPACE}"
  fi

  log "Bootstrap verified for ${TASK_NAMESPACE}"
}

register_entities() {
  if [[ ! -f "${WORKFLOW_SOURCE_FILE}" ]]; then
    fatal "workflow source file not found: ${WORKFLOW_SOURCE_FILE}"
  fi

  local git_sha short_sha
  git_sha="$(git rev-parse HEAD)"
  short_sha="$(git rev-parse --short=12 HEAD)"

  log "Registering ELT from commit ${git_sha}"
  log "Workflow module: ${WORKFLOW_MODULE}"
  log "Profile: ${ELT_PROFILE} | Cluster: ${K8S_CLUSTER} | Namespace: ${TASK_NAMESPACE}"

  local -a cmd=(
    pyflyte register
    --project "${REMOTE_PROJECT}"
    --domain "${REMOTE_DOMAIN}"
    --image "${ELT_TASK_IMAGE}"
    --version "${git_sha}"
    --service-account "${SPARK_SERVICE_ACCOUNT}"
    --activate-launchplans
    --env "ELT_PROFILE=${ELT_PROFILE}"
    --env "K8S_CLUSTER=${K8S_CLUSTER}"
  )

  if [[ -n "${RAW_OUTPUT_DATA_PREFIX}" ]]; then
    cmd+=(--raw-data-prefix "${RAW_OUTPUT_DATA_PREFIX}")
  fi

  cmd+=("${WORKFLOW_MODULE}")

  "${cmd[@]}"

  log "Registration complete for version ${short_sha}"
}

main() {
  activate_venv_if_present

  require_bin kubectl
  require_bin pyflyte
  require_bin git
  if [[ "${USE_PORT_FORWARD}" == "1" ]]; then
    require_bin flytectl
  fi

  bootstrap_k8s
  lint_sources
  import_check
  start_port_forward
  init_flytectl
  register_entities
}

main "$@"