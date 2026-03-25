#!/usr/bin/env bash
# Establishes a local port-forward to Flyte Admin and ensures connectivity before submission
# Derives a deterministic execution name using git SHA (7 chars) and current timestamp
# Initializes Flyte CLI configuration against the forwarded admin endpoint
# Activates the Python virtual environment to ensure correct runtime dependencies
# Submits the specified Flyte workflow remotely with a reproducible execution identifier

set -euo pipefail

GHCR_USER="${GHCR_USER:-athithya-sakthivel}"
IMAGE_TAG="${IMAGE_TAG:-1.0.8}"

export ELT_TASK_IMAGE="${ELT_TASK_IMAGE:-ghcr.io/${GHCR_USER}/flyte-elt-task:${IMAGE_TAG}}"
export PYTHONPATH="/workspace/src:${PYTHONPATH:-}"

PORT_FORWARD_PID_FILE=/tmp/flyteadmin-portforward.pid
PORT_FORWARD_LOG=/tmp/flyteadmin-portforward.log
PORT_FORWARD_HOST=127.0.0.1
PORT_FORWARD_PORT=30081
REMOTE_PROJECT=flytesnacks
REMOTE_DOMAIN=development
WORKFLOW_FILE=src/workflows/ELT/workflows/elt_workflow.py
WORKFLOW_NAME=elt_workflow

cleanup() {
  if [[ -f "${PORT_FORWARD_PID_FILE}" ]]; then
    old_pid="$(cat "${PORT_FORWARD_PID_FILE}")"
    if kill -0 "${old_pid}" >/dev/null 2>&1; then
      kill "${old_pid}" >/dev/null 2>&1 || true
      wait "${old_pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${PORT_FORWARD_PID_FILE}"
  fi
}
trap cleanup EXIT

if [[ -f "${PORT_FORWARD_PID_FILE}" ]]; then
  old_pid="$(cat "${PORT_FORWARD_PID_FILE}")"
  if kill -0 "${old_pid}" >/dev/null 2>&1; then
    kill "${old_pid}" >/dev/null 2>&1 || true
    wait "${old_pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${PORT_FORWARD_PID_FILE}"
fi

nohup kubectl -n flyte port-forward svc/flyteadmin "${PORT_FORWARD_PORT}:81" >"${PORT_FORWARD_LOG}" 2>&1 &
echo $! > "${PORT_FORWARD_PID_FILE}"

for _ in $(seq 1 60); do
  if (echo >"/dev/tcp/${PORT_FORWARD_HOST}/${PORT_FORWARD_PORT}") >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! (echo >"/dev/tcp/${PORT_FORWARD_HOST}/${PORT_FORWARD_PORT}") >/dev/null 2>&1; then
  echo "flyteadmin port-forward did not become ready" >&2
  exit 1
fi

SHORT_SHA="nogit"
if [[ -d .git ]]; then
  SHORT_SHA="$(git rev-parse --short=7 HEAD)"
  echo "Submitting workflow from commit $(git rev-parse HEAD)"
fi

TIMESTAMP="$(date -u +%Y-%m-%d-%H%M%S)"
EXEC_NAME="${SHORT_SHA}-${TIMESTAMP}"

echo "Execution name: ${EXEC_NAME}"

source .venv/bin/activate

flytectl config init --host="${PORT_FORWARD_HOST}:${PORT_FORWARD_PORT}" --insecure --force >/dev/null

pyflyte run --remote \
  -p "${REMOTE_PROJECT}" \
  -d "${REMOTE_DOMAIN}" \
  "${WORKFLOW_FILE}" \
  "${WORKFLOW_NAME}" \
  --name "${EXEC_NAME}"