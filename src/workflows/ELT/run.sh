#!/usr/bin/env bash
set -euo pipefail

export ELT_TASK_IMAGE=ghcr.io/athithya-sakthivel/flyte-elt-spark-base@sha256:04ecd3631080e8627f0ae9308697ca0d96250c0b07a0b3a147fdcc22276585e5
export PYTHONPATH=/workspace/src:${PYTHONPATH:-}

PORT_FORWARD_PID_FILE=/tmp/flyteadmin-portforward.pid
PORT_FORWARD_LOG=/tmp/flyteadmin-portforward.log
PORT_FORWARD_HOST=127.0.0.1
PORT_FORWARD_PORT=30081
REMOTE_PROJECT=flytesnacks
REMOTE_DOMAIN=development
WORKFLOW_FILE=src/workflows/ELT/workflows/elt_workflow.py
WORKFLOW_NAME=elt_workflow

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

source .venv/bin/activate

flytectl config init --host="${PORT_FORWARD_HOST}:${PORT_FORWARD_PORT}" --insecure --force >/dev/null

pyflyte run --remote -p "${REMOTE_PROJECT}" -d "${REMOTE_DOMAIN}" "${WORKFLOW_FILE}" "${WORKFLOW_NAME}"