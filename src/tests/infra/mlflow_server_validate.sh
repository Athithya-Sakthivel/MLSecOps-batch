#!/usr/bin/env bash
set -euo pipefail

TARGET_NS="${TARGET_NS:-mlflow}"
SERVICE_NAME="${MLFLOW_SERVICE:-mlflow}"
REMOTE_PORT="${MLFLOW_PORT:-5000}"
LOCAL_PORT="${LOCAL_PORT:-18080}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
POLL_SECONDS="${POLL_SECONDS:-2}"
EXPERIMENT_NAME="${EXPERIMENT_NAME:-mlflow-server-validate}"
RUN_NAME="${RUN_NAME:-mlflow-server-validate-$(date -u +%Y%m%dT%H%M%SZ)}"
BASE_URL="http://127.0.0.1:${LOCAL_PORT}"

PF_LOG="$(mktemp)"
PF_PID=""

log() {
  printf '[%s] [mlflow-validate] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  printf '[%s] [mlflow-validate][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
  exit 1
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"
}

cleanup() {
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" >/dev/null 2>&1; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
    wait "${PF_PID}" >/dev/null 2>&1 || true
  fi

  if [[ -f "${PF_LOG}" ]]; then
    log "port-forward log:"
    sed 's/^/[pf] /' "${PF_LOG}" >&2 || true
  fi
}
trap cleanup EXIT

start_port_forward() {
  : > "${PF_LOG}"
  log "starting port-forward ${LOCAL_PORT} -> ${REMOTE_PORT}"
  kubectl -n "${TARGET_NS}" port-forward "svc/${SERVICE_NAME}" "${LOCAL_PORT}:${REMOTE_PORT}" >"${PF_LOG}" 2>&1 &
  PF_PID="$!"
}

wait_for_rollout() {
  log "waiting for deployment rollout"
  kubectl -n "${TARGET_NS}" rollout status "deployment/${SERVICE_NAME}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null
}

wait_for_http() {
  local start now elapsed
  start="$(date +%s)"

  while true; do
    if python3 - <<PY >/dev/null 2>&1
from urllib.request import urlopen
urlopen("${BASE_URL}/", timeout=3).read()
PY
    then
      log "HTTP endpoint is reachable at ${BASE_URL}"
      return 0
    fi

    now="$(date +%s)"
    elapsed="$((now - start))"
    if [[ "${elapsed}" -ge "${TIMEOUT_SECONDS}" ]]; then
      fatal "timed out waiting for ${BASE_URL}"
    fi

    if ! kill -0 "${PF_PID}" >/dev/null 2>&1; then
      fatal "port-forward process exited unexpectedly"
    fi

    sleep "${POLL_SECONDS}"
  done
}

run_smoke_test() {
  python3 - "${BASE_URL}" "${EXPERIMENT_NAME}" "${RUN_NAME}" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

base_url, experiment_name, run_name = sys.argv[1:4]

def req(method, path, payload=None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base_url + path, data=data, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=15) as response:
        return response.status, response.read().decode("utf-8")

def get_json(method, path, payload=None):
    _, body = req(method, path, payload)
    return json.loads(body)

# Basic server reachability.
status, body = req("GET", "/")
if status != 200:
    raise SystemExit(f"root endpoint returned {status}")
print(f"[mlflow-validate] root endpoint ok ({len(body)} bytes)")

# Experiment: get-or-create.
try:
    data = get_json(
        "GET",
        "/api/2.0/mlflow/experiments/get-by-name?"
        + urllib.parse.urlencode({"experiment_name": experiment_name}),
    )
    exp_id = data["experiment"]["experiment_id"]
    print(f"[mlflow-validate] reusing experiment {experiment_name} id={exp_id}")
except urllib.error.HTTPError as exc:
    if exc.code != 404:
        raise
    data = get_json("POST", "/api/2.0/mlflow/experiments/create", {"name": experiment_name})
    exp_id = data["experiment_id"]
    print(f"[mlflow-validate] created experiment {experiment_name} id={exp_id}")

start_time = int(time.time() * 1000)

# Create run and write small tracking data.
data = get_json(
    "POST",
    "/api/2.0/mlflow/runs/create",
    {
        "experiment_id": exp_id,
        "start_time": start_time,
        "tags": [
            {"key": "mlflow.runName", "value": run_name},
            {"key": "validation", "value": "true"},
        ],
    },
)
run_id = data["run"]["info"]["run_id"]
print(f"[mlflow-validate] created run_id={run_id}")

get_json(
    "POST",
    "/api/2.0/mlflow/runs/log-parameter",
    {"run_id": run_id, "key": "validation_param", "value": "ok"},
)
get_json(
    "POST",
    "/api/2.0/mlflow/runs/log-metric",
    {
        "run_id": run_id,
        "key": "validation_metric",
        "value": 1.0,
        "timestamp": start_time,
        "step": 0,
    },
)

data = get_json("GET", "/api/2.0/mlflow/runs/get?" + urllib.parse.urlencode({"run_id": run_id}))
params = data["run"]["data"]["params"]
metrics = data["run"]["data"]["metrics"]

if params.get("validation_param") != "ok":
    raise SystemExit("parameter validation failed")
if float(metrics.get("validation_metric", -1)) != 1.0:
    raise SystemExit("metric validation failed")

print("[mlflow-validate] smoke test passed")
PY
}

main() {
  require_bin kubectl
  require_bin python3

  log "waiting for mlflow deployment in namespace ${TARGET_NS}"
  wait_for_rollout

  start_port_forward
  wait_for_http
  run_smoke_test

  log "validation succeeded"
}

main "$@"