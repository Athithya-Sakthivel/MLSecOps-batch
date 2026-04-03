#!/usr/bin/env bash
set -euo pipefail

TARGET_NS="${TARGET_NS:-mlflow}"
SERVICE_NAME="${MLFLOW_SERVICE:-mlflow}"
DEPLOYMENT_NAME="${MLFLOW_DEPLOYMENT:-${SERVICE_NAME}}"
REMOTE_PORT="${MLFLOW_PORT:-5000}"
LOCAL_PORT="${LOCAL_PORT:-18080}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"
POLL_SECONDS="${POLL_SECONDS:-2}"
STABILITY_SECONDS="${STABILITY_SECONDS:-20}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-5}"
EXPERIMENT_NAME="${EXPERIMENT_NAME:-mlflow-server-validate}"
RUN_NAME="${RUN_NAME:-mlflow-server-validate-$(date -u +%Y%m%dT%H%M%SZ)}"
BASE_URL="http://127.0.0.1:${LOCAL_PORT}"

PF_LOG="$(mktemp)"
PF_PID=""
POD_NAME=""
BASE_RESTART_COUNT=""

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

require_python_module() {
  python3 - "$1" <<'PY'
import importlib
import sys

mod = sys.argv[1]
try:
    importlib.import_module(mod)
except Exception as exc:
    raise SystemExit(f"missing Python module: {mod}: {exc}") from exc
PY
}

pod_restart_count() {
  kubectl -n "${TARGET_NS}" get pod "${POD_NAME}" -o jsonpath='{.status.containerStatuses[0].restartCount}'
}

pod_ready_status() {
  kubectl -n "${TARGET_NS}" get pod "${POD_NAME}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
}

dump_debug() {
  log "dumping debug state"
  kubectl -n "${TARGET_NS}" get pods -o wide >&2 || true
  kubectl -n "${TARGET_NS}" get svc -o wide >&2 || true
  kubectl -n "${TARGET_NS}" get deploy "${DEPLOYMENT_NAME}" -o wide >&2 || true
  kubectl -n "${TARGET_NS}" get rs -o wide >&2 || true
  kubectl -n "${TARGET_NS}" get events --sort-by=.lastTimestamp >&2 || true
  if [[ -n "${POD_NAME}" ]]; then
    kubectl -n "${TARGET_NS}" logs "${POD_NAME}" --all-containers=true --tail=300 >&2 || true
  else
    kubectl -n "${TARGET_NS}" logs "deployment/${DEPLOYMENT_NAME}" --all-containers=true --tail=300 >&2 || true
  fi
  if [[ -n "${PF_LOG}" && -f "${PF_LOG}" ]]; then
    log "port-forward log:"
    sed 's/^/[pf] /' "${PF_LOG}" >&2 || true
  fi
}

cleanup() {
  local exit_code=$?
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" >/dev/null 2>&1; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
    wait "${PF_PID}" >/dev/null 2>&1 || true
  fi
  if [[ ${exit_code} -ne 0 ]]; then
    dump_debug
  fi
  rm -f "${PF_LOG}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_rollout() {
  log "waiting for deployment rollout: deployment/${DEPLOYMENT_NAME}"
  kubectl -n "${TARGET_NS}" rollout status "deployment/${DEPLOYMENT_NAME}" --timeout="${TIMEOUT_SECONDS}s" >/dev/null
}

wait_for_ready_pod() {
  log "waiting for ready pod with labels app.kubernetes.io/name=${SERVICE_NAME},app.kubernetes.io/component=server"
  kubectl -n "${TARGET_NS}" wait \
    --for=condition=Ready \
    pod \
    -l "app.kubernetes.io/name=${SERVICE_NAME},app.kubernetes.io/component=server" \
    --timeout="${TIMEOUT_SECONDS}s" >/dev/null

  POD_NAME="$(
    kubectl -n "${TARGET_NS}" get pod \
      -l "app.kubernetes.io/name=${SERVICE_NAME},app.kubernetes.io/component=server" \
      -o jsonpath='{.items[0].metadata.name}'
  )"
  [[ -n "${POD_NAME}" ]] || fatal "could not resolve ready pod name"
  log "using pod ${POD_NAME}"
}

wait_for_stable_pod() {
  log "waiting for pod to stay stable for ${STABILITY_SECONDS}s"
  BASE_RESTART_COUNT="$(pod_restart_count)"
  local start elapsed now current_restart current_ready
  start="$(date +%s)"

  while true; do
    current_restart="$(pod_restart_count)"
    current_ready="$(pod_ready_status)"

    if [[ "${current_ready}" != "True" ]]; then
      fatal "pod ${POD_NAME} is not Ready during stability window"
    fi

    if [[ "${current_restart}" != "${BASE_RESTART_COUNT}" ]]; then
      fatal "pod ${POD_NAME} restarted during stability window: ${BASE_RESTART_COUNT} -> ${current_restart}"
    fi

    now="$(date +%s)"
    elapsed="$((now - start))"
    if [[ "${elapsed}" -ge "${STABILITY_SECONDS}" ]]; then
      log "pod stable: restarts=${BASE_RESTART_COUNT} ready=${current_ready}"
      return 0
    fi

    sleep "${POLL_SECONDS}"
  done
}

start_port_forward() {
  : > "${PF_LOG}"
  log "starting port-forward ${LOCAL_PORT} -> ${REMOTE_PORT}"
  kubectl -n "${TARGET_NS}" port-forward \
    --address 127.0.0.1 \
    "svc/${SERVICE_NAME}" \
    "${LOCAL_PORT}:${REMOTE_PORT}" \
    >"${PF_LOG}" 2>&1 &
  PF_PID="$!"
}

port_forward_alive() {
  kill -0 "${PF_PID}" >/dev/null 2>&1
}

wait_for_http_root() {
  local start elapsed last_log
  start="$(date +%s)"
  last_log="${start}"

  while true; do
    if ! port_forward_alive; then
      fatal "port-forward process exited unexpectedly"
    fi

    if python3 - <<PY >/dev/null 2>&1
from urllib.request import urlopen
with urlopen("${BASE_URL}/", timeout=5) as resp:
    body = resp.read(256).decode("utf-8", errors="replace")
    if resp.status < 200 or resp.status >= 400:
        raise SystemExit(1)
PY
    then
      log "HTTP root is reachable at ${BASE_URL}"
      return 0
    fi

    elapsed="$(( $(date +%s) - start ))"
    if [[ "${elapsed}" -ge "${TIMEOUT_SECONDS}" ]]; then
      fatal "timed out waiting for ${BASE_URL}"
    fi

    if (( $(date +%s) - last_log >= POLL_SECONDS )); then
      log "waiting for HTTP root at ${BASE_URL}"
      last_log="$(date +%s)"
    fi

    sleep "${POLL_SECONDS}"
  done
}

run_validation_python() {
  python3 - "${BASE_URL}" "${EXPERIMENT_NAME}" "${RUN_NAME}" <<'PY'
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    from mlflow.tracking import MlflowClient
except Exception as exc:
    raise SystemExit(f"mlflow Python module is required in this environment: {exc}") from exc

base_url, experiment_name, run_name = sys.argv[1:4]

def log(msg: str) -> None:
    print(f"[mlflow-validate] {msg}", flush=True)

def must(cond: bool, msg: str) -> None:
    if not cond:
        raise SystemExit(msg)

def request(method: str, path: str, payload=None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base_url + path, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, resp.headers.get("Content-Type", ""), body

def request_json(method: str, path: str, payload=None):
    status, ctype, body = request(method, path, payload)
    must("json" in ctype.lower(), f"expected JSON from {path}, got {ctype!r}: {body[:200]!r}")
    return json.loads(body)

def flatten_kv(items):
    if isinstance(items, dict):
        return dict(items)
    out = {}
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict) and "key" in item:
                out[item["key"]] = item.get("value")
    return out

def wait_http(path: str, label: str, attempts: int = 30, sleep_s: float = 1.0):
    last = None
    for _ in range(attempts):
        try:
            status, _, body = request("GET", path)
            must(200 <= status < 400, f"{label} returned {status}: {body[:200]!r}")
            log(f"{label} ok")
            return
        except Exception as exc:
            last = exc
            time.sleep(sleep_s)
    raise SystemExit(f"{label} failed: {last}")

client = MlflowClient(tracking_uri=base_url)

wait_http("/", "GET /")
wait_http("/api/2.0/mlflow/experiments/list", "GET experiments/list")

try:
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        exp_id = client.create_experiment(experiment_name)
        log(f"created experiment {experiment_name} id={exp_id}")
    else:
        exp_id = exp.experiment_id
        log(f"reused experiment {experiment_name} id={exp_id}")
except Exception as exc:
    raise SystemExit(f"experiment setup failed: {exc}") from exc

run = client.create_run(
    str(exp_id),
    tags={
        "mlflow.runName": run_name,
        "validation": "true",
    },
)
run_id = run.info.run_id
must(run_id, "missing run_id")
log(f"created run_id={run_id}")

client.log_param(run_id, "validation_param", "ok")
client.log_metric(run_id, "validation_metric", 1.0)
client.set_tag(run_id, "validation_tag", "present")

artifact_name = "validation_artifact.txt"
artifact_content = "artifact-check\n"
tmp_path = None
try:
    with tempfile.NamedTemporaryFile("w", delete=False, prefix="mlflow-validate-", suffix=".txt") as f:
        f.write(artifact_content)
        tmp_path = f.name
    client.log_artifact(run_id, tmp_path)
    log("artifact logged")
finally:
    if tmp_path:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

got = client.get_run(run_id)
must(str(got.info.experiment_id) == str(exp_id), "experiment_id mismatch")
must(got.info.run_id == run_id, "run_id mismatch")

params = flatten_kv(got.data.params)
metrics = flatten_kv(got.data.metrics)
tags = flatten_kv(got.data.tags)

must(params.get("validation_param") == "ok", f"param mismatch: {params!r}")
must(tags.get("validation_tag") == "present", f"tag mismatch: {tags!r}")
metric_val = metrics.get("validation_metric")
must(metric_val is not None, f"metric missing: {metrics!r}")
must(abs(float(metric_val) - 1.0) < 1e-9, f"metric value mismatch: {metric_val!r}")

hist = client.get_metric_history(run_id, "validation_metric")
must(len(hist) >= 1, "metric history is empty")
must(abs(float(hist[-1].value) - 1.0) < 1e-9, f"metric history mismatch: {hist[-1].value!r}")

artifacts = client.list_artifacts(run_id)
artifact_paths = sorted(item.path for item in artifacts)
must(Path(artifact_name).name in [Path(p).name for p in artifact_paths], f"artifact not listed: {artifact_paths!r}")

downloaded = client.download_artifacts(run_id, artifact_name)
with open(downloaded, "r", encoding="utf-8") as f:
    downloaded_content = f.read()
must(downloaded_content == artifact_content, f"artifact content mismatch: {downloaded_content!r}")

log(f"readback ok: params={params}")
log(f"readback ok: metrics={metrics}")
log(f"readback ok: tags={tags}")
log(f"readback ok: artifacts={artifact_paths}")
log("validation passed")
PY
}

run_attempt() {
  start_port_forward
  local wait_start
  wait_start="$(date +%s)"

  while true; do
    if ! port_forward_alive; then
      log "port-forward died before validation; retrying"
      return 1
    fi

    if wait_for_http_root; then
      if run_validation_python; then
        return 0
      fi
      return 1
    fi

    if (( $(date +%s) - wait_start >= TIMEOUT_SECONDS )); then
      return 1
    fi
  done
}

main() {
  require_bin kubectl
  require_bin python3
  require_python_module mlflow

  log "waiting for mlflow deployment in namespace ${TARGET_NS}"
  wait_for_rollout
  wait_for_ready_pod
  wait_for_stable_pod

  local attempt=1
  while [[ "${attempt}" -le "${MAX_ATTEMPTS}" ]]; do
    log "validation attempt ${attempt}/${MAX_ATTEMPTS}"
    if run_attempt; then
      log "validation succeeded"
      return 0
    fi

    log "attempt ${attempt} failed; will retry"
    if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" >/dev/null 2>&1; then
      kill "${PF_PID}" >/dev/null 2>&1 || true
      wait "${PF_PID}" >/dev/null 2>&1 || true
    fi
    PF_PID=""
    attempt=$((attempt + 1))
    sleep "${POLL_SECONDS}"
  done

  fatal "validation failed after ${MAX_ATTEMPTS} attempts"
}

main "$@"