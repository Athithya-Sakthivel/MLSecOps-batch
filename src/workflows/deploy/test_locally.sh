#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/src/workflows/deploy"
MODEL_CACHE_DIR="${MODEL_CACHE_DIR:-${ROOT_DIR}/.model-cache}"
OTEL_COLLECTOR_IMAGE="${OTEL_COLLECTOR_IMAGE:-otel/opentelemetry-collector:0.149.0}"
OTEL_COLLECTOR_CONTAINER_NAME="${OTEL_COLLECTOR_CONTAINER_NAME:-otel-local}"
OTEL_COLLECTOR_CONFIG_DIR="$(mktemp -d /tmp/otel-local-XXXXXX)"
COLLECTOR_GRPC_PORT="${COLLECTOR_GRPC_PORT:-4317}"
COLLECTOR_HTTP_PORT="${COLLECTOR_HTTP_PORT:-4318}"
SERVICE_HOST="${SERVICE_HOST:-127.0.0.1}"
SERVICE_PORT="${SERVICE_PORT:-8000}"
SERVICE_URL="http://${SERVICE_HOST}:${SERVICE_PORT}"
SERVICE_LOG="${SERVICE_LOG:-${ROOT_DIR}/.serve-local.log}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-300}"
REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-30}"

unset RAY_ADDRESS

export MODEL_URI="${MODEL_URI:?MODEL_URI must point to the S3 bundle root}"
export MODEL_VERSION="${MODEL_VERSION:-v1}"
export MODEL_SHA256="${MODEL_SHA256:-29505278adb825a2f79812221b5d3a245145e140973d0354b74e278b50811976}"
export FEATURE_ORDER="${FEATURE_ORDER:-pickup_hour,pickup_dow,pickup_month,pickup_is_weekend,pickup_borough_id,pickup_zone_id,pickup_service_zone_id,dropoff_borough_id,dropoff_zone_id,dropoff_service_zone_id,route_pair_id,avg_duration_7d_zone_hour,avg_fare_30d_zone,trip_count_90d_zone_hour}"
export ALLOW_EXTRA_FEATURES="${ALLOW_EXTRA_FEATURES:-false}"
export MODEL_CACHE_DIR="${MODEL_CACHE_DIR}"
export DEPLOYMENT_ENVIRONMENT="${DEPLOYMENT_ENVIRONMENT:-local-env}"
export K8S_CLUSTER_NAME="${K8S_CLUSTER_NAME:-local-cluster}"
export POD_NAME="${POD_NAME:-local-1}"
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-inference-api}"
export SERVICE_VERSION="${SERVICE_VERSION:-v1}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:${COLLECTOR_GRPC_PORT}}"
export OTEL_EXPORTER_OTLP_TIMEOUT="${OTEL_EXPORTER_OTLP_TIMEOUT:-10000}"
export OTEL_METRIC_EXPORT_INTERVAL_MS="${OTEL_METRIC_EXPORT_INTERVAL_MS:-5000}"
export OTEL_METRIC_EXPORT_TIMEOUT_MS="${OTEL_METRIC_EXPORT_TIMEOUT_MS:-3000}"
export OTEL_TRACES_SAMPLER="${OTEL_TRACES_SAMPLER:-parentbased_traceidratio}"
export OTEL_TRACES_SAMPLER_ARG="${OTEL_TRACES_SAMPLER_ARG:-1.0}"
export ORT_PROVIDERS="${ORT_PROVIDERS:-CPUExecutionProvider}"
export PYTHONPATH="${DEPLOY_DIR}:${PYTHONPATH:-}"
export PYTHONUNBUFFERED=1

for tool in docker ray python; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Missing required tool: $tool" >&2
    exit 1
  }
done

mkdir -p "${MODEL_CACHE_DIR}"
: >"${SERVICE_LOG}"

cleanup() {
  local exit_code=$?
  set +e

  if [[ -n "${SERVICE_PID:-}" ]]; then
    kill "${SERVICE_PID}" >/dev/null 2>&1 || true
    wait "${SERVICE_PID}" >/dev/null 2>&1 || true
  fi

  docker rm -f "${OTEL_COLLECTOR_CONTAINER_NAME}" >/dev/null 2>&1 || true
  ray stop --force >/dev/null 2>&1 || true
  rm -rf "${OTEL_COLLECTOR_CONFIG_DIR}" >/dev/null 2>&1 || true

  exit "${exit_code}"
}
trap cleanup EXIT

cat >"${OTEL_COLLECTOR_CONFIG_DIR}/config.yaml" <<EOF
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:${COLLECTOR_GRPC_PORT}
      http:
        endpoint: 0.0.0.0:${COLLECTOR_HTTP_PORT}

processors:
  batch: {}

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
EOF

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_s="$3"

  python - "$host" "$port" "$timeout_s" <<'PY'
from __future__ import annotations

import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
timeout_s = int(sys.argv[3])

deadline = time.time() + timeout_s
last_error = ""

while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            sys.exit(0)
    except Exception as exc:
        last_error = str(exc)
        time.sleep(0.5)

raise SystemExit(f"Timed out waiting for {host}:{port}: {last_error}")
PY
}

wait_for_http_200() {
  local url="$1"
  local timeout_s="$2"

  python - "$url" "$timeout_s" "$REQUEST_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

import sys
import time
import urllib.error
import urllib.request

url = sys.argv[1]
timeout_s = int(sys.argv[2])
request_timeout = float(sys.argv[3])

deadline = time.time() + timeout_s
attempt = 0
last_error = ""

while time.time() < deadline:
    attempt += 1
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=request_timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if 200 <= resp.status < 300:
                print(f"{url} is ready (HTTP {resp.status})", flush=True)
                sys.exit(0)
            last_error = f"HTTP {resp.status}: {body}"
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        last_error = f"HTTP {exc.code}: {body}"
    except Exception as exc:
        last_error = str(exc)

    if attempt % 5 == 0:
        print(f"Waiting for {url} ... last error: {last_error}", flush=True)

    time.sleep(1)

raise SystemExit(f"Timed out waiting for {url}: {last_error}")
PY
}

get_json() {
  local path="$1"
  python - "$SERVICE_URL" "$path" "$REQUEST_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

base_url = sys.argv[1]
path = sys.argv[2]
request_timeout = float(sys.argv[3])

req = urllib.request.Request(f"{base_url}{path}", method="GET")
try:
    with urllib.request.urlopen(req, timeout=request_timeout) as resp:
        body = resp.read().decode("utf-8")
        if resp.status < 200 or resp.status >= 300:
            raise SystemExit(f"GET {path} failed: HTTP {resp.status} {body}")
        print(body)
except urllib.error.HTTPError as exc:
    body = exc.read().decode("utf-8", errors="replace")
    raise SystemExit(f"GET {path} failed: HTTP {exc.code} {body}") from exc
PY
}

post_json() {
  local path="$1"
  local payload_json="$2"

  python - "$SERVICE_URL" "$path" "$payload_json" "$REQUEST_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

base_url = sys.argv[1]
path = sys.argv[2]
payload = json.loads(sys.argv[3])
request_timeout = float(sys.argv[4])

req = urllib.request.Request(
    f"{base_url}{path}",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=request_timeout) as resp:
        body = resp.read().decode("utf-8")
        if resp.status < 200 or resp.status >= 300:
            raise SystemExit(f"POST {path} failed: HTTP {resp.status} {body}")
        print(body)
except urllib.error.HTTPError as exc:
    body = exc.read().decode("utf-8", errors="replace")
    raise SystemExit(f"POST {path} failed: HTTP {exc.code} {body}") from exc
PY
}

echo "Starting OpenTelemetry Collector..."
docker rm -f "${OTEL_COLLECTOR_CONTAINER_NAME}" >/dev/null 2>&1 || true
docker run -d \
  --name "${OTEL_COLLECTOR_CONTAINER_NAME}" \
  -p "127.0.0.1:${COLLECTOR_GRPC_PORT}:4317" \
  -p "127.0.0.1:${COLLECTOR_HTTP_PORT}:4318" \
  -v "${OTEL_COLLECTOR_CONFIG_DIR}:/etc/otelcol:ro" \
  "${OTEL_COLLECTOR_IMAGE}" \
  --config=/etc/otelcol/config.yaml >/dev/null

wait_for_port 127.0.0.1 "${COLLECTOR_GRPC_PORT}" 30

echo "Starting Ray head..."
ray stop --force >/dev/null 2>&1 || true
ray start --head --dashboard-host=127.0.0.1 >/dev/null

echo "Starting Serve app..."
cd "${ROOT_DIR}"
python -u - <<'PY' > >(tee -a "${SERVICE_LOG}") 2>&1 &
from __future__ import annotations

import time

import ray
from ray import serve

ray.init(address="auto")

from service import app  # noqa: E402

serve.run(app)
print("Serve deployment submitted", flush=True)

while True:
    time.sleep(3600)
PY
SERVICE_PID=$!

echo "Waiting for readiness..."
wait_for_http_200 "${SERVICE_URL}/readyz" "${READY_TIMEOUT_SECONDS}"

echo "Ready. Sending validation requests..."
python - "${SERVICE_URL}" "${REQUEST_TIMEOUT_SECONDS}" <<'PY'
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

base_url = sys.argv[1]
request_timeout = float(sys.argv[2])

sample_row = {
    "pickup_hour": 12,
    "pickup_dow": 1,
    "pickup_month": 4,
    "pickup_is_weekend": 0,
    "pickup_borough_id": 1,
    "pickup_zone_id": 10,
    "pickup_service_zone_id": 2,
    "dropoff_borough_id": 1,
    "dropoff_zone_id": 20,
    "dropoff_service_zone_id": 3,
    "route_pair_id": 100,
    "avg_duration_7d_zone_hour": 12.5,
    "avg_fare_30d_zone": 18.2,
    "trip_count_90d_zone_hour": 45.0,
}

batch_rows = [
    sample_row,
    {
        **sample_row,
        "pickup_hour": 13,
        "pickup_zone_id": 11,
        "dropoff_zone_id": 21,
        "route_pair_id": 101,
        "avg_duration_7d_zone_hour": 14.0,
        "avg_fare_30d_zone": 19.1,
        "trip_count_90d_zone_hour": 46.0,
    },
]


def get_json(path: str) -> dict[str, object]:
    req = urllib.request.Request(f"{base_url}{path}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=request_timeout) as resp:
            body = resp.read().decode("utf-8")
            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError(f"GET {path} failed: {resp.status} {body}")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GET {path} failed: HTTP {exc.code} {body}") from exc


def post_json(path: str, payload: dict[str, object]) -> dict[str, object]:
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=request_timeout) as resp:
            body = resp.read().decode("utf-8")
            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError(f"POST {path} failed: {resp.status} {body}")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST {path} failed: HTTP {exc.code} {body}") from exc


health = get_json("/healthz")
ready = get_json("/readyz")
single = post_json("/predict", {"instances": [sample_row]})
batch = post_json("/predict", {"instances": batch_rows})

if health.get("status") != "ok":
    raise RuntimeError(f"Unexpected /healthz response: {health}")
if ready.get("status") != "ok":
    raise RuntimeError(f"Unexpected /readyz response: {ready}")
if single.get("n_instances") != 1:
    raise RuntimeError(f"Unexpected single prediction response: {single}")
if batch.get("n_instances") != 2:
    raise RuntimeError(f"Unexpected batch prediction response: {batch}")

single_preds = single.get("predictions")
batch_preds = batch.get("predictions")

if not isinstance(single_preds, list) or len(single_preds) != 1:
    raise RuntimeError(f"Single prediction payload shape is wrong: {single}")
if not isinstance(batch_preds, list) or len(batch_preds) != 2:
    raise RuntimeError(f"Batch prediction payload shape is wrong: {batch}")

print(json.dumps(
    {
        "health": health,
        "ready": ready,
        "single": single,
        "batch": batch,
        "result": "ok",
    },
    indent=2,
    ensure_ascii=False,
))
PY

echo "E2E validation passed."
echo "Service log: ${SERVICE_LOG}"