#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
umask 077

PROFILE="${K8S_CLUSTER:-kind}"
K8S_CLUSTER="${PROFILE}"

SIGNOZ_NAMESPACE="${SIGNOZ_NAMESPACE:-signoz}"
SIGNOZ_HELM_VERSION="${SIGNOZ_HELM_VERSION:-0.117.1}"
SIGNOZ_K8S_INFRA_VERSION="${SIGNOZ_K8S_INFRA_VERSION:-0.15.0}"

SIGNOZ_INFERENCE_NAMESPACE="${SIGNOZ_INFERENCE_NAMESPACE:-inference-ns}"
SIGNOZ_LOG_NAMESPACES="${SIGNOZ_LOG_NAMESPACES:-${SIGNOZ_INFERENCE_NAMESPACE}}"
SIGNOZ_INCLUDE_ONLY_LOG_NAMESPACES="${SIGNOZ_INCLUDE_ONLY_LOG_NAMESPACES:-${SIGNOZ_INFERENCE_NAMESPACE}}"

SIGNOZ_STORAGE_CLASS="${SIGNOZ_STORAGE_CLASS:-default-storage-class}"
SIGNOZ_CLUSTER_DOMAIN="${SIGNOZ_CLUSTER_DOMAIN:-cluster.local}"
SIGNOZ_CLUSTER_NAME="${SIGNOZ_CLUSTER_NAME:-}"
SIGNOZ_CLOUD="${SIGNOZ_CLOUD:-}"
SIGNOZ_CLICKHOUSE_ENABLED="${SIGNOZ_CLICKHOUSE_ENABLED:-true}"

SIGNOZ_SIGNOZ_NAME="${SIGNOZ_SIGNOZ_NAME:-signoz}"
SIGNOZ_SIGNOZ_REPLICAS="${SIGNOZ_SIGNOZ_REPLICAS:-1}"
SIGNOZ_SIGNOZ_RESOURCES="${SIGNOZ_SIGNOZ_RESOURCES:-requests.cpu=100m,requests.memory=256Mi,limits.cpu=500m,limits.memory=512Mi}"
SIGNOZ_SIGNOZ_PERSISTENCE_SIZE="${SIGNOZ_SIGNOZ_PERSISTENCE_SIZE:-1Gi}"

SIGNOZ_CLICKHOUSE_USER="${SIGNOZ_CLICKHOUSE_USER:-admin}"
SIGNOZ_CLICKHOUSE_PASSWORD="${SIGNOZ_CLICKHOUSE_PASSWORD:-27ff0399-0d3a-4bd8-919d-17c2181e6fb9}"
SIGNOZ_CLICKHOUSE_REPLICAS="${SIGNOZ_CLICKHOUSE_REPLICAS:-1}"
SIGNOZ_CLICKHOUSE_SHARDS="${SIGNOZ_CLICKHOUSE_SHARDS:-1}"
SIGNOZ_CLICKHOUSE_RESOURCES="${SIGNOZ_CLICKHOUSE_RESOURCES:-requests.cpu=250m,requests.memory=512Mi,limits.cpu=1,limits.memory=1Gi}"
SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE="${SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE:-10Gi}"
SIGNOZ_ZOOKEEPER_REPLICAS="${SIGNOZ_ZOOKEEPER_REPLICAS:-1}"
SIGNOZ_ZOOKEEPER_RESOURCES="${SIGNOZ_ZOOKEEPER_RESOURCES:-requests.cpu=100m,requests.memory=256Mi,limits.cpu=200m,limits.memory=512Mi}"

SIGNOZ_HELM_TIMEOUT="${SIGNOZ_HELM_TIMEOUT:-1h}"
SIGNOZ_ATOMIC="${SIGNOZ_ATOMIC:-false}"

HELM_REPO_NAME="${HELM_REPO_NAME:-signoz}"
HELM_REPO_URL="${HELM_REPO_URL:-https://charts.signoz.io}"
HELM_RELEASE="${HELM_RELEASE:-signoz}"
HELM_CHART="${HELM_CHART:-signoz/signoz}"

MANIFESTS_DIR="${MANIFESTS_DIR:-./manifests/signoz}"
VALUES_FILE="${VALUES_FILE:-${MANIFESTS_DIR}/values.yaml}"
STATE_FILE="${STATE_FILE:-${MANIFESTS_DIR}/.signoz-rollout.sha256}"
POST_RENDERER_FILE="${POST_RENDERER_FILE:-${MANIFESTS_DIR}/.signoz-post-renderer.py}"
STATE_CONFIGMAP="${STATE_CONFIGMAP:-signoz-bootstrap-state}"
POST_RENDERER_VERSION="${POST_RENDERER_VERSION:-1}"

log() { printf '[%s] [%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${K8S_CLUSTER}" "$*" >&2; }
fatal() { printf '[FATAL] [%s] %s\n' "${K8S_CLUSTER}" "$*" >&2; exit 1; }
require_bin() { command -v "$1" >/dev/null 2>&1 || fatal "$1 not found in PATH"; }

yaml_bool() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|Yes|on|ON|On) printf 'true' ;;
    *) printf 'false' ;;
  esac
}

trim() {
  local s="${1:-}"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "${s}"
}

apply_manifest() {
  kubectl apply -f - >/dev/null
}

ensure_namespace() {
  kubectl create namespace "${1}" --dry-run=client -o yaml | apply_manifest
}

ensure_storage_class() {
  if kubectl get storageclass "${SIGNOZ_STORAGE_CLASS}" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "${PROFILE}" != "kind" ]]; then
    fatal "StorageClass '${SIGNOZ_STORAGE_CLASS}' not found"
  fi

  if ! kubectl get storageclass local-path >/dev/null 2>&1; then
    log "installing local-path provisioner"
    kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml >/dev/null
    kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=180s >/dev/null
  fi

  cat <<EOF_SC | apply_manifest
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${SIGNOZ_STORAGE_CLASS}
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
EOF_SC
}

to_resources_yaml() {
  local spec="$1"
  local indent="$2"
  local req_cpu="" req_mem="" lim_cpu="" lim_mem="" pair key value
  local -a pairs=()
  IFS=',' read -r -a pairs <<< "${spec}"
  for pair in "${pairs[@]}"; do
    pair="$(trim "${pair}")"
    [[ -n "${pair}" ]] || continue
    key="$(trim "${pair%%=*}")"
    value="$(trim "${pair#*=}")"
    case "${key}" in
      requests.cpu) req_cpu="${value}" ;;
      requests.memory) req_mem="${value}" ;;
      limits.cpu) lim_cpu="${value}" ;;
      limits.memory) lim_mem="${value}" ;;
      *) fatal "unsupported resources key '${key}' in '${spec}'" ;;
    esac
  done
  printf '%sresources:\n' "${indent}"
  if [[ -n "${req_cpu}" || -n "${req_mem}" ]]; then
    printf '%s  requests:\n' "${indent}"
    [[ -n "${req_cpu}" ]] && printf '%s    cpu: %s\n' "${indent}" "${req_cpu}"
    [[ -n "${req_mem}" ]] && printf '%s    memory: %s\n' "${indent}" "${req_mem}"
  fi
  if [[ -n "${lim_cpu}" || -n "${lim_mem}" ]]; then
    printf '%s  limits:\n' "${indent}"
    [[ -n "${lim_cpu}" ]] && printf '%s    cpu: %s\n' "${indent}" "${lim_cpu}"
    [[ -n "${lim_mem}" ]] && printf '%s    memory: %s\n' "${indent}" "${lim_mem}"
  fi
}

compute_profile_defaults() {
  case "${PROFILE}" in
    kind)
      SIGNOZ_CLUSTER_NAME="${SIGNOZ_CLUSTER_NAME:-kind-local}"
      SIGNOZ_CLOUD="${SIGNOZ_CLOUD:-other}"
      SIGNOZ_CLICKHOUSE_REPLICAS="${SIGNOZ_CLICKHOUSE_REPLICAS:-1}"
      SIGNOZ_CLICKHOUSE_SHARDS="${SIGNOZ_CLICKHOUSE_SHARDS:-1}"
      SIGNOZ_ZOOKEEPER_REPLICAS="${SIGNOZ_ZOOKEEPER_REPLICAS:-1}"
      SIGNOZ_SIGNOZ_REPLICAS="${SIGNOZ_SIGNOZ_REPLICAS:-1}"
      SIGNOZ_SIGNOZ_RESOURCES="${SIGNOZ_SIGNOZ_RESOURCES:-requests.cpu=100m,requests.memory=256Mi,limits.cpu=500m,limits.memory=512Mi}"
      SIGNOZ_CLICKHOUSE_RESOURCES="${SIGNOZ_CLICKHOUSE_RESOURCES:-requests.cpu=250m,requests.memory=512Mi,limits.cpu=1,limits.memory=1Gi}"
      SIGNOZ_ZOOKEEPER_RESOURCES="${SIGNOZ_ZOOKEEPER_RESOURCES:-requests.cpu=100m,requests.memory=256Mi,limits.cpu=200m,limits.memory=512Mi}"
      SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE="${SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE:-10Gi}"
      SIGNOZ_SIGNOZ_PERSISTENCE_SIZE="${SIGNOZ_SIGNOZ_PERSISTENCE_SIZE:-1Gi}"
      ;;
    eks)
      SIGNOZ_CLUSTER_NAME="${SIGNOZ_CLUSTER_NAME:-prod-eks}"
      SIGNOZ_CLOUD="${SIGNOZ_CLOUD:-aws}"
      SIGNOZ_CLICKHOUSE_REPLICAS="${SIGNOZ_CLICKHOUSE_REPLICAS:-2}"
      SIGNOZ_CLICKHOUSE_SHARDS="${SIGNOZ_CLICKHOUSE_SHARDS:-2}"
      SIGNOZ_ZOOKEEPER_REPLICAS="${SIGNOZ_ZOOKEEPER_REPLICAS:-3}"
      SIGNOZ_SIGNOZ_REPLICAS="${SIGNOZ_SIGNOZ_REPLICAS:-2}"
      SIGNOZ_SIGNOZ_RESOURCES="${SIGNOZ_SIGNOZ_RESOURCES:-requests.cpu=250m,requests.memory=512Mi,limits.cpu=1,limits.memory=1Gi}"
      SIGNOZ_CLICKHOUSE_RESOURCES="${SIGNOZ_CLICKHOUSE_RESOURCES:-requests.cpu=500m,requests.memory=1Gi,limits.cpu=2,limits.memory=4Gi}"
      SIGNOZ_ZOOKEEPER_RESOURCES="${SIGNOZ_ZOOKEEPER_RESOURCES:-requests.cpu=100m,requests.memory=256Mi,limits.cpu=200m,limits.memory=512Mi}"
      SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE="${SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE:-100Gi}"
      SIGNOZ_SIGNOZ_PERSISTENCE_SIZE="${SIGNOZ_SIGNOZ_PERSISTENCE_SIZE:-1Gi}"
      ;;
    *)
      fatal "invalid profile: ${PROFILE} (expected kind|eks)"
      ;;
  esac
}

render_values_file() {
  mkdir -p "${MANIFESTS_DIR}"
  cat > "${VALUES_FILE}" <<EOF
global:
  storageClass: "${SIGNOZ_STORAGE_CLASS}"
  clusterDomain: "${SIGNOZ_CLUSTER_DOMAIN}"
  clusterName: "${SIGNOZ_CLUSTER_NAME}"
  cloud: "${SIGNOZ_CLOUD}"

clusterName: "${SIGNOZ_CLUSTER_NAME}"
nameOverride: ""
fullnameOverride: ""
imagePullSecrets: []

clickhouse:
  enabled: $(yaml_bool "${SIGNOZ_CLICKHOUSE_ENABLED}")
  cluster: "cluster"
  database: "signoz_metrics"
  traceDatabase: "signoz_traces"
  logDatabase: "signoz_logs"
  meterDatabase: "signoz_meter"
  user: "${SIGNOZ_CLICKHOUSE_USER}"
  password: "${SIGNOZ_CLICKHOUSE_PASSWORD}"
  image:
    registry: docker.io
    repository: clickhouse/clickhouse-server
    tag: 25.5.6
    pullPolicy: IfNotPresent
  imagePullSecrets: []
  annotations: {}
  serviceAccount:
    create: true
    annotations: {}
  service:
    annotations: {}
    labels: {}
    type: ClusterIP
    httpPort: 8123
    tcpPort: 9000
  secure: false
  verify: false
  installCustomStorageClass: false
  layout:
    shardsCount: ${SIGNOZ_CLICKHOUSE_SHARDS}
    replicasCount: ${SIGNOZ_CLICKHOUSE_REPLICAS}
  zookeeper:
    enabled: true
    replicaCount: ${SIGNOZ_ZOOKEEPER_REPLICAS}
    resources:
$(to_resources_yaml "${SIGNOZ_ZOOKEEPER_RESOURCES}" "      ")
  settings:
    prometheus/endpoint: /metrics
    prometheus/port: 9363
  defaultSettings:
    format_schema_path: /etc/clickhouse-server/config.d/
    user_scripts_path: /var/lib/clickhouse/user_scripts/
    user_defined_executable_functions_config: /etc/clickhouse-server/functions/custom-functions.xml
  podAnnotations:
    signoz.io/scrape: "true"
    signoz.io/port: "9363"
    signoz.io/path: /metrics
  podDistribution: []
  nodeSelector: {}
  tolerations: []
  affinity: {}
  resources:
$(to_resources_yaml "${SIGNOZ_CLICKHOUSE_RESOURCES}" "    ")
  securityContext:
    enabled: true
    runAsUser: 101
    runAsGroup: 101
    fsGroup: 101
    fsGroupChangePolicy: OnRootMismatch
  allowedNetworkIps:
    - "10.0.0.0/8"
    - "100.64.0.0/10"
    - "172.16.0.0/12"
    - "192.0.0.0/24"
    - "198.18.0.0/15"
    - "192.168.0.0/16"
  persistence:
    enabled: true
    existingClaim: ""
    storageClass: "${SIGNOZ_STORAGE_CLASS}"
    accessModes:
      - ReadWriteOnce
    size: "${SIGNOZ_CLICKHOUSE_PERSISTENCE_SIZE}"
  profiles: {}
  defaultProfiles:
    default/allow_experimental_window_functions: "1"
    default/allow_nondeterministic_mutations: "1"
    default/secondary_indices_enable_bulk_filtering: "0"
    admin/secondary_indices_enable_bulk_filtering: "0"
    default/query_plan_max_limit_for_lazy_materialization: "0"
    admin/query_plan_max_limit_for_lazy_materialization: "0"
  initContainers:
    enabled: true
    udf:
      enabled: true
      image:
        registry: docker.io
        repository: alpine
        tag: 3.18.2
        pullPolicy: IfNotPresent
      command:
        - sh
        - -c
        - |
          set -e
          version="v0.0.1"
          node_os=$(uname -s | tr '[:upper:]' '[:lower:]')
          node_arch=$(uname -m | sed s/aarch64/arm64/ | sed s/x86_64/amd64/)
          echo "Fetching histogram-binary for ${node_os}/${node_arch}"
          cd /tmp
          wget -O histogram-quantile.tar.gz "https://github.com/SigNoz/signoz/releases/download/histogram-quantile%2F${version}/histogram-quantile_${node_os}_${node_arch}.tar.gz"
          tar -xzf histogram-quantile.tar.gz
          chmod +x histogram-quantile
          mv histogram-quantile /var/lib/clickhouse/user_scripts/histogramQuantile
          echo "histogram-quantile installed successfully"
    init:
      enabled: false
      image:
        registry: docker.io
        repository: busybox
        tag: 1.35
        pullPolicy: IfNotPresent
      command:
        - /bin/sh
        - -c
        - |
          set -e
          until curl -s -o /dev/null http://signoz-clickhouse:8123/
          do sleep 1
          done

signoz:
  name: "${SIGNOZ_SIGNOZ_NAME}"
  replicaCount: ${SIGNOZ_SIGNOZ_REPLICAS}
  image:
    registry: docker.io
    repository: signoz/signoz
    tag: v0.118.0
    pullPolicy: IfNotPresent
  imagePullSecrets: []
  serviceAccount:
    create: true
    annotations: {}
  service:
    annotations: {}
    labels: {}
    type: ClusterIP
    port: 8080
    internalPort: 8085
    opampPort: 4320
    nodePort: null
    internalNodePort: null
    opampInternalNodePort: null
  annotations: {}
  additionalArgs: []
  env:
    signoz_telemetrystore_provider: clickhouse
    signoz_emailing_enabled: false
    signoz_prometheus_active__query__tracker_enabled: false
    signoz_alertmanager_provider: signoz
    signoz_alertmanager_signoz_external__url: "http://localhost:8080"
    signoz_log_namespaces: "${SIGNOZ_LOG_NAMESPACES}"
    signoz_include_only_log_namespaces: "${SIGNOZ_INCLUDE_ONLY_LOG_NAMESPACES}"
  podSecurityContext: {}
  podAnnotations: {}
  securityContext: {}
  additionalVolumeMounts: []
  additionalVolumes: []
  livenessProbe:
    enabled: true
  readinessProbe:
    enabled: true
  resources:
$(to_resources_yaml "${SIGNOZ_SIGNOZ_RESOURCES}" "    ")
  priorityClassName: ""
  nodeSelector: {}
  tolerations: []
  affinity: {}
  topologySpreadConstraints: []
  persistence:
    enabled: true
    existingClaim: ""
    storageClass: "${SIGNOZ_STORAGE_CLASS}"
    accessModes:
      - ReadWriteOnce
    size: "${SIGNOZ_SIGNOZ_PERSISTENCE_SIZE}"

externalClickhouse:
  host:
  cluster: cluster
  database: signoz_metrics
  traceDatabase: signoz_traces
  logDatabase: signoz_logs
  meterDatabase: signoz_meter
  user: ""
  password: ""
  existingSecret:
  existingSecretPasswordKey:
  secure: false
  verify: false
  httpPort: 8123
  tcpPort: 9000
EOF
}

write_post_renderer() {
  local spec_hash="$1"
  cat > "${POST_RENDERER_FILE}" <<EOF
#!/usr/bin/env python3
from __future__ import annotations
import sys
import yaml

SPEC_HASH = "${spec_hash}"

def mutate(doc):
    if not isinstance(doc, dict):
        return doc
    metadata = doc.setdefault("metadata", {})
    labels = metadata.setdefault("labels", {})
    annotations = metadata.setdefault("annotations", {})
    labels.setdefault("app.kubernetes.io/managed-by", "signoz-setup")
    labels.setdefault("app.kubernetes.io/part-of", "signoz")
    annotations["signoz.dev/spec-hash"] = SPEC_HASH
    annotations.setdefault("signoz.dev/post-rendered", "true")
    return doc

def main() -> int:
    text = sys.stdin.read()
    if not text.strip():
        return 0
    docs = list(yaml.safe_load_all(text))
    yaml.safe_dump_all(
        [mutate(doc) if doc is not None else None for doc in docs],
        sys.stdout,
        sort_keys=False,
        explicit_start=True,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
EOF
  chmod 0755 "${POST_RENDERER_FILE}"
}

compute_spec_hash() {
  python3 - <<'PY'
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import os

values_path = Path(os.environ["VALUES_FILE"])
payload = {
    "post_renderer_version": os.environ["POST_RENDERER_VERSION"],
    "profile": os.environ["PROFILE"],
    "cluster_name": os.environ["SIGNOZ_CLUSTER_NAME"],
    "cloud": os.environ["SIGNOZ_CLOUD"],
    "namespace": os.environ["SIGNOZ_NAMESPACE"],
    "release": os.environ["HELM_RELEASE"],
    "chart": {
        "repo": os.environ["HELM_REPO_NAME"],
        "url": os.environ["HELM_REPO_URL"],
        "chart": os.environ["HELM_CHART"],
        "version": os.environ["SIGNOZ_HELM_VERSION"],
    },
    "values_sha256": hashlib.sha256(values_path.read_bytes()).hexdigest(),
}
data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
print(hashlib.sha256(data).hexdigest())
PY
}

read_state_hash() {
  kubectl -n "${SIGNOZ_NAMESPACE}" get configmap "${STATE_CONFIGMAP}" -o jsonpath='{.data.spec-hash}' 2>/dev/null || true
}

write_state_hash() {
  local spec_hash="$1"
  cat <<EOF | apply_manifest
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${STATE_CONFIGMAP}
  namespace: ${SIGNOZ_NAMESPACE}
  labels:
    app.kubernetes.io/name: signoz-bootstrap-state
    app.kubernetes.io/part-of: signoz
  annotations:
    signoz.dev/spec-hash: ${spec_hash}
data:
  spec-hash: ${spec_hash}
  updated-at: $(date -u +'%Y-%m-%dT%H:%M:%SZ')
EOF
}

release_exists() {
  [[ -n "$(helm list -n "${SIGNOZ_NAMESPACE}" --filter "^${HELM_RELEASE}$" -q 2>/dev/null || true)" ]]
}

helm_repo_sync() {
  helm repo add "${HELM_REPO_NAME}" "${HELM_REPO_URL}" --force-update >/dev/null
  helm repo update >/dev/null
}

validate_rendered_values() {
  helm template "${HELM_RELEASE}" "${HELM_CHART}" \
    --version "${SIGNOZ_HELM_VERSION}" \
    --namespace "${SIGNOZ_NAMESPACE}" \
    --values "${VALUES_FILE}" \
    --post-renderer "${POST_RENDERER_FILE}" >/dev/null
}

wait_for_rollouts() {
  local kind name
  for kind in deployment statefulset; do
    while IFS= read -r name; do
      [[ -n "${name}" ]] || continue
      kubectl -n "${SIGNOZ_NAMESPACE}" rollout status "${kind}/${name}" --timeout="${ROLLOUT_TIMEOUT}" >/dev/null
    done < <(kubectl -n "${SIGNOZ_NAMESPACE}" get "${kind}" -l "app.kubernetes.io/instance=${HELM_RELEASE}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
  done
}

resolve_service_name() {
  local want_port="$1"
  local label_json
  label_json="$(kubectl -n "${SIGNOZ_NAMESPACE}" get svc -l "app.kubernetes.io/instance=${HELM_RELEASE}" -o json 2>/dev/null || true)"
  LABEL_JSON="${label_json}" python3 - "$want_port" <<'PY'
from __future__ import annotations
import json
import os
import sys

want = int(sys.argv[1])
raw = os.environ.get("LABEL_JSON", "").strip()
if not raw:
    raise SystemExit(0)
obj = json.loads(raw)
items = obj.get("items", [])
for item in items:
    ports = item.get("spec", {}).get("ports", [])
    for port in ports:
        if int(port.get("port", -1)) == want:
            print(item.get("metadata", {}).get("name", ""))
            raise SystemExit(0)
for item in items:
    name = item.get("metadata", {}).get("name", "")
    if name:
        print(name)
        raise SystemExit(0)
PY
}

wait_for_service_endpoints() {
  local svc_name="$1"
  local max_wait="${2:-180}"
  local elapsed=0
  while true; do
    if kubectl -n "${SIGNOZ_NAMESPACE}" get endpoints "${svc_name}" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | grep -q '.'; then
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    [[ "${elapsed}" -lt "${max_wait}" ]] || fatal "Service '${svc_name}' has no endpoints after ${max_wait}s"
  done
}

find_clickhouse_pod() {
  local name
  name="$(kubectl -n "${SIGNOZ_NAMESPACE}" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -m1 -i 'clickhouse' || true)"
  [[ -n "${name}" ]] || fatal "ClickHouse pod not found"
  printf '%s' "${name}"
}

wait_for_clickhouse() {
  local pod container elapsed=0
  pod="$(find_clickhouse_pod)"
  container="$(kubectl -n "${SIGNOZ_NAMESPACE}" get pod "${pod}" -o jsonpath='{.spec.containers[0].name}')"
  while true; do
    if kubectl -n "${SIGNOZ_NAMESPACE}" exec -c "${container}" "${pod}" -- sh -lc 'clickhouse-client --query="SELECT 1"' >/dev/null 2>&1; then
      return 0
    fi
    sleep 10
    elapsed=$((elapsed + 10))
    [[ "${elapsed}" -lt 900 ]] || fatal "ClickHouse did not become ready after 900s"
  done
}

print_connection_info() {
  local signoz_svc clickhouse_svc
  signoz_svc="$(resolve_service_name 8080)"
  [[ -n "${signoz_svc}" ]] || signoz_svc="${HELM_RELEASE}"
  clickhouse_svc="$(resolve_service_name 9000)"
  [[ -n "${clickhouse_svc}" ]] || clickhouse_svc="${HELM_RELEASE}-clickhouse"
  log "SigNoz UI: kubectl -n ${SIGNOZ_NAMESPACE} port-forward svc/${signoz_svc} 3301:8080"
  log "SigNoz UI URL: http://localhost:3301"
  log "ClickHouse TCP: ${clickhouse_svc}.${SIGNOZ_NAMESPACE}.svc.cluster.local:9000"
}

install_or_upgrade() {
  local spec_hash="$1"
  helm_repo_sync
  validate_rendered_values

  local current_hash
  current_hash="$(read_state_hash)"
  if [[ -z "${current_hash}" && -f "${STATE_FILE}" ]]; then
    current_hash="$(cat "${STATE_FILE}" 2>/dev/null || true)"
  fi

  if release_exists && [[ "${current_hash}" == "${spec_hash}" ]]; then
    log "rollout hash unchanged; skipping helm upgrade"
    return 0
  fi

  local atomic_flag=()
  if [[ "${SIGNOZ_ATOMIC}" == "true" ]]; then
    atomic_flag+=(--atomic)
  fi

  helm upgrade --install "${HELM_RELEASE}" "${HELM_CHART}" \
    --namespace "${SIGNOZ_NAMESPACE}" \
    --create-namespace \
    --version "${SIGNOZ_HELM_VERSION}" \
    --values "${VALUES_FILE}" \
    --post-renderer "${POST_RENDERER_FILE}" \
    --wait \
    --timeout "${SIGNOZ_HELM_TIMEOUT}" \
    "${atomic_flag[@]}" >/dev/null

  write_state_hash "${spec_hash}" >/dev/null
  printf '%s' "${spec_hash}" > "${STATE_FILE}"
}

hash_payload() {
  PROFILE="${PROFILE}" \
  SIGNOZ_CLUSTER_NAME="${SIGNOZ_CLUSTER_NAME}" \
  SIGNOZ_CLOUD="${SIGNOZ_CLOUD}" \
  SIGNOZ_NAMESPACE="${SIGNOZ_NAMESPACE}" \
  HELM_RELEASE="${HELM_RELEASE}" \
  HELM_REPO_NAME="${HELM_REPO_NAME}" \
  HELM_REPO_URL="${HELM_REPO_URL}" \
  HELM_CHART="${HELM_CHART}" \
  SIGNOZ_HELM_VERSION="${SIGNOZ_HELM_VERSION}" \
  POST_RENDERER_VERSION="${POST_RENDERER_VERSION}" \
  VALUES_FILE="${VALUES_FILE}" \
  compute_spec_hash
}

rollout() {
  log "starting SigNoz rollout"
  ensure_namespace "${SIGNOZ_NAMESPACE}"
  ensure_storage_class
  compute_profile_defaults
  render_values_file
  local spec_hash
  spec_hash="$(hash_payload)"
  write_post_renderer "${spec_hash}"
  install_or_upgrade "${spec_hash}"
  wait_for_rollouts
  local signoz_svc clickhouse_svc
  signoz_svc="$(resolve_service_name 8080)"
  [[ -n "${signoz_svc}" ]] || signoz_svc="${HELM_RELEASE}"
  clickhouse_svc="$(resolve_service_name 9000)"
  [[ -n "${clickhouse_svc}" ]] || clickhouse_svc="${HELM_RELEASE}-clickhouse"
  wait_for_service_endpoints "${signoz_svc}" 180
  wait_for_service_endpoints "${clickhouse_svc}" 300
  wait_for_clickhouse
  print_connection_info
  printf 'NAMESPACE=%s\nRELEASE=%s\nVALUES=%s\nHASH=%s\n' "${SIGNOZ_NAMESPACE}" "${HELM_RELEASE}" "${VALUES_FILE}" "${spec_hash}"
}

delete_all() {
  helm uninstall "${HELM_RELEASE}" -n "${SIGNOZ_NAMESPACE}" >/dev/null 2>&1 || true
  kubectl -n "${SIGNOZ_NAMESPACE}" delete configmap "${STATE_CONFIGMAP}" --ignore-not-found >/dev/null 2>&1 || true
  rm -f "${STATE_FILE}" "${POST_RENDERER_FILE}" >/dev/null 2>&1 || true
  log "deleted SigNoz release and bootstrap state"
}

require_prereqs() {
  require_bin kubectl
  require_bin helm
  require_bin python3
  kubectl cluster-info >/dev/null
}

main() {
  require_prereqs
  case "${1:---rollout}" in
    --rollout)
      ensure_dns_ready
      rollout
      ;;
    --delete)
      delete_all
      ;;
    --help|-h)
      cat <<EOF
Usage: signoz.sh [--rollout|--delete]

Environment:
  K8S_CLUSTER=kind|eks
  SIGNOZ_NAMESPACE=signoz
  SIGNOZ_STORAGE_CLASS=default-storage-class
  SIGNOZ_HELM_VERSION=0.117.1
  SIGNOZ_CLICKHOUSE_PASSWORD=...
EOF
      ;;
    *)
      fatal "unknown option: ${1}"
      ;;
  esac
}

ensure_dns_ready() {
  log "waiting for cluster DNS"
  local elapsed=0
  while [[ "${elapsed}" -lt 120 ]]; do
    if kubectl -n kube-system get deploy coredns >/dev/null 2>&1 || kubectl -n kube-system get deploy kube-dns >/dev/null 2>&1; then
      if kubectl -n kube-system get pods -l k8s-app=coredns -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -q '^Running$'; then
        return 0
      fi
      if kubectl -n kube-system get pods -l k8s-app=kube-dns -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -q '^Running$'; then
        return 0
      fi
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  fatal "DNS not ready after 120s"
}

main "${1:-}"