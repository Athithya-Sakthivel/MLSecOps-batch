#!/usr/bin/env bash
set -Eeuo pipefail

# =============================================================================
# Iceberg REST Catalog Deployment Orchestrator
# Supports:
#   - K8S_CLUSTER=kind
#   - K8S_CLUSTER=eks
#
# Auth modes:
#   - USE_IAM=false -> static AWS credentials in a Kubernetes Secret
#   - USE_IAM=true  -> EKS IRSA via ServiceAccount annotation
# =============================================================================

TARGET_NS="${TARGET_NS:-default}"
MANIFEST_DIR="${MANIFEST_DIR:-src/manifests/iceberg}"

DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iceberg-rest}"
SERVICE_NAME="${SERVICE_NAME:-iceberg-rest}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-iceberg-rest-sa}"
SECRET_NAME="${SECRET_NAME:-iceberg-storage-credentials}"
PDB_NAME="${PDB_NAME:-iceberg-rest-pdb}"
ANNOTATION_KEY="${ANNOTATION_KEY:-mlsecops.iceberg.checksum}"

K8S_CLUSTER="${K8S_CLUSTER:-kind}"
USE_IAM_RAW="${USE_IAM:-}"

# Default to IRSA on EKS, static secret on kind.
if [[ -z "${USE_IAM_RAW}" ]]; then
  if [[ "${K8S_CLUSTER}" == "eks" ]]; then
    USE_IAM="true"
  else
    USE_IAM="false"
  fi
else
  USE_IAM="${USE_IAM_RAW}"
fi

bool_true() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|Yes|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

if bool_true "${USE_IAM}"; then
  USE_IAM="true"
else
  USE_IAM="false"
fi

if [[ "${K8S_CLUSTER}" != "kind" && "${K8S_CLUSTER}" != "eks" ]]; then
  printf '[%s] [iceberg][FATAL] unsupported K8S_CLUSTER=%s (expected kind or eks)\n' \
    "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "${K8S_CLUSTER}" >&2
  exit 1
fi

if [[ "${K8S_CLUSTER}" == "kind" && "${USE_IAM}" == "true" ]]; then
  printf '[%s] [iceberg][FATAL] USE_IAM=true requires K8S_CLUSTER=eks\n' \
    "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >&2
  exit 1
fi

if [[ "${USE_IAM}" == "true" ]]; then
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN || true
fi

# Pinned working image that already includes the PostgreSQL JDBC driver.
IMAGE="${IMAGE:-ghcr.io/athithya-sakthivel/iceberg-rest:2026-04-03-20-08--861d47a@sha256:0fde6e09b4dd16c0f08165517a604d59dc0538d1b868275a46c1bf5d9b5f1bcc}"

CONTAINER_PORT="${CONTAINER_PORT:-8181}"
SERVICE_PORT="${SERVICE_PORT:-8181}"

AWS_REGION="${AWS_REGION:-ap-south-1}"
S3_BUCKET="${S3_BUCKET:-e2e-mlops-data-681802563986}"
S3_PREFIX="${S3_PREFIX:-iceberg/warehouse}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
S3_PATH_STYLE_ACCESS="${S3_PATH_STYLE_ACCESS:-false}"

# Static AWS credentials are required only when USE_IAM=false.
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

# EKS IAM role required only when USE_IAM=true.
IAM_ROLE_ARN="${IAM_ROLE_ARN:-}"

# PostgreSQL / CNPG defaults
POSTGRES_NAMESPACE="${POSTGRES_NAMESPACE:-default}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres-pooler}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-iceberg}"
POSTGRES_SECRET_NAME="${POSTGRES_SECRET_NAME:-postgres-cluster-app}"
POSTGRES_USERNAME_KEY="${POSTGRES_USERNAME_KEY:-username}"
POSTGRES_PASSWORD_KEY="${POSTGRES_PASSWORD_KEY:-password}"

READY_TIMEOUT="${READY_TIMEOUT:-600}"
VALIDATE_SCRIPT="${VALIDATE_SCRIPT:-src/tests/elt/iceberg_server_validate.sh}"
ENABLE_PDB="${ENABLE_PDB:-true}"

log() {
  printf '[%s] [iceberg] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

fatal() {
  printf '[%s] [iceberg][FATAL] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
  exit 1
}

is_iam_mode() {
  [[ "${USE_IAM}" == "true" ]]
}

is_static_mode() {
  [[ "${USE_IAM}" == "false" ]]
}

# --- Prerequisites ------------------------------------------------------------

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "$1 required in PATH"
}

require_prereqs() {
  require_bin kubectl
  require_bin python3
  require_bin sha256sum
  kubectl version --client >/dev/null 2>&1 || fatal "kubectl client unavailable"
  kubectl cluster-info >/dev/null 2>&1 || fatal "kubectl cannot reach cluster"
}

# --- Helpers ------------------------------------------------------------------

ensure_namespace() {
  if kubectl get ns "${TARGET_NS}" >/dev/null 2>&1; then
    return 0
  fi
  log "creating namespace ${TARGET_NS}"
  kubectl create ns "${TARGET_NS}" >/dev/null
}

normalize_prefix() {
  local p="${1#/}"
  p="${p%/}"
  printf '%s' "$p"
}

trim_trailing_slash() {
  local s="$1"
  while [[ "$s" == */ ]]; do
    s="${s%/}"
  done
  printf '%s' "$s"
}

warehouse_uri() {
  local prefix
  prefix="$(normalize_prefix "${S3_PREFIX}")"
  if [[ -n "${prefix}" ]]; then
    printf 's3://%s/%s/' "${S3_BUCKET}" "${prefix}"
  else
    printf 's3://%s/' "${S3_BUCKET}"
  fi
}

rest_base_url() {
  printf 'http://%s.%s.svc.cluster.local:%s' "${SERVICE_NAME}" "${TARGET_NS}" "${SERVICE_PORT}"
}

normalized_s3_endpoint() {
  local endpoint="${S3_ENDPOINT}"
  if [[ -z "${endpoint}" ]]; then
    endpoint="https://s3.${AWS_REGION}.amazonaws.com"
  elif [[ "${endpoint}" != http://* && "${endpoint}" != https://* ]]; then
    endpoint="https://${endpoint}"
  fi
  trim_trailing_slash "${endpoint}"
}

postgres_jdbc_uri() {
  printf 'jdbc:postgresql://%s.%s.svc.cluster.local:%s/%s' \
    "${POSTGRES_SERVICE}" "${POSTGRES_NAMESPACE}" "${POSTGRES_PORT}" "${POSTGRES_DB}"
}

secret_fingerprint() {
  python3 - <<'PY'
import hashlib
import os

parts = [
    os.environ.get("AWS_REGION", ""),
    os.environ.get("S3_BUCKET", ""),
    os.environ.get("S3_PREFIX", ""),
    os.environ.get("S3_ENDPOINT", ""),
    os.environ.get("S3_PATH_STYLE_ACCESS", ""),
    os.environ.get("AWS_ACCESS_KEY_ID", ""),
    os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    os.environ.get("AWS_SESSION_TOKEN", ""),
]
h = hashlib.sha256()
for part in parts:
    h.update(part.encode("utf-8"))
    h.update(b"\0")
print(h.hexdigest())
PY
}

postgres_secret_fingerprint() {
  kubectl -n "${POSTGRES_NAMESPACE}" get secret "${POSTGRES_SECRET_NAME}" \
    -o "jsonpath={.data.${POSTGRES_USERNAME_KEY}}{.data.${POSTGRES_PASSWORD_KEY}}" 2>/dev/null \
    | sha256sum | awk '{print $1}'
}

require_postgres_secret() {
  if ! kubectl -n "${POSTGRES_NAMESPACE}" get secret "${POSTGRES_SECRET_NAME}" >/dev/null 2>&1; then
    fatal "missing Postgres secret ${POSTGRES_SECRET_NAME} in namespace ${POSTGRES_NAMESPACE}"
  fi

  local username_b64 password_b64
  username_b64="$(kubectl -n "${POSTGRES_NAMESPACE}" get secret "${POSTGRES_SECRET_NAME}" -o "jsonpath={.data.${POSTGRES_USERNAME_KEY}}" 2>/dev/null || true)"
  password_b64="$(kubectl -n "${POSTGRES_NAMESPACE}" get secret "${POSTGRES_SECRET_NAME}" -o "jsonpath={.data.${POSTGRES_PASSWORD_KEY}}" 2>/dev/null || true)"

  [[ -n "${username_b64}" ]] || fatal "secret ${POSTGRES_SECRET_NAME} missing key ${POSTGRES_USERNAME_KEY}"
  [[ -n "${password_b64}" ]] || fatal "secret ${POSTGRES_SECRET_NAME} missing key ${POSTGRES_PASSWORD_KEY}"
}

render_scheduling_block() {
  if [[ "${K8S_CLUSTER}" == "eks" ]]; then
    cat <<'EOF'
      nodeSelector:
        node-type: general
      tolerations:
      - key: node-type
        operator: Equal
        value: general
        effect: NoSchedule
EOF
  fi
}

render_serviceaccount() {
  if is_iam_mode; then
    [[ -n "${IAM_ROLE_ARN}" ]] || fatal "IAM_ROLE_ARN is required when USE_IAM=true"
    cat > "${MANIFEST_DIR}/serviceaccount.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  labels:
    app.kubernetes.io/name: iceberg-rest
  annotations:
    eks.amazonaws.com/role-arn: ${IAM_ROLE_ARN}
automountServiceAccountToken: true
EOF
  else
    cat > "${MANIFEST_DIR}/serviceaccount.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${TARGET_NS}
  labels:
    app.kubernetes.io/name: iceberg-rest
automountServiceAccountToken: false
EOF
  fi
}

render_static_aws_env() {
  local lines=""
  lines+=$'        - name: AWS_ACCESS_KEY_ID\n'
  lines+=$'          valueFrom:\n'
  lines+=$'            secretKeyRef:\n'
  lines+=$"              name: ${SECRET_NAME}\n"
  lines+=$'              key: AWS_ACCESS_KEY_ID\n'
  lines+=$'        - name: AWS_SECRET_ACCESS_KEY\n'
  lines+=$'          valueFrom:\n'
  lines+=$'            secretKeyRef:\n'
  lines+=$"              name: ${SECRET_NAME}\n"
  lines+=$'              key: AWS_SECRET_ACCESS_KEY\n'
  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    lines+=$'        - name: AWS_SESSION_TOKEN\n'
    lines+=$'          valueFrom:\n'
    lines+=$'            secretKeyRef:\n'
    lines+=$"              name: ${SECRET_NAME}\n"
    lines+=$'              key: AWS_SESSION_TOKEN\n'
  fi
  printf '%s' "${lines}"
}

render_secret() {
  if is_static_mode; then
    [[ -n "${AWS_ACCESS_KEY_ID}" ]] || fatal "AWS_ACCESS_KEY_ID is required when USE_IAM=false"
    [[ -n "${AWS_SECRET_ACCESS_KEY}" ]] || fatal "AWS_SECRET_ACCESS_KEY is required when USE_IAM=false"

    log "creating/updating AWS storage secret ${SECRET_NAME}"

    local args=(
      kubectl -n "${TARGET_NS}" create secret generic "${SECRET_NAME}"
      --dry-run=client
      -o yaml
      --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
      --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
    )

    if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
      args+=(--from-literal=AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}")
    fi

    "${args[@]}" | kubectl -n "${TARGET_NS}" apply -f - >/dev/null
  fi
}

render_deployment() {
  local warehouse s3_endpoint jdbc_uri aws_env scheduling_block
  warehouse="$(warehouse_uri)"
  s3_endpoint="$(normalized_s3_endpoint)"
  jdbc_uri="$(postgres_jdbc_uri)"
  aws_env="$(render_static_aws_env)"
  scheduling_block="$(render_scheduling_block)"

  cat > "${MANIFEST_DIR}/deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOYMENT_NAME}
  namespace: ${TARGET_NS}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${DEPLOYMENT_NAME}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  template:
    metadata:
      labels:
        app: ${DEPLOYMENT_NAME}
      annotations:
        ${ANNOTATION_KEY}: "pending"
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT_NAME}
$(printf '%s\n' "${scheduling_block}")
      terminationGracePeriodSeconds: 30
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
        fsGroupChangePolicy: OnRootMismatch
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: rest
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: ${CONTAINER_PORT}
        env:
        - name: REST_PORT
          value: "${CONTAINER_PORT}"
        - name: AWS_REGION
          value: "${AWS_REGION}"
        - name: AWS_DEFAULT_REGION
          value: "${AWS_REGION}"
        - name: AWS_EC2_METADATA_DISABLED
          value: "true"
        - name: CATALOG_URI
          value: "${jdbc_uri}"
        - name: CATALOG_JDBC_USER
          valueFrom:
            secretKeyRef:
              name: ${POSTGRES_SECRET_NAME}
              key: ${POSTGRES_USERNAME_KEY}
        - name: CATALOG_JDBC_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ${POSTGRES_SECRET_NAME}
              key: ${POSTGRES_PASSWORD_KEY}
        - name: CATALOG_WAREHOUSE
          value: "${warehouse}"
        - name: CATALOG_IO__IMPL
          value: "org.apache.iceberg.aws.s3.S3FileIO"
        - name: CATALOG_CLIENT_REGION
          value: "${AWS_REGION}"
        - name: CATALOG_S3_ENDPOINT
          value: "${s3_endpoint}"
        - name: CATALOG_S3_PATH_STYLE_ACCESS
          value: "${S3_PATH_STYLE_ACCESS}"
        - name: HOME
          value: "/tmp"
        - name: TMPDIR
          value: "/tmp"
$(if is_static_mode; then printf '%s\n' "${aws_env}"; fi)
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        startupProbe:
          httpGet:
            path: /v1/config
            port: ${CONTAINER_PORT}
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 60
        readinessProbe:
          httpGet:
            path: /v1/config
            port: ${CONTAINER_PORT}
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 24
        livenessProbe:
          httpGet:
            path: /v1/config
            port: ${CONTAINER_PORT}
          initialDelaySeconds: 20
          periodSeconds: 10
          timeoutSeconds: 2
          failureThreshold: 6
        resources:
          requests:
            cpu: "250m"
            memory: "512Mi"
          limits:
            cpu: "1000m"
            memory: "1Gi"
        volumeMounts:
        - name: tmp
          mountPath: /tmp
      volumes:
      - name: tmp
        emptyDir: {}
EOF
}

render_service() {
  cat > "${MANIFEST_DIR}/service.yaml" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${SERVICE_NAME}
  namespace: ${TARGET_NS}
spec:
  type: ClusterIP
  selector:
    app: ${DEPLOYMENT_NAME}
  ports:
  - name: http
    port: ${SERVICE_PORT}
    targetPort: ${CONTAINER_PORT}
EOF
}

render_pdb() {
  if bool_true "${ENABLE_PDB}"; then
    cat > "${MANIFEST_DIR}/pdb.yaml" <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ${PDB_NAME}
  namespace: ${TARGET_NS}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: ${DEPLOYMENT_NAME}
EOF
  else
    rm -f "${MANIFEST_DIR}/pdb.yaml" || true
  fi
}

compute_manifests_hash() {
  local tmp files=()
  tmp="$(mktemp)"
  files+=("${MANIFEST_DIR}/serviceaccount.yaml")
  files+=("${MANIFEST_DIR}/deployment.yaml")
  files+=("${MANIFEST_DIR}/service.yaml")
  if [[ -f "${MANIFEST_DIR}/pdb.yaml" ]]; then
    files+=("${MANIFEST_DIR}/pdb.yaml")
  fi
  cat "${files[@]}" > "${tmp}"

  printf '\nK8S_CLUSTER=%s\nUSE_IAM=%s\n' "${K8S_CLUSTER}" "${USE_IAM}" >> "${tmp}"
  printf 'AWS_REGION=%s\nS3_BUCKET=%s\nS3_PREFIX=%s\nS3_ENDPOINT=%s\nS3_PATH_STYLE_ACCESS=%s\n' \
    "${AWS_REGION}" "${S3_BUCKET}" "${S3_PREFIX}" "${S3_ENDPOINT}" "${S3_PATH_STYLE_ACCESS}" >> "${tmp}"

  if is_static_mode; then
    printf '%s\n' "$(secret_fingerprint)" >> "${tmp}"
  fi
  printf '%s\n' "$(postgres_secret_fingerprint)" >> "${tmp}"
  if is_iam_mode; then
    printf 'IAM_ROLE_ARN=%s\n' "${IAM_ROLE_ARN}" >> "${tmp}"
  fi

  sha256sum "${tmp}" | awk '{print $1}'
  rm -f "${tmp}"
}

apply_manifests() {
  local hash existing
  hash="$(compute_manifests_hash)"

  existing="$(
    kubectl -n "${TARGET_NS}" get deployment "${DEPLOYMENT_NAME}" \
      -o "jsonpath={.spec.template.metadata.annotations['${ANNOTATION_KEY}']}" 2>/dev/null || true
  )"

  if [[ "${existing}" == "${hash}" ]]; then
    log "manifests unchanged (hash match); skipping apply"
    return 0
  fi

  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/serviceaccount.yaml" >/dev/null
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/deployment.yaml" >/dev/null
  kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/service.yaml" >/dev/null
  if [[ -f "${MANIFEST_DIR}/pdb.yaml" ]]; then
    kubectl -n "${TARGET_NS}" apply -f "${MANIFEST_DIR}/pdb.yaml" >/dev/null
  fi

  kubectl -n "${TARGET_NS}" patch deployment "${DEPLOYMENT_NAME}" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"${ANNOTATION_KEY}\":\"${hash}\"}}}}}" >/dev/null

  log "applied manifests and wrote template annotation ${ANNOTATION_KEY}=${hash}"
}

# --- Diagnostics --------------------------------------------------------------

dump_diagnostics() {
  log "diagnostics: pods"
  kubectl -n "${TARGET_NS}" get pods -o wide 2>&1 || true

  log "diagnostics: deployment"
  kubectl -n "${TARGET_NS}" describe deployment "${DEPLOYMENT_NAME}" 2>&1 || true

  log "diagnostics: service"
  kubectl -n "${TARGET_NS}" get svc "${SERVICE_NAME}" -o wide 2>&1 || true

  log "diagnostics: endpoints"
  kubectl -n "${TARGET_NS}" get endpoints "${SERVICE_NAME}" -o wide 2>&1 || true

  log "diagnostics: recent events"
  kubectl get events -A --sort-by=.lastTimestamp 2>&1 | tail -n 80 || true

  log "diagnostics: logs"
  kubectl -n "${TARGET_NS}" logs deployment/"${DEPLOYMENT_NAME}" --tail=200 2>&1 || true
}

on_err() {
  local rc=$?
  dump_diagnostics
  exit "$rc"
}

wait_for_deployment_ready() {
  log "waiting for deployment availability (timeout=${READY_TIMEOUT}s)"
  kubectl -n "${TARGET_NS}" wait --for=condition=Available deployment/"${DEPLOYMENT_NAME}" --timeout="${READY_TIMEOUT}s" >/dev/null \
    || fatal "timeout waiting for deployment availability"
  log "deployment ready"
}

# --- Main Operations ----------------------------------------------------------

rollout() {
  local run_validate="${1:-false}"

  require_prereqs
  mkdir -p "${MANIFEST_DIR}"
  ensure_namespace
  require_postgres_secret

  log "starting iceberg rollout"
  log "namespace=${TARGET_NS}"
  log "cluster=${K8S_CLUSTER}"
  log "use_iam=${USE_IAM}"
  log "image=${IMAGE}"
  log "rest_url=$(rest_base_url)"
  log "warehouse=$(warehouse_uri)"
  log "aws_region=${AWS_REGION}"
  log "s3_endpoint=$(normalized_s3_endpoint)"
  log "s3_path_style_access=${S3_PATH_STYLE_ACCESS}"
  log "secret_name=${SECRET_NAME}"
  log "postgres_namespace=${POSTGRES_NAMESPACE}"
  log "postgres_service=${POSTGRES_SERVICE}"
  log "postgres_port=${POSTGRES_PORT}"
  log "postgres_db=${POSTGRES_DB}"
  log "postgres_secret_name=${POSTGRES_SECRET_NAME}"
  log "validate=${run_validate}"

  render_serviceaccount
  render_secret
  render_deployment
  render_service
  render_pdb
  apply_manifests
  wait_for_deployment_ready

  if [[ "${run_validate}" == "true" ]]; then
    if [[ -x "${VALIDATE_SCRIPT}" ]]; then
      log "running validation script ${VALIDATE_SCRIPT}"
      bash "${VALIDATE_SCRIPT}"
    elif [[ -f "${VALIDATE_SCRIPT}" ]]; then
      log "running validation script ${VALIDATE_SCRIPT}"
      bash "${VALIDATE_SCRIPT}"
    else
      fatal "validation script not found: ${VALIDATE_SCRIPT}"
    fi
  else
    log "skipping validation (pass --validate to enable)"
  fi

  log "[SUCCESS] iceberg rollout complete"
}

delete_all() {
  log "deleting iceberg resources"
  kubectl -n "${TARGET_NS}" delete deployment "${DEPLOYMENT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete svc "${SERVICE_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete sa "${SERVICE_ACCOUNT_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete secret "${SECRET_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${TARGET_NS}" delete pdb "${PDB_NAME}" --ignore-not-found >/dev/null 2>&1 || true
  rm -f \
    "${MANIFEST_DIR}/serviceaccount.yaml" \
    "${MANIFEST_DIR}/deployment.yaml" \
    "${MANIFEST_DIR}/service.yaml" \
    "${MANIFEST_DIR}/pdb.yaml" || true
  log "deleted iceberg resources; data in object storage preserved"
}

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --rollout [--validate]   Deploy Iceberg REST catalog (optionally run validation)
  --delete                 Remove all Iceberg resources
  --help, -h               Show this help message

Environment:
  K8S_CLUSTER=kind|eks
  USE_IAM=true|false
  IAM_ROLE_ARN=<arn>      Required when USE_IAM=true
  ENABLE_PDB=true|false

Examples:
  K8S_CLUSTER=kind USE_IAM=false $0 --rollout
  K8S_CLUSTER=eks USE_IAM=true IAM_ROLE_ARN=arn:aws:iam::123456789012:role/iceberg-s3 $0 --rollout --validate
EOF
}

# --- Entry Point --------------------------------------------------------------

main() {
  local run_validate="false"
  local action=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --rollout)
        action="rollout"
        shift
        ;;
      --validate)
        run_validate="true"
        shift
        ;;
      --delete)
        action="delete"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        fatal "unknown argument: $1"
        ;;
    esac
  done

  if [[ -z "${action}" ]]; then
    action="rollout"
  fi

  trap on_err ERR

  case "${action}" in
    rollout) rollout "${run_validate}" ;;
    delete) delete_all ;;
  esac
}

main "$@"
