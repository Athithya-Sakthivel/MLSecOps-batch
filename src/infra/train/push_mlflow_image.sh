#!/usr/bin/env bash
set -euo pipefail

: "${GIT_PAT:?GIT_PAT is required}"

GHCR_USER="${GHCR_USER:-athithya-sakthivel}"
IMAGE_NAME="${IMAGE_NAME:-mlflow}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-src/infra/train/Dockerfile.mlflow}"
IMAGE_TAG="${IMAGE_TAG:-$(TZ=Asia/Kolkata date +%Y-%m-%d-%H-%M)--$(git rev-parse --short HEAD)}"
FULL_IMAGE="ghcr.io/${GHCR_USER}/${IMAGE_NAME}:${IMAGE_TAG}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[ $(ts) ] [mlflow-image] $*" >&2; }
die() { log "FATAL: $*"; exit 1; }

cleanup() {
  if [[ -n "${DOCKER_CONFIG_DIR:-}" && -d "${DOCKER_CONFIG_DIR:-}" ]]; then
    rm -rf "${DOCKER_CONFIG_DIR}" || true
  fi
}
trap cleanup EXIT

log "validating prerequisites"

command -v docker >/dev/null 2>&1 || die "docker not found"
command -v git >/dev/null 2>&1 || die "git not found"

[[ -f "${DOCKERFILE_PATH}" ]] || die "Dockerfile not found at ${DOCKERFILE_PATH}"

DOCKER_CONFIG_DIR="$(mktemp -d)"
export DOCKER_CONFIG="${DOCKER_CONFIG_DIR}"

log "logging into GHCR as ${GHCR_USER}"
printf '%s' "${GIT_PAT}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin >/dev/null

log "building image"
log "image=${FULL_IMAGE}"
log "dockerfile=${DOCKERFILE_PATH}"

docker build \
  --no-cache \
  --pull \
  -t "${FULL_IMAGE}" \
  -f "${DOCKERFILE_PATH}" \
  .

log "verifying runtime dependencies inside image"

docker run --rm --entrypoint python "${FULL_IMAGE}" - <<'PY'
import os
import sys

print("[verify] python:", sys.version.split()[0])
print("[verify] euid:", os.geteuid())

if os.geteuid() == 0:
    raise SystemExit("[verify] expected non-root user, got root")

import mlflow
print("[verify] mlflow:", mlflow.__version__)

import psycopg2
print("[verify] psycopg2: OK")

import boto3
print("[verify] boto3:", boto3.__version__)

print("[verify] all checks passed")
PY

log "pushing image to GHCR"
docker push "${FULL_IMAGE}"

echo
echo "========================================"
echo "MLFLOW IMAGE PUSHED SUCCESSFULLY"
echo "========================================"
echo "Image:"
echo "  ${FULL_IMAGE}"
echo
echo "Next step:"
echo "  export MLFLOW_IMAGE=${FULL_IMAGE}"
echo "  python3 src/infra/train/mlflow_server.py"
echo "========================================"