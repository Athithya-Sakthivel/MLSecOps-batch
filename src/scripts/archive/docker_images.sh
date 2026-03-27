#!/usr/bin/env bash
set -euo pipefail

# =========================
# Required env
# =========================
: "${GIT_PAT:?GIT_PAT is required}"
: "${GHCR_USER:=athithya-sakthivel}"

# =========================
# Config
# =========================
SOURCE_IMAGES="${SOURCE_IMAGES:-ghcr.io/cloudnative-pg/postgresql:18.3-minimal-trixie,ghcr.io/cloudnative-pg/cloudnative-pg:1.28.1}"
TARGET_PREFIX="${TARGET_PREFIX:-ghcr.io/${GHCR_USER}}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
PUSH="${PUSH:-true}"

# Pinned Trivy image (immutable)
TRIVY_IMAGE="ghcr.io/athithya-sakthivel/trivy-safe-0.69.3@sha256:bcc376de8d77cfe086a917230e818dc9f8528e3c852f7b1aff648949b6258d1c"

# Fail on HIGH/CRITICAL by default
TRIVY_SEVERITY="${TRIVY_SEVERITY:-HIGH,CRITICAL}"

# =========================
# Utils
# =========================
log(){ printf "\033[0;34m[INFO]\033[0m %s\n" "$*"; }
err(){ printf "\033[0;31m[ERROR]\033[0m %s\n" "$*" >&2; }

retry() {
  local n=0
  until "$@"; do
    n=$((n+1))
    [ "$n" -ge 4 ] && return 1
    sleep $((2**n))
  done
}

require() {
  command -v "$1" >/dev/null || { err "$1 required"; exit 1; }
}

# =========================
# Preconditions
# =========================
require docker

echo "${GIT_PAT}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin

IFS=',' read -ra IMAGES <<< "$SOURCE_IMAGES"

# =========================
# Main loop
# =========================
for SRC in "${IMAGES[@]}"; do
  SRC="$(echo "$SRC" | xargs)"

  IMAGE_NAME="$(echo "$SRC" | awk -F/ '{print $NF}' | cut -d: -f1)"
  IMAGE_TAG="$(echo "$SRC" | awk -F: '{print $NF}')"

  TARGET="${TARGET_PREFIX}/${IMAGE_NAME}:${IMAGE_TAG}"

  log "Processing: $SRC → $TARGET"

  # Idempotency check
  if docker manifest inspect "$TARGET" >/dev/null 2>&1; then
    log "Already exists → skipping"
    continue
  fi

  # -------------------------
  # Pull (single arch for scan)
  # -------------------------
  log "Pulling image for scan"
  retry docker pull --platform linux/amd64 "$SRC"

  # -------------------------
  # Security scan (pre-push)
  # -------------------------
  log "Scanning with pinned Trivy"
  docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "$TRIVY_IMAGE" image \
  --scanners vuln \
  --severity "$TRIVY_SEVERITY" \
  --ignore-unfixed \
  --exit-code 1 \
  --no-progress \
  "$SRC"

  log "Scan passed"

  # -------------------------
  # Push (multi-arch mirror)
  # -------------------------
  if [ "$PUSH" = "true" ]; then
    log "Publishing multi-arch image"

    retry docker buildx imagetools create \
      --platform "$PLATFORMS" \
      -t "$TARGET" \
      "$SRC"

    log "Pushed → $TARGET"
  else
    log "PUSH=false → skipping push"
  fi

done

log "All images processed successfully"