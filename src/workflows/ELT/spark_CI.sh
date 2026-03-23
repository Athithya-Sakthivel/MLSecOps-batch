#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
umask 022

log() { printf '\033[0;34m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m[WARN]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[0;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_cmd docker
require_cmd curl
require_cmd git
require_cmd sha256sum
require_cmd awk
require_cmd tar

repo_root="${GITHUB_WORKSPACE:-$(pwd)}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${repo_root}"

IMAGE_NAME="${IMAGE_NAME:-flyte-elt-spark-base}"
IMAGE_TAG="${IMAGE_TAG:-${GITHUB_SHA:-dev}}"
BUILD_CONTEXT="${BUILD_CONTEXT:-src/workflows/ELT}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-src/workflows/ELT/Dockerfile.spark}"
PLATFORMS="${PLATFORMS:-linux/amd64}"
REGISTRY_TYPE="${REGISTRY_TYPE:-ghcr}"

GHCR_USERNAME="${GHCR_USERNAME:-athithya-sakthivel}"
GHCR_IMAGE_REPO="${GHCR_IMAGE_REPO:-ghcr.io/${GHCR_USERNAME}/${IMAGE_NAME}}"

ECR_IMAGE_REPO="${ECR_IMAGE_REPO:-}"
AWS_REGION="${AWS_REGION:-ap-south-1}"

TRIVY_IMAGE="${TRIVY_IMAGE:-aquasec/trivy@sha256:3d1f862cb6c4fe13c1506f96f816096030d8d5ccdb2380a3069f7bf07daa86aa}"
TRIVY_SCANNERS="${TRIVY_SCANNERS:-vuln,secret,misconfig,license}"
TRIVY_SEVERITY="${TRIVY_SEVERITY:-HIGH,CRITICAL}"
TRIVY_IGNORE_UNFIXED="${TRIVY_IGNORE_UNFIXED:-true}"
TRIVY_TIMEOUT="${TRIVY_TIMEOUT:-10m}"

GITLEAKS_VERSION="${GITLEAKS_VERSION:-8.30.1}"
GITLEAKS_CONFIG="${GITLEAKS_CONFIG:-.gitleaks.toml}"

PUSH_IMAGE="${PUSH_IMAGE:-true}"
GIT_PAT="${GIT_PAT:-}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

ARTIFACT_DIR="${ARTIFACT_DIR:-${repo_root}/.spark-ci-artifacts}"
TRIVY_CACHE_DIR="${TRIVY_CACHE_DIR:-${ARTIFACT_DIR}/trivy-cache}"
TEMP_DIR="$(mktemp -d)"
BUILDER_NAME="spark-ci-${GITHUB_RUN_ID:-local}"

mkdir -p "${ARTIFACT_DIR}" "${TRIVY_CACHE_DIR}"
rm -f "${ARTIFACT_DIR}"/*.sarif 2>/dev/null || true

export PATH="${TEMP_DIR}:${PATH}"

cleanup() {
  docker buildx rm -f "${BUILDER_NAME}" >/dev/null 2>&1 || true
  rm -rf "${TEMP_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

resolve_gitleaks_config() {
  local input="${GITLEAKS_CONFIG}"
  local candidates=()
  local candidate

  if [ -n "${input}" ]; then
    case "${input}" in
      /*) candidates+=("${input}") ;;
      *) candidates+=("${repo_root}/${input}" "${script_dir}/${input}") ;;
    esac
  fi

  candidates+=(
    "${repo_root}/.gitleaks.toml"
    "${script_dir}/.gitleaks.toml"
  )

  for candidate in "${candidates[@]}"; do
    if [ -f "${candidate}" ]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

install_gitleaks() {
  local asset base_url checksum_file tarball expected_sha actual_sha

  case "$(uname -m)" in
    x86_64) asset="gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz" ;;
    aarch64|arm64) asset="gitleaks_${GITLEAKS_VERSION}_linux_arm64.tar.gz" ;;
    *)
      die "unsupported architecture for gitleaks: $(uname -m)"
      ;;
  esac

  base_url="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}"
  checksum_file="${TEMP_DIR}/gitleaks_checksums.txt"
  tarball="${TEMP_DIR}/${asset}"

  log "Downloading Gitleaks ${GITLEAKS_VERSION}"
  curl -fsSLo "${tarball}" "${base_url}/${asset}"
  curl -fsSLo "${checksum_file}" "${base_url}/gitleaks_${GITLEAKS_VERSION}_checksums.txt"

  expected_sha="$(awk -v asset="${asset}" '$0 ~ asset"$" {gsub(/^sha256:/,"",$1); print $1}' "${checksum_file}")"
  actual_sha="$(sha256sum "${tarball}" | awk '{print $1}')"

  [ -n "${expected_sha}" ] || die "checksum not found for ${asset}"
  [ "${expected_sha}" = "${actual_sha}" ] || die "checksum mismatch for ${asset}"

  tar -xzf "${tarball}" -C "${TEMP_DIR}"
  chmod 0755 "${TEMP_DIR}/gitleaks"
  gitleaks version
}

ensure_builder() {
  if docker buildx inspect "${BUILDER_NAME}" >/dev/null 2>&1; then
    docker buildx rm -f "${BUILDER_NAME}" >/dev/null 2>&1 || true
  fi

  docker buildx create --name "${BUILDER_NAME}" --use >/dev/null
  docker buildx inspect --bootstrap >/dev/null
}

validate_inputs() {
  [ -d "${BUILD_CONTEXT}" ] || die "build context not found: ${BUILD_CONTEXT}"
  [ -f "${DOCKERFILE_PATH}" ] || die "dockerfile not found: ${DOCKERFILE_PATH}"
  [ -n "${PLATFORMS}" ] || die "PLATFORMS is empty"

  case "${REGISTRY_TYPE}" in
    ghcr|ecr) ;;
    *) die "REGISTRY_TYPE must be ghcr or ecr" ;;
  esac

  if [ "${PUSH_IMAGE}" = "true" ]; then
    case "${REGISTRY_TYPE}" in
      ghcr)
        [ -n "${GHCR_IMAGE_REPO}" ] || die "GHCR_IMAGE_REPO is required for REGISTRY_TYPE=ghcr when PUSH_IMAGE=true"
        ;;
      ecr)
        [ -n "${ECR_IMAGE_REPO}" ] || die "ECR_IMAGE_REPO is required for REGISTRY_TYPE=ecr when PUSH_IMAGE=true"
        [ -n "${AWS_REGION}" ] || die "AWS_REGION is required for REGISTRY_TYPE=ecr when PUSH_IMAGE=true"
        ;;
    esac
  fi
}

registry_repo() {
  case "${REGISTRY_TYPE}" in
    ghcr)
      echo "${GHCR_IMAGE_REPO}"
      ;;
    ecr)
      echo "${ECR_IMAGE_REPO}"
      ;;
    *)
      die "REGISTRY_TYPE must be ghcr or ecr"
      ;;
  esac
}

run_gitleaks() {
  local report="${ARTIFACT_DIR}/gitleaks.sarif"
  local config_path=""

  log "Running Gitleaks"
  if config_path="$(resolve_gitleaks_config)"; then
    log "Running Gitleaks with config: ${config_path}"
    gitleaks detect \
      --source "${repo_root}" \
      --config "${config_path}" \
      --redact \
      --no-banner \
      --report-format sarif \
      --report-path "${report}" \
      --exit-code 1
  else
    warn "No Gitleaks config found; running with default rules"
    gitleaks detect \
      --source "${repo_root}" \
      --redact \
      --no-banner \
      --report-format sarif \
      --report-path "${report}" \
      --exit-code 1
  fi
}

run_trivy_fs() {
  local report_name="trivy-fs.sarif"
  local trivy_args=()

  log "Running Trivy filesystem scan"
  trivy_args=(
    fs
    --cache-dir /root/.cache/trivy
    --scanners "${TRIVY_SCANNERS}"
    --severity "${TRIVY_SEVERITY}"
    --timeout "${TRIVY_TIMEOUT}"
    --exit-code 1
    --format sarif
    --output "/reports/${report_name}"
    "${BUILD_CONTEXT}"
  )

  if [ "${TRIVY_IGNORE_UNFIXED}" = "true" ]; then
    trivy_args=(fs
      --cache-dir /root/.cache/trivy
      --scanners "${TRIVY_SCANNERS}"
      --severity "${TRIVY_SEVERITY}"
      --ignore-unfixed
      --timeout "${TRIVY_TIMEOUT}"
      --exit-code 1
      --format sarif
      --output "/reports/${report_name}"
      "${BUILD_CONTEXT}"
    )
  fi

  docker run --rm \
    -v "${repo_root}:/repo:ro" \
    -v "${ARTIFACT_DIR}:/reports" \
    -v "${TRIVY_CACHE_DIR}:/root/.cache/trivy" \
    -w /repo \
    "${TRIVY_IMAGE}" \
    "${trivy_args[@]}"
}

build_and_scan_platform() {
  local platform="$1"
  local safe_platform temp_tag image_report trivy_args=()

  safe_platform="${platform//\//_}"
  safe_platform="${safe_platform//,/__}"
  temp_tag="${IMAGE_NAME}:${IMAGE_TAG}-${safe_platform}"
  image_report="trivy-image-${safe_platform}.sarif"

  log "Building local image for scan: ${temp_tag} (${platform})"
  docker buildx build \
    --builder "${BUILDER_NAME}" \
    --platform "${platform}" \
    --file "${DOCKERFILE_PATH}" \
    --tag "${temp_tag}" \
    --load \
    --pull \
    --provenance=false \
    --sbom=false \
    "${BUILD_CONTEXT}"

  log "Running Trivy image scan: ${temp_tag}"
  trivy_args=(
    image
    --cache-dir /root/.cache/trivy
    --scanners "${TRIVY_SCANNERS}"
    --severity "${TRIVY_SEVERITY}"
    --timeout "${TRIVY_TIMEOUT}"
    --exit-code 1
    --format sarif
    --output "/reports/${image_report}"
    "${temp_tag}"
  )

  if [ "${TRIVY_IGNORE_UNFIXED}" = "true" ]; then
    trivy_args=(image
      --cache-dir /root/.cache/trivy
      --scanners "${TRIVY_SCANNERS}"
      --severity "${TRIVY_SEVERITY}"
      --ignore-unfixed
      --timeout "${TRIVY_TIMEOUT}"
      --exit-code 1
      --format sarif
      --output "/reports/${image_report}"
      "${temp_tag}"
    )
  fi

  docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v "${ARTIFACT_DIR}:/reports" \
    -v "${TRIVY_CACHE_DIR}:/root/.cache/trivy" \
    -w /repo \
    "${TRIVY_IMAGE}" \
    "${trivy_args[@]}"
}

login_ghcr() {
  local token username

  if [ -n "${GIT_PAT}" ]; then
    token="${GIT_PAT}"
    username="${GHCR_USERNAME:-${GITHUB_ACTOR:-}}"
  else
    token="${GITHUB_TOKEN:-}"
    username="${GHCR_USERNAME:-${GITHUB_ACTOR:-}}"
  fi

  [ -n "${token}" ] || die "GIT_PAT or GITHUB_TOKEN is required for GHCR push"
  [ -n "${username}" ] || die "GHCR_USERNAME or GITHUB_ACTOR is required for GHCR push"

  log "Logging in to GHCR as ${username}"
  printf '%s\n' "${token}" | docker login ghcr.io -u "${username}" --password-stdin >/dev/null
}

login_ecr() {
  local registry_host

  [ -n "${ECR_IMAGE_REPO}" ] || die "ECR_IMAGE_REPO is required for ECR push"
  [ -n "${AWS_REGION}" ] || die "AWS_REGION is required for ECR push"
  command -v aws >/dev/null 2>&1 || die "aws CLI not found; required for ECR login"

  registry_host="${ECR_IMAGE_REPO%%/*}"
  [ "${registry_host}" != "${ECR_IMAGE_REPO}" ] || die "ECR_IMAGE_REPO must include a repository path, e.g. 123456789012.dkr.ecr.ap-south-1.amazonaws.com/repo-name"

  log "Logging in to ECR registry ${registry_host}"
  aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${registry_host}" >/dev/null
}

push_multiarch_manifest() {
  local image_repo

  image_repo="$(registry_repo)"
  [ -n "${image_repo}" ] || die "registry repository is empty"

  log "Pushing multi-arch image: ${image_repo}:${IMAGE_TAG}"
  docker buildx build \
    --builder "${BUILDER_NAME}" \
    --platform "${PLATFORMS}" \
    --file "${DOCKERFILE_PATH}" \
    --tag "${image_repo}:${IMAGE_TAG}" \
    --provenance=true \
    --sbom=true \
    --push \
    "${BUILD_CONTEXT}"
}

main() {
  PLATFORMS="${PLATFORMS// /}"
  validate_inputs

  log "IMAGE_NAME=${IMAGE_NAME}"
  log "IMAGE_TAG=${IMAGE_TAG}"
  log "BUILD_CONTEXT=${BUILD_CONTEXT}"
  log "DOCKERFILE_PATH=${DOCKERFILE_PATH}"
  log "PLATFORMS=${PLATFORMS}"
  log "REGISTRY_TYPE=${REGISTRY_TYPE}"
  log "PUSH_IMAGE=${PUSH_IMAGE}"
  log "ARTIFACT_DIR=${ARTIFACT_DIR}"
  log "TRIVY_CACHE_DIR=${TRIVY_CACHE_DIR}"

  install_gitleaks
  ensure_builder

  run_gitleaks
  run_trivy_fs

  IFS=',' read -r -a platform_list <<< "${PLATFORMS}"
  for platform in "${platform_list[@]}"; do
    [ -n "${platform}" ] || continue
    build_and_scan_platform "${platform}"
  done

  if [ "${PUSH_IMAGE}" = "true" ]; then
    case "${REGISTRY_TYPE}" in
      ghcr)
        login_ghcr
        ;;
      ecr)
        login_ecr
        ;;
      *)
        die "REGISTRY_TYPE must be ghcr or ecr"
        ;;
    esac
    push_multiarch_manifest
  else
    warn "PUSH_IMAGE=false; skipping registry push"
  fi

  log "Spark CI complete"
}

main "$@"