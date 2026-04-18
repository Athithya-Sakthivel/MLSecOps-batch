#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

STACK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_BIN="${TF_BIN:-tofu}"
TUNNEL_ENV_FILE="${TUNNEL_ENV_FILE:-${STACK_DIR}/.tunnel.env}"

usage() {
  cat <<'USAGE'
Usage: run.sh --plan|--apply|--destroy
USAGE
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: missing required command: $1" >&2
    exit 1
  }
}

require_cmd "$TF_BIN"
require_cmd curl
require_cmd jq
require_cmd cloudflared

[ $# -eq 1 ] || usage
MODE="$1"
case "$MODE" in
  --plan|--apply|--destroy) ;;
  *) usage ;;
esac

export TF_VAR_account_id="${TF_VAR_account_id:-${CLOUDFLARE_ACCOUNT_ID:-}}"
export TF_VAR_zone_id="${TF_VAR_zone_id:-${CLOUDFLARE_ZONE_ID:-}}"
export TF_VAR_domain="${TF_VAR_domain:-${CLOUDFLARE_ZONE:-${DOMAIN:-}}}"
export TF_VAR_pages_repo_owner="${TF_VAR_pages_repo_owner:-${GITHUB_OWNER:-}}"
export TF_VAR_pages_repo_name="${TF_VAR_pages_repo_name:-${GITHUB_REPO:-}}"
export TF_VAR_pages_repo_id="${TF_VAR_pages_repo_id:-${GITHUB_REPOSITORY_ID:-}}"
export TF_VAR_pages_project_name="${TF_VAR_pages_project_name:-tabular-ui}"
export TF_VAR_pages_branch="${TF_VAR_pages_branch:-main}"
export TF_VAR_pages_root_dir="${TF_VAR_pages_root_dir:-.}"
export TF_VAR_pages_destination_dir="${TF_VAR_pages_destination_dir:-dist}"
export TF_VAR_tunnel_name="${TF_VAR_tunnel_name:-tabular-api-tunnel}"
export TF_VAR_rate_limit_enabled="${TF_VAR_rate_limit_enabled:-true}"
export TF_VAR_rate_limit_action="${TF_VAR_rate_limit_action:-managed_challenge}"
export TF_VAR_rate_limit_requests="${TF_VAR_rate_limit_requests:-60}"
export TF_VAR_rate_limit_period="${TF_VAR_rate_limit_period:-60}"
export TF_VAR_rate_limit_mitigation_timeout="${TF_VAR_rate_limit_mitigation_timeout:-0}"

: "${TF_VAR_account_id:?TF_VAR_account_id or CLOUDFLARE_ACCOUNT_ID is required}"
: "${TF_VAR_domain:?TF_VAR_domain or CLOUDFLARE_ZONE is required}"
: "${TF_VAR_pages_repo_owner:?TF_VAR_pages_repo_owner or GITHUB_OWNER is required}"
: "${TF_VAR_pages_repo_name:?TF_VAR_pages_repo_name or GITHUB_REPO is required}"

if [[ -n "${CLOUDFLARE_API_TOKEN:-}" && ( -n "${CLOUDFLARE_API_KEY:-}" || -n "${CLOUDFLARE_GLOBAL_API_KEY:-}" ) ]]; then
  echo "ERROR: set either CLOUDFLARE_API_TOKEN or CLOUDFLARE_API_KEY/CLOUDFLARE_GLOBAL_API_KEY, not both" >&2
  exit 2
fi

if [[ -n "${CLOUDFLARE_API_TOKEN:-}" ]]; then
  export CLOUDFLARE_API_TOKEN
  unset CLOUDFLARE_API_KEY
  unset CLOUDFLARE_GLOBAL_API_KEY
  unset CLOUDFLARE_EMAIL
else
  export CLOUDFLARE_API_KEY="${CLOUDFLARE_API_KEY:-${CLOUDFLARE_GLOBAL_API_KEY:-}}"
  : "${CLOUDFLARE_API_KEY:?set CLOUDFLARE_API_TOKEN or CLOUDFLARE_GLOBAL_API_KEY}"
  : "${CLOUDFLARE_EMAIL:?CLOUDFLARE_EMAIL is required with a global API key}"
  export CLOUDFLARE_API_KEY
  export CLOUDFLARE_EMAIL
  unset CLOUDFLARE_API_TOKEN
fi

placeholder_values=(
  "your_zone_id"
  "your_real_zone_id"
  "your_repo_id"
  "replace-me"
  "your_real_global_api_key"
)
for v in "${placeholder_values[@]}"; do
  if [[ "${TF_VAR_zone_id:-}" == "$v" || "${TF_VAR_pages_repo_id:-}" == "$v" || "${CLOUDFLARE_GLOBAL_API_KEY:-}" == "$v" ]]; then
    echo "ERROR: placeholder value still present: $v" >&2
    exit 3
  fi
done

cf_headers() {
  if [[ -n "${CLOUDFLARE_API_TOKEN:-}" ]]; then
    printf '%s\n' -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}"
  else
    printf '%s\n' -H "X-Auth-Key: ${CLOUDFLARE_API_KEY}" -H "X-Auth-Email: ${CLOUDFLARE_EMAIL}"
  fi
}

cf_request() {
  local method="$1"
  shift
  local -a args=()
  while IFS= read -r line; do
    args+=("$line")
  done < <(cf_headers)
  curl -fsS -X "$method" "${args[@]}" "$@"
}

resolve_zone_id() {
  if [[ -n "${TF_VAR_zone_id:-}" ]]; then
    return 0
  fi
  echo "[INFO] resolving zone_id for ${TF_VAR_domain}" >&2
  zone_json="$(
    cf_request GET "https://api.cloudflare.com/client/v4/zones?name=${TF_VAR_domain}&status=active&per_page=1"
  )"
  TF_VAR_zone_id="$(jq -r '.result[0].id // empty' <<<"${zone_json}")"
  if [[ -z "${TF_VAR_zone_id}" ]]; then
    echo "ERROR: failed to resolve zone_id for ${TF_VAR_domain}" >&2
    exit 4
  fi
  export TF_VAR_zone_id
  echo "[INFO] zone_id=${TF_VAR_zone_id}" >&2
}

resolve_repo_id() {
  if [[ -n "${TF_VAR_pages_repo_id:-}" ]]; then
    return 0
  fi
  echo "[INFO] resolving GitHub repo ID for ${TF_VAR_pages_repo_owner}/${TF_VAR_pages_repo_name}" >&2
  gh_auth=()
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    gh_auth=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
  elif [[ -n "${GH_TOKEN:-}" ]]; then
    gh_auth=(-H "Authorization: Bearer ${GH_TOKEN}")
  fi
  repo_json="$(curl -fsS -H "Accept: application/vnd.github+json" "${gh_auth[@]}" "https://api.github.com/repos/${TF_VAR_pages_repo_owner}/${TF_VAR_pages_repo_name}")"
  TF_VAR_pages_repo_id="$(jq -r '.id // empty' <<<"${repo_json}")"
  if [[ -z "${TF_VAR_pages_repo_id}" ]]; then
    echo "ERROR: failed to resolve GitHub repo ID" >&2
    exit 5
  fi
  export TF_VAR_pages_repo_id
  echo "[INFO] pages_repo_id=${TF_VAR_pages_repo_id}" >&2
}

ensure_cloudflared_login() {
  if [[ ! -f "${HOME}/.cloudflared/cert.pem" ]]; then
    echo "[INFO] cloudflared login required" >&2
    cloudflared tunnel login >&2
  fi
}

get_tunnel_id() {
  cloudflared tunnel list --output json \
    | jq -r --arg n "${TF_VAR_tunnel_name}" '.[] | select(.name == $n) | .id' \
    | head -n1
}

ensure_tunnel() {
  local tunnel_id
  tunnel_id="$(get_tunnel_id || true)"

  if [[ -z "${tunnel_id}" || "${tunnel_id}" == "null" ]]; then
    echo "[INFO] creating tunnel ${TF_VAR_tunnel_name}" >&2
    cloudflared tunnel create "${TF_VAR_tunnel_name}" >/dev/null
    tunnel_id="$(get_tunnel_id || true)"
  else
    echo "[INFO] reusing tunnel ${TF_VAR_tunnel_name} (${tunnel_id})" >&2
  fi

  if [[ -z "${tunnel_id}" || "${tunnel_id}" == "null" ]]; then
    echo "ERROR: could not resolve tunnel ID" >&2
    exit 6
  fi

  printf '%s\n' "${tunnel_id}"
}

upsert_cname() {
  local record_name="$1"
  local record_target="$2"

  local existing_json
  existing_json="$(
    cf_request GET "https://api.cloudflare.com/client/v4/zones/${TF_VAR_zone_id}/dns_records?type=CNAME&name=${record_name}"
  )"

  local existing_id existing_content
  existing_id="$(jq -r '.result[0].id // empty' <<<"${existing_json}")"
  existing_content="$(jq -r '.result[0].content // empty' <<<"${existing_json}")"

  if [[ -n "${existing_id}" && "${existing_content}" == "${record_target}" ]]; then
    echo "[INFO] DNS already correct for ${record_name}" >&2
    return 0
  fi

  local payload
  payload="$(jq -n --arg type "CNAME" --arg name "${record_name}" --arg content "${record_target}" '{type:$type,name:$name,content:$content,proxied:true,ttl:1}')"

  if [[ -n "${existing_id}" ]]; then
    echo "[INFO] updating DNS ${record_name} -> ${record_target}" >&2
    cf_request PUT "https://api.cloudflare.com/client/v4/zones/${TF_VAR_zone_id}/dns_records/${existing_id}" --json "${payload}" >/dev/null
  else
    echo "[INFO] creating DNS ${record_name} -> ${record_target}" >&2
    cf_request POST "https://api.cloudflare.com/client/v4/zones/${TF_VAR_zone_id}/dns_records" --json "${payload}" >/dev/null
  fi
}

fetch_tunnel_token() {
  local tunnel_id="$1"
  local token_json token_value
  token_json="$(
    cf_request GET "https://api.cloudflare.com/client/v4/accounts/${TF_VAR_account_id}/cfd_tunnel/${tunnel_id}/token"
  )"
  token_value="$(jq -r 'if type=="object" and has("result") then .result elif type=="string" then . else empty end' <<<"${token_json}")"
  if [[ -z "${token_value}" ]]; then
    echo "ERROR: failed to fetch tunnel token for ${tunnel_id}" >&2
    exit 7
  fi
  printf '%s\n' "${token_value}"
}

write_tunnel_env_file() {
  local tunnel_id="$1"
  local tunnel_token="$2"
  cat >"${TUNNEL_ENV_FILE}" <<EOF
export TUNNEL_ID="${tunnel_id}"
export TUNNEL_TOKEN="${tunnel_token}"
EOF
  chmod 600 "${TUNNEL_ENV_FILE}"
  echo "[INFO] wrote ${TUNNEL_ENV_FILE}" >&2
}

resolve_zone_id
resolve_repo_id

if [[ "${MODE}" != "--destroy" ]]; then
  ensure_cloudflared_login
  TUNNEL_ID="$(ensure_tunnel)"
  upsert_cname "auth.api.${TF_VAR_domain}" "${TUNNEL_ID}.cfargotunnel.com"
  upsert_cname "predict.api.${TF_VAR_domain}" "${TUNNEL_ID}.cfargotunnel.com"
  TUNNEL_TOKEN="$(fetch_tunnel_token "${TUNNEL_ID}")"
  write_tunnel_env_file "${TUNNEL_ID}" "${TUNNEL_TOKEN}"
fi

"${TF_BIN}" -chdir="${STACK_DIR}" init -input=false -upgrade
"${TF_BIN}" -chdir="${STACK_DIR}" validate

case "${MODE}" in
  --plan)
    "${TF_BIN}" -chdir="${STACK_DIR}" plan -input=false -out=tfplan
    ;;
  --apply)
    "${TF_BIN}" -chdir="${STACK_DIR}" apply -input=false -auto-approve
    "${TF_BIN}" -chdir="${STACK_DIR}" output
    if [[ -f "${TUNNEL_ENV_FILE}" ]]; then
      echo "[INFO] source ${TUNNEL_ENV_FILE} to load TUNNEL_ID and TUNNEL_TOKEN" >&2
    fi
    ;;
  --destroy)
    "${TF_BIN}" -chdir="${STACK_DIR}" destroy -input=false -auto-approve
    ;;
esac