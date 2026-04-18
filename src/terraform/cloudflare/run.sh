#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

STACK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_BIN="${TF_BIN:-tofu}"

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
export TF_VAR_rate_limit_enabled="${TF_VAR_rate_limit_enabled:-true}"
export TF_VAR_rate_limit_action="${TF_VAR_rate_limit_action:-block}"
export TF_VAR_rate_limit_requests="${TF_VAR_rate_limit_requests:-60}"
export TF_VAR_rate_limit_period="${TF_VAR_rate_limit_period:-10}"
export TF_VAR_rate_limit_mitigation_timeout="${TF_VAR_rate_limit_mitigation_timeout:-10}"
export TUNNEL_NAME="${TUNNEL_NAME:-tabular-api-tunnel}"

: "${TF_VAR_account_id:?TF_VAR_account_id or CLOUDFLARE_ACCOUNT_ID is required}"
: "${TF_VAR_domain:?TF_VAR_domain or CLOUDFLARE_ZONE is required}"
: "${TF_VAR_pages_repo_owner:?TF_VAR_pages_repo_owner or GITHUB_OWNER is required}"
: "${TF_VAR_pages_repo_name:?TF_VAR_pages_repo_name or GITHUB_REPO is required}"

if [[ -n "${CLOUDFLARE_API_TOKEN:-}" && ( -n "${CLOUDFLARE_API_KEY:-}" || -n "${CLOUDFLARE_GLOBAL_API_KEY:-}" ) ]]; then
  echo "ERROR: set either CLOUDFLARE_API_TOKEN or CLOUDFLARE_GLOBAL_API_KEY/CLOUDFLARE_API_KEY, not both" >&2
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

cf_headers() {
  if [[ -n "${CLOUDFLARE_API_TOKEN:-}" ]]; then
    printf '%s\n' -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}"
  else
    printf '%s\n' -H "X-Auth-Key: ${CLOUDFLARE_API_KEY}" -H "X-Auth-Email: ${CLOUDFLARE_EMAIL}"
  fi
}

cf_curl() {
  local -a args=()
  while IFS= read -r line; do
    args+=("$line")
  done < <(cf_headers)
  curl -fsS "${args[@]}" "$@"
}

cf_status() {
  local -a args=()
  while IFS= read -r line; do
    args+=("$line")
  done < <(cf_headers)
  curl -sS -o /dev/null -w '%{http_code}' "${args[@]}" "$1"
}

state_has() {
  local addr="$1"
  "$TF_BIN" -chdir="${STACK_DIR}" state list 2>/dev/null | grep -qx "${addr}"
}

resolve_zone_id() {
  if [[ -n "${TF_VAR_zone_id:-}" && "${TF_VAR_zone_id}" != "your_zone_id" && "${TF_VAR_zone_id}" != "your_real_zone_id" && "${TF_VAR_zone_id}" != "replace-me" && "${TF_VAR_zone_id}" != "null" ]]; then
    return 0
  fi

  echo "[INFO] resolving zone_id for ${TF_VAR_domain}" >&2
  local zone_json
  zone_json="$(cf_curl "https://api.cloudflare.com/client/v4/zones?name=${TF_VAR_domain}&status=active&per_page=1")"
  TF_VAR_zone_id="$(jq -r '.result[0].id // empty' <<<"${zone_json}")"
  if [[ -z "${TF_VAR_zone_id}" ]]; then
    echo "ERROR: failed to resolve zone_id for ${TF_VAR_domain}" >&2
    exit 3
  fi
  export TF_VAR_zone_id
  echo "[INFO] zone_id=${TF_VAR_zone_id}" >&2
}

resolve_repo_id() {
  if [[ -n "${TF_VAR_pages_repo_id:-}" && "${TF_VAR_pages_repo_id}" != "replace-me" && "${TF_VAR_pages_repo_id}" != "your_repo_id" && "${TF_VAR_pages_repo_id}" != "null" ]]; then
    return 0
  fi

  echo "[INFO] resolving GitHub repo ID for ${TF_VAR_pages_repo_owner}/${TF_VAR_pages_repo_name}" >&2
  local gh_auth=()
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    gh_auth=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
  elif [[ -n "${GH_TOKEN:-}" ]]; then
    gh_auth=(-H "Authorization: Bearer ${GH_TOKEN}")
  fi

  local repo_json
  repo_json="$(curl -fsS -H "Accept: application/vnd.github+json" "${gh_auth[@]}" "https://api.github.com/repos/${TF_VAR_pages_repo_owner}/${TF_VAR_pages_repo_name}")"
  TF_VAR_pages_repo_id="$(jq -r '.id // empty' <<<"${repo_json}")"
  if [[ -z "${TF_VAR_pages_repo_id}" ]]; then
    echo "ERROR: failed to resolve GitHub repo ID" >&2
    exit 4
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
    | jq -r --arg n "${TUNNEL_NAME}" '.[] | select(.name == $n) | .id' \
    | head -n1
}

ensure_tunnel() {
  ensure_cloudflared_login

  local tunnel_id
  tunnel_id="$(get_tunnel_id || true)"

  if [[ -z "${tunnel_id}" || "${tunnel_id}" == "null" ]]; then
    echo "[INFO] creating tunnel ${TUNNEL_NAME}" >&2
    cloudflared tunnel create "${TUNNEL_NAME}" >&2
    tunnel_id="$(get_tunnel_id || true)"
  else
    echo "[INFO] reusing tunnel ${TUNNEL_NAME} (${tunnel_id})" >&2
  fi

  if [[ -z "${tunnel_id}" || "${tunnel_id}" == "null" ]]; then
    echo "ERROR: could not resolve tunnel ID" >&2
    exit 5
  fi

  echo "${tunnel_id}"
}

delete_dns_records_for_host() {
  local zone_id="$1"
  local host="$2"

  local record_ids
  record_ids="$(
    cf_curl "https://api.cloudflare.com/client/v4/zones/${zone_id}/dns_records?name=${host}&type=CNAME" \
      | jq -r '.result[]?.id'
  )"

  if [[ -z "${record_ids}" ]]; then
    return 0
  fi

  while IFS= read -r rid; do
    [[ -z "${rid}" ]] && continue
    echo "[INFO] deleting existing DNS record ${rid} for ${host}" >&2
    cf_curl -X DELETE "https://api.cloudflare.com/client/v4/zones/${zone_id}/dns_records/${rid}" >/dev/null
  done <<< "${record_ids}"
}

bind_tunnel_dns() {
  local tunnel_id="$1"
  local expected="${tunnel_id}.cfargotunnel.com"

  for host in "auth.api.${TF_VAR_domain}" "predict.api.${TF_VAR_domain}"; do
    delete_dns_records_for_host "${TF_VAR_zone_id}" "${host}"
    echo "[INFO] binding ${host} -> ${expected}" >&2
    cloudflared tunnel route dns "${tunnel_id}" "${host}" >&2
  done
}

cleanup_tunnel() {
  local tunnel_id
  tunnel_id="$(get_tunnel_id || true)"

  for host in "auth.api.${TF_VAR_domain}" "predict.api.${TF_VAR_domain}"; do
    delete_dns_records_for_host "${TF_VAR_zone_id}" "${host}"
  done

  if [[ -n "${tunnel_id}" && "${tunnel_id}" != "null" ]]; then
    echo "[INFO] deleting tunnel ${TUNNEL_NAME} (${tunnel_id})" >&2
    cloudflared tunnel delete -f "${tunnel_id}" >/dev/null || true
  fi
}

import_if_exists() {
  local addr="$1"
  local import_id="$2"

  if "$TF_BIN" -chdir="${STACK_DIR}" state show "${addr}" >/dev/null 2>&1; then
    return 0
  fi

  echo "[INFO] importing ${addr}" >&2
  "$TF_BIN" -chdir="${STACK_DIR}" import -input=false "${addr}" "${import_id}"
}

import_cloudflare_pages_project_if_exists() {
  local status
  status="$(cf_status "https://api.cloudflare.com/client/v4/accounts/${TF_VAR_account_id}/pages/projects/${TF_VAR_pages_project_name}")"
  if [[ "${status}" == "200" ]]; then
    import_if_exists "cloudflare_pages_project.frontend" "${TF_VAR_account_id}/${TF_VAR_pages_project_name}"
  fi
}

import_cloudflare_pages_domain_if_exists() {
  local domain_name="app.${TF_VAR_domain}"
  local status
  status="$(cf_status "https://api.cloudflare.com/client/v4/accounts/${TF_VAR_account_id}/pages/projects/${TF_VAR_pages_project_name}/domains/${domain_name}")"
  if [[ "${status}" == "200" ]]; then
    import_if_exists "cloudflare_pages_domain.frontend_domain" "${TF_VAR_account_id}/${TF_VAR_pages_project_name}/${domain_name}"
  fi
}

import_frontend_dns_if_exists() {
  local domain_name="app.${TF_VAR_domain}"
  local record_json record_id
  record_json="$(cf_curl "https://api.cloudflare.com/client/v4/zones/${TF_VAR_zone_id}/dns_records?name=${domain_name}&type=CNAME")"
  record_id="$(jq -r '.result[0].id // empty' <<<"${record_json}")"
  if [[ -n "${record_id}" ]]; then
    import_if_exists "cloudflare_dns_record.frontend_cname" "${TF_VAR_zone_id}/${record_id}"
  fi
}


import_ruleset_if_exists() {
  local addr="$1"
  local name="$2"
  local phase="$3"

  local rulesets_json ruleset_id import_id

  rulesets_json="$(
    cf_curl "https://api.cloudflare.com/client/v4/zones/${TF_VAR_zone_id}/rulesets"
  )"

  ruleset_id="$(
    jq -r \
      --arg name "${name}" \
      --arg phase "${phase}" \
      '
      .result[]
      | select(.name == $name and .phase == $phase)
      | .id
      ' <<<"${rulesets_json}" | head -n1
  )"

  if [[ -n "${ruleset_id}" && "${ruleset_id}" != "null" ]]; then
    # Cloudflare provider expects:
    # zones/<zone_id>/<ruleset_id>
    import_id="zones/${TF_VAR_zone_id}/${ruleset_id}"

    import_if_exists "${addr}" "${import_id}"
  fi
}

resolve_zone_id
resolve_repo_id

if [[ "${MODE}" != "--destroy" ]]; then
  TUNNEL_ID="$(ensure_tunnel)"
  bind_tunnel_dns "${TUNNEL_ID}"
fi

"$TF_BIN" -chdir="${STACK_DIR}" init -input=false -upgrade
"$TF_BIN" -chdir="${STACK_DIR}" validate

if [[ "${MODE}" != "--destroy" ]]; then
  import_cloudflare_pages_project_if_exists
  import_cloudflare_pages_domain_if_exists
  import_frontend_dns_if_exists
  import_ruleset_if_exists "cloudflare_ruleset.zone_custom_firewall" "zone-custom-firewall" "http_request_firewall_custom"
  import_ruleset_if_exists "cloudflare_ruleset.zone_rate_limit[0]" "zone-rate-limit" "http_ratelimit"
fi

case "${MODE}" in
  --plan)
    "$TF_BIN" -chdir="${STACK_DIR}" plan -input=false -out=tfplan
    ;;
  --apply)
    "$TF_BIN" -chdir="${STACK_DIR}" apply -input=false -auto-approve
    "$TF_BIN" -chdir="${STACK_DIR}" output
    ;;
  --destroy)
    "$TF_BIN" -chdir="${STACK_DIR}" destroy -input=false -auto-approve
    cleanup_tunnel
    ;;
esac