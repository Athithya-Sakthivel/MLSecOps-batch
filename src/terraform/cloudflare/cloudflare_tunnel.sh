#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
umask 077

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: missing required command: $1" >&2
    exit 1
  }
}

require_cmd cloudflared
require_cmd curl
require_cmd jq

TUNNEL_NAME="${TUNNEL_NAME:-tabular-api-tunnel}"
DOMAIN="${DOMAIN:-${CLOUDFLARE_ZONE:-athithya.site}}"
AUTH_HOST="${AUTH_HOST:-auth.api.${DOMAIN}}"
PREDICT_HOST="${PREDICT_HOST:-predict.api.${DOMAIN}}"

if [[ -z "${CLOUDFLARE_API_TOKEN:-}" && -z "${CLOUDFLARE_API_KEY:-}" && -z "${CLOUDFLARE_GLOBAL_API_KEY:-}" ]]; then
  echo "ERROR: set CLOUDFLARE_API_TOKEN or CLOUDFLARE_API_KEY/CLOUDFLARE_GLOBAL_API_KEY" >&2
  exit 2
fi

if [[ -n "${CLOUDFLARE_API_TOKEN:-}" ]]; then
  CF_TOKEN="${CLOUDFLARE_API_TOKEN}"
else
  CF_TOKEN="${CLOUDFLARE_API_KEY:-${CLOUDFLARE_GLOBAL_API_KEY:-}}"
fi

CF_EMAIL="${CLOUDFLARE_EMAIL:-}"

cf_headers() {
  if [[ -n "${CLOUDFLARE_API_TOKEN:-}" ]]; then
    printf '%s\n' -H "Authorization: Bearer ${CF_TOKEN}"
  else
    printf '%s\n' -H "X-Auth-Key: ${CF_TOKEN}" -H "X-Auth-Email: ${CF_EMAIL}"
  fi
}

cf_curl() {
  local -a args=()
  while IFS= read -r line; do
    args+=("$line")
  done < <(cf_headers)
  curl -fsS "${args[@]}" "$@"
}

ensure_login() {
  if [[ ! -f "${HOME}/.cloudflared/cert.pem" ]]; then
    echo "[INFO] cloudflared login required"
    cloudflared tunnel login
  fi
}

get_tunnel_id() {
  cloudflared tunnel list --output json \
    | jq -r --arg n "${TUNNEL_NAME}" '.[] | select(.name == $n) | .id' \
    | head -n1
}

ensure_tunnel() {
  local tunnel_id
  tunnel_id="$(get_tunnel_id || true)"

  if [[ -z "${tunnel_id}" || "${tunnel_id}" == "null" ]]; then
    echo "[INFO] creating tunnel ${TUNNEL_NAME}"
    cloudflared tunnel create "${TUNNEL_NAME}" >/dev/null
    tunnel_id="$(get_tunnel_id)"
  else
    echo "[INFO] reusing tunnel ${TUNNEL_NAME} (${tunnel_id})"
  fi

  if [[ -z "${tunnel_id}" || "${tunnel_id}" == "null" ]]; then
    echo "ERROR: could not resolve tunnel ID" >&2
    exit 3
  fi

  echo "${tunnel_id}"
}

bind_dns() {
  local tunnel_id="$1"
  for host in "${AUTH_HOST}" "${PREDICT_HOST}"; do
    echo "[INFO] binding ${host} -> ${tunnel_id}.cfargotunnel.com"
    cloudflared tunnel route dns "${tunnel_id}" "${host}" >/dev/null
  done
}

verify_dns() {
  local tunnel_id="$1"
  local expected="${tunnel_id}.cfargotunnel.com"

  zone_id="$(
    cf_curl "https://api.cloudflare.com/client/v4/zones?name=${DOMAIN}&status=active&per_page=1" \
      | jq -r '.result[0].id // empty'
  )"

  if [[ -z "${zone_id}" ]]; then
    echo "ERROR: zone not found for ${DOMAIN}" >&2
    exit 4
  fi

  for host in "${AUTH_HOST}" "${PREDICT_HOST}"; do
    got="$(
      cf_curl "https://api.cloudflare.com/client/v4/zones/${zone_id}/dns_records?name=${host}" \
        | jq -r '.result[0].content // empty'
    )"
    if [[ "${got}" != "${expected}" ]]; then
      echo "ERROR: DNS verification failed for ${host}" >&2
      echo "expected: ${expected}" >&2
      echo "got:      ${got}" >&2
      exit 5
    fi
  done
}

ensure_login
TUNNEL_ID="$(ensure_tunnel)"
bind_dns "${TUNNEL_ID}"
verify_dns "${TUNNEL_ID}"

TUNNEL_TOKEN="$(cloudflared tunnel token "${TUNNEL_ID}")"

cat <<EOF
export TUNNEL_ID="${TUNNEL_ID}"
export TUNNEL_TOKEN="${TUNNEL_TOKEN}"
EOF