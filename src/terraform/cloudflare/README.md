# Cloudflare + OpenTofu Setup

This repository uses three separate pieces:

1. **OpenTofu** for Cloudflare Pages, DNS, WAF, and rate limiting.
2. **`cloudflare_tunnel.sh`** for tunnel creation and tunnel DNS bindings.
3. **Environment variables** as the single source of truth for credentials and IDs.

Cloudflare’s current Terraform docs treat Pages projects, WAF custom rules, managed rules, and rate limiting as separate configurable features, and Cloudflare Tunnel is an outbound-only connection model via `cloudflared`. ([Cloudflare Docs][1])

## 1) Prerequisites

You need:

* a domain you control,
* permission to change nameservers at your registrar,
* a Cloudflare account,
* a GitHub repository for the frontend.

Example domain used in this stack:

```text
athithya.site
```

## 2) Add the domain to Cloudflare

1. Open the Cloudflare dashboard.
2. Add your domain.
3. Choose the plan you want to use.
4. Cloudflare will assign two nameservers.
5. Replace the registrar nameservers with Cloudflare’s nameservers.
6. Wait until the zone becomes active.

## 3) Collect the required IDs

You need:

* **Account ID**: Cloudflare dashboard, visible in the account area.
* **Zone ID**: Cloudflare dashboard → your zone → Overview.
* **GitHub repository ID**: GitHub API lookup for the repository.

## 4) Export environment variables

Use **one authentication method only**. This README uses the **Global API Key** path as the default bootstrap method.

```bash
unset CLOUDFLARE_API_TOKEN
export CLOUDFLARE_ACCOUNT_ID="4f75c52006dba7aa4096a71f1ed30223"
export CLOUDFLARE_GLOBAL_API_KEY="your_real_global_api_key"
export CLOUDFLARE_EMAIL="athithya651@gmail.com"

export TF_VAR_account_id="$CLOUDFLARE_ACCOUNT_ID"
export TF_VAR_domain="athithya.site"
export TF_VAR_zone_id="your_real_zone_id"

export TF_VAR_tunnel_name="tabular-api-tunnel"

export TF_VAR_pages_project_name="tabular-ui"
export TF_VAR_pages_branch="main"
export TF_VAR_pages_repo_owner="Athithya-Sakthivel"
export TF_VAR_pages_repo_name="MLSecOps-tabular"
export TF_VAR_pages_repo_id="1187963457"
export TF_VAR_pages_root_dir="."
export TF_VAR_pages_destination_dir="dist"

export TF_VAR_rate_limit_enabled="true"
export TF_VAR_rate_limit_requests="60"
export TF_VAR_rate_limit_period="60"
export TF_VAR_rate_limit_block_seconds="300"
export TF_VAR_rate_limit_action="block"

export GITHUB_OWNER="$TF_VAR_pages_repo_owner"
export GITHUB_REPO="$TF_VAR_pages_repo_name"
```

If you prefer to auto-fetch the IDs instead of setting them manually:

```bash
export TF_VAR_zone_id=$(curl -s \
  -H "X-Auth-Key: $CLOUDFLARE_GLOBAL_API_KEY" \
  -H "X-Auth-Email: $CLOUDFLARE_EMAIL" \
  "https://api.cloudflare.com/client/v4/zones?name=${TF_VAR_domain}" \
  | jq -r '.result[0].id')

export TF_VAR_pages_repo_id=$(curl -s \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}" \
  | jq -r '.id')
```

## 5) Rate limiting variables

These are the rate limiting inputs used by the Terraform stack:

* `TF_VAR_rate_limit_enabled`
* `TF_VAR_rate_limit_requests`
* `TF_VAR_rate_limit_period`
* `TF_VAR_rate_limit_block_seconds`
* `TF_VAR_rate_limit_action`

Cloudflare’s rate limiting docs use `requests_per_period`, `period`, `action`, and `mitigation_timeout` as the core rule parameters. ([Cloudflare Docs][2])

Recommended starter values:

```bash
export TF_VAR_rate_limit_requests="60"
export TF_VAR_rate_limit_period="60"
export TF_VAR_rate_limit_block_seconds="300"
export TF_VAR_rate_limit_action="block"
```

For a stricter public API:

```bash
export TF_VAR_rate_limit_requests="30"
export TF_VAR_rate_limit_period="60"
export TF_VAR_rate_limit_block_seconds="600"
export TF_VAR_rate_limit_action="block"
```

## 6) Tunnel setup

The tunnel logic lives in `src/infra/security/cloudflare_tunnel.sh`. That script is responsible for:

* creating or reusing the tunnel,
* binding DNS for `auth.api.<domain>` and `predict.api.<domain>`,
* verifying the DNS targets,
* exporting `TUNNEL_ID` and `TUNNEL_TOKEN`.

Cloudflare Tunnel uses outbound-only connections from `cloudflared`, and the local-management docs explicitly note that the CLI is useful for single-service flows while configuration files are more appropriate when you connect multiple services. ([Cloudflare Docs][3])

## 7) Run provisioning

```bash
bash src/infra/security/cloudflare_tunnel.sh
bash src/terraform/cloudflare/run.sh --plan
bash src/terraform/cloudflare/run.sh --apply
```

## 8) Expected result

After a successful apply, you should have:

* `app.athithya.site` pointing to Cloudflare Pages,
* `auth.api.athithya.site` routed through the tunnel,
* `predict.api.athithya.site` routed through the tunnel,
* DNS managed in Cloudflare,
* basic rate limiting enabled,
* a deterministic OpenTofu workflow.

## 9) Notes on the Terraform layout

The Terraform stack should keep these responsibilities separate:

* `pages.tf` for Pages and frontend DNS,
* `waf.tf` for WAF custom rules,
* `rate_limit.tf` for rate limiting,
* `providers.tf` for provider setup only,
* `variables.tf` for inputs only,
* `outputs.tf` for outputs only.

Cloudflare’s current Pages project schema still supports `build_config` and `source.config`, and the docs mark `deployments_enabled` as deprecated in favor of `production_deployments_enabled` plus `preview_deployment_setting`. ([Cloudflare Docs][1])

[1]: https://developers.cloudflare.com/api/terraform/resources/pages/subresources/projects/ "Projects | Cloudflare API"
[2]: https://developers.cloudflare.com/waf/rate-limiting-rules/parameters/ "Rate limiting parameters · Cloudflare Web Application Firewall (WAF) docs"
[3]: https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/ "Cloudflare Tunnel · Cloudflare One docs"
