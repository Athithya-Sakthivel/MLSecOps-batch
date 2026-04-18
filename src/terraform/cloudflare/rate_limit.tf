locals {
  auth_hostname    = "auth.api.${var.domain}"
  predict_hostname = "predict.api.${var.domain}"
  api_host_expr    = "(http.host in {\"${local.auth_hostname}\" \"${local.predict_hostname}\"})"
}

resource "cloudflare_ruleset" "zone_rate_limit" {
  count       = var.rate_limit_enabled ? 1 : 0
  zone_id     = var.zone_id
  name        = "zone-rate-limit"
  description = "Basic rate limiting for API hosts"
  kind        = "zone"
  phase       = "http_ratelimit"

  rules {
    ref         = "rate_limit_api_hosts"
    description = "Rate limit auth and predict API hosts by IP"
    expression  = local.api_host_expr
    action      = var.rate_limit_action

    ratelimit {
      characteristics     = ["cf.colo.id", "ip.src"]
      period              = var.rate_limit_period
      requests_per_period = var.rate_limit_requests
      mitigation_timeout  = var.rate_limit_mitigation_timeout
    }
  }
}