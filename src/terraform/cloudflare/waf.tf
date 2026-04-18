resource "cloudflare_ruleset" "zone_custom_firewall" {
  zone_id     = var.zone_id
  name        = "zone-custom-firewall"
  description = "Basic zone firewall rule"
  kind        = "zone"
  phase       = "http_request_firewall_custom"

  rules {
    ref         = "block_trace_track"
    description = "Block TRACE and TRACK methods"
    expression  = "http.request.method in {\"TRACE\" \"TRACK\"}"
    action      = "block"
  }
}