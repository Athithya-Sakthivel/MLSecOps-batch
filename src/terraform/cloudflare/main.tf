variable "account_id" {
  description = "Cloudflare account ID."
  type        = string
}

variable "zone_id" {
  description = "Cloudflare zone ID for the root domain."
  type        = string
}

variable "domain" {
  description = "Root domain, for example example.com."
  type        = string
}

variable "tunnel_cname" {
  description = "Existing Cloudflare Tunnel CNAME target, for example <tunnel-id>.cfargotunnel.com."
  type        = string
}

variable "pages_project_name" {
  description = "Cloudflare Pages project name."
  type        = string
  default     = "tabular-ui"
}

variable "pages_branch" {
  description = "Production branch for Pages."
  type        = string
  default     = "main"
}

variable "pages_repo_owner" {
  description = "GitHub owner/org for the Pages repo."
  type        = string
}

variable "pages_repo_name" {
  description = "GitHub repository name for the Pages project."
  type        = string
}

variable "pages_repo_id" {
  description = "GitHub repository ID for the Pages project."
  type        = string
}

variable "pages_root_dir" {
  description = "Repository root to run the Pages build from."
  type        = string
  default     = "."
}

variable "pages_destination_dir" {
  description = "Build output directory for the static frontend."
  type        = string
  default     = "dist"
}

locals {
  app_hostname     = "app.${var.domain}"
  auth_hostname    = "auth.api.${var.domain}"
  predict_hostname = "predict.api.${var.domain}"
}

# --------------------------------------------------------------------
# Frontend: Cloudflare Pages
#
# The frontend is a simple static app living under:
#   src/frontend/index.html
#   src/frontend/styles.css
#   src/frontend/app.js
#
# This Pages project copies those assets into dist/ during the build.
# Do not add extra zone-level cache rules for app.example.com.
# --------------------------------------------------------------------
resource "cloudflare_pages_project" "frontend" {
  account_id        = var.account_id
  name              = var.pages_project_name
  production_branch = var.pages_branch

  build_config = {
    build_caching   = true
    build_command   = "rm -rf dist && mkdir -p dist && cp -R src/frontend/. dist/"
    destination_dir = var.pages_destination_dir
    root_dir        = var.pages_root_dir
  }

  source = {
    type = "github"
    config = {
      owner                         = var.pages_repo_owner
      repo_name                     = var.pages_repo_name
      repo_id                       = var.pages_repo_id
      production_branch             = var.pages_branch
      production_deployments_enabled = true
    }
  }
}

resource "cloudflare_pages_domain" "frontend_domain" {
  account_id   = var.account_id
  project_name = cloudflare_pages_project.frontend.name
  name         = local.app_hostname
}

# --------------------------------------------------------------------
# Backend API DNS -> existing tunnel CNAME
#
# The actual request routing to auth-svc and predict-service is
# handled by the cloudflared runtime generated in src/manifests/cloudflared/.
# --------------------------------------------------------------------
resource "cloudflare_dns_record" "auth_api" {
  zone_id = var.zone_id
  name    = "auth.api"
  type    = "CNAME"
  content = var.tunnel_cname
  proxied = true
  ttl     = 1
  comment = "Auth service routed through Cloudflare Tunnel"
}

resource "cloudflare_dns_record" "predict_api" {
  zone_id = var.zone_id
  name    = "predict.api"
  type    = "CNAME"
  content = var.tunnel_cname
  proxied = true
  ttl     = 1
  comment = "Predict service routed through Cloudflare Tunnel"
}

output "frontend_url" {
  value = "https://${local.app_hostname}"
}

output "auth_url" {
  value = "https://${local.auth_hostname}"
}

output "predict_url" {
  value = "https://${local.predict_hostname}"
}